import asyncio
import logging
import os
import subprocess
import re
from pathlib import Path
from uuid import uuid4
from datetime import datetime
from dotenv import load_dotenv
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup
from telegram.constants import ParseMode
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    filters,
    ContextTypes,
)
from storage import UserSettings

load_dotenv()

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
    handlers=[
        logging.FileHandler("logs/bot.log", encoding="utf-8"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

Path("logs").mkdir(exist_ok=True)
Path("tmp").mkdir(exist_ok=True)
Path("data").mkdir(exist_ok=True)

conversion_queue = asyncio.Queue(maxsize=5)
active_tasks = {}
settings_db = UserSettings()

MAIN_REPLY_KEYBOARD = ReplyKeyboardMarkup(
    [["📚 Отправить книгу", "⚙️ Настройки", "❓ Помощь"]],
    resize_keyboard=True,
    one_time_keyboard=False
)


def extract_metadata(input_path: str) -> dict:
    """Извлекает метаданные через ebook-meta"""
    try:
        if not Path(input_path).exists() or Path(input_path).stat().st_size == 0:
            return {"title": None, "authors": None, "has_cover": False}
        
        result = subprocess.run(
            ["ebook-meta", input_path],
            capture_output=True,
            text=True,
            timeout=30,
            encoding='utf-8',
            errors='replace'
        )
        
        metadata = {"title": None, "authors": None, "has_cover": False}
        
        # Проверяем обложку
        if "cover" in result.stdout.lower() or "Cover image" in result.stdout:
            metadata["has_cover"] = True
        
        # Извлекаем автора и название
        for line in result.stdout.splitlines():
            line = line.strip()
            if line.startswith("Title:") and len(line) > 6:
                val = line[6:].strip()
                if val and val.lower() != "unknown":
                    metadata["title"] = val
            elif line.startswith("Author(s):") and len(line) > 10:
                val = line[10:].strip()
                if val and val.lower() != "unknown":
                    metadata["authors"] = [a.strip() for a in val.split(",")]
        
        # Fallback из имени файла
        if not metadata["title"] and Path(input_path).name:
            fname = Path(input_path).name
            clean = re.sub(r'\.fb2.*$', '', fname, flags=re.IGNORECASE)
            clean = re.sub(r'[._-]+', ' ', clean)
            metadata["title"] = clean.strip() or None
        
        logger.info(f"Метаданные: title={metadata['title']}, authors={metadata['authors']}, cover={metadata['has_cover']}")
        return metadata
        
    except Exception as e:
        logger.warning(f"Ошибка извлечения метаданных: {e}")
        return {"title": None, "authors": None, "has_cover": False}


def convert_book(input_path: str, output_path: str) -> tuple[bool, str]:
    """МИНИМАЛЬНАЯ рабочая конвертация — только пути, без опций"""
    try:
        # Используем абсолютные пути без спецсимволов
        input_abs = str(Path(input_path).resolve())
        output_abs = str(Path(output_path).resolve())
        
        input_p = Path(input_abs)
        if not input_p.exists() or input_p.stat().st_size == 0:
            return False, "Файл не найден или пустой"
        
        # КРИТИЧЕСКИ ВАЖНО: только 2 аргумента — входной и выходной файлы
        cmd = ["ebook-convert", input_abs, output_abs]
        
        logger.info(f"Конвертация: {Path(input_abs).name} → {Path(output_abs).name}")
        logger.debug(f"Полная команда: {' '.join(cmd)}")
        
        # Запускаем без оболочки (shell=False по умолчанию)
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=180,
            encoding='utf-8',
            errors='replace'
        )
        
        output_p = Path(output_abs)
        if result.returncode != 0:
            # Логируем полную ошибку для диагностики
            logger.error(f"STDERR ebook-convert: {result.stderr}")
            error_preview = result.stderr[:400].replace('\n', ' | ')
            return False, f"Код {result.returncode} | {error_preview}"
        
        if not output_p.exists() or output_p.stat().st_size == 0:
            return False, f"Выходной файл не создан ({output_p.stat().st_size} байт)"
        
        return True, f"{output_p.stat().st_size / 1024:.1f} КБ"
        
    except subprocess.TimeoutExpired:
        return False, "Таймаут 180 сек"
    except Exception as e:
        logger.error(f"Исключение в конвертации: {e}", exc_info=True)
        return False, f"{type(e).__name__}: {str(e)[:150]}"


async def conversion_worker(application: Application):
    logger.info("🔄 Воркер конвертации запущен")
    
    while True:
        try:
            task = await conversion_queue.get()
            task_id = task["task_id"]
            active_tasks[task_id]["status"] = "converting"
            
            # Извлекаем метаданные ДО конвертации
            metadata = extract_metadata(task["input_path"])
            title = metadata["title"] or "Без названия"
            author = metadata["authors"][0] if metadata["authors"] else "Неизвестен"
            has_cover = metadata["has_cover"]
            
            # Обновляем статус
            try:
                status_text = f"⏳ Конвертирую:\n<b>{title}</b>\n<i>{author}</i>"
                if not has_cover:
                    status_text += "\n⚠️ Обложка не обнаружена во входном файле"
                await application.bot.edit_message_text(
                    chat_id=task["user_id"],
                    message_id=task["message_id"],
                    text=status_text,
                    parse_mode=ParseMode.HTML
                )
            except Exception as e:
                logger.warning(f"Не удалось обновить статус: {e}")
            
            # Конвертируем БЕЗ ОПЦИЙ (минимальная рабочая конфигурация)
            success, diag = convert_book(
                task["input_path"],
                task["output_path"]
            )
            
            # Отправляем результат
            output_path = Path(task["output_path"])
            if success and output_path.exists():
                # Формируем безопасное имя файла
                safe_title = re.sub(r'[<>:"/\\|?*]', '', title)[:50]
                safe_author = re.sub(r'[<>:"/\\|?*]', '', author)[:30]
                output_ext = output_path.suffix
                filename = f"{safe_author} - {safe_title}{output_ext}"
                
                caption = (
                    f"✅ Готово! Сконвертировано в <b>{output_ext[1:].upper()}</b>\n"
                    f"📚 {title}\n"
                    f"👤 {author}\n"
                    f"📦 {diag}"
                )
                if not has_cover:
                    caption += "\n\n⚠️ Обложка отсутствовала во входном файле"
                
                await application.bot.send_document(
                    chat_id=task["user_id"],
                    document=open(output_path, "rb"),
                    filename=filename,
                    caption=caption,
                    parse_mode=ParseMode.HTML,
                )
                await application.bot.send_message(
                    chat_id=task["user_id"],
                    text="Файл готов к отправке на Kindle! 📚",
                    reply_markup=MAIN_REPLY_KEYBOARD
                )
            else:
                await application.bot.send_message(
                    chat_id=task["user_id"],
                    text=f"❌ Ошибка конвертации <b>{title}</b>:\n<code>{diag}</code>",
                    parse_mode=ParseMode.HTML,
                    reply_markup=MAIN_REPLY_KEYBOARD
                )
            
            # Чистим временные файлы
            for p in [task["input_path"], task["output_path"]]:
                try:
                    fp = Path(p)
                    if fp.exists():
                        fp.unlink()
                        logger.debug(f"Удалён файл: {p}")
                except Exception as e:
                    logger.warning(f"Не удалось удалить {p}: {e}")
            
            conversion_queue.task_done()
            active_tasks.pop(task_id, None)
            
        except Exception as e:
            logger.error(f"Ошибка в воркере: {e}", exc_info=True)
            await asyncio.sleep(5)


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    message = (
        "📚 <b>KindleGarden Bot</b>\n\n"
        "Отправляй FB2/EPUB → получаешь книгу для Kindle!\n\n"
        "✅ Поддерживаемые форматы:\n"
        "• <b>AZW3</b> — рекомендуется для современных Kindle\n"
        "• <b>EPUB</b> — универсальный формат\n"
        "• <b>MOBI</b> — для старых устройств"
    )
    await update.message.reply_text(
        message,
        parse_mode=ParseMode.HTML,
        reply_markup=MAIN_REPLY_KEYBOARD
    )


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    message = (
        "📚 <b>Как получить лучший результат:</b>\n\n"
        "1️⃣ <b>Проверь исходный файл:</b>\n"
        "   • Открой FB2 в Calibre на ПК\n"
        "   • Убедись, что есть обложка и заполнены «Автор»/«Название»\n\n"
        "2️⃣ <b>Если метаданные пустые:</b>\n"
        "   • В Calibre: ПКМ → «Редактировать метаданные»\n"
        "   • Заполни поля и добавь обложку\n"
        "   • Сохрани изменения (Ctrl+S)\n\n"
        "3️⃣ <b>Отправь исправленный файл в бота</b>\n\n"
        "💡 Бот автоматически извлекает метаданные и обложку из правильно оформленного FB2."
    )
    await update.message.reply_text(
        message,
        parse_mode=ParseMode.HTML,
        reply_markup=MAIN_REPLY_KEYBOARD
    )


async def settings_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id
    current = settings_db.get_preferred_format(user_id)
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton(f"{'✅ ' if f == current else ''}📘 AZW3 (рекомендуется)", callback_data=f"setfmt:{f}")] for f in ["azw3"]
    ] + [
        [InlineKeyboardButton(f"{'✅ ' if f == current else ''}📖 EPUB", callback_data=f"setfmt:{f}")] for f in ["epub"]
    ] + [
        [InlineKeyboardButton(f"{'✅ ' if f == current else ''}📙 MOBI (устаревший)", callback_data=f"setfmt:{f}")] for f in ["mobi"]
    ])
    await update.message.reply_text(
        f"⚙️ Текущий формат: <b>{current.upper()}</b>\nВыбери новый:",
        parse_mode=ParseMode.HTML,
        reply_markup=kb
    )


async def handle_format_setting(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    _, fmt = query.data.split(":")
    settings_db.set_preferred_format(update.effective_user.id, fmt)
    await query.edit_message_text(
        f"✅ Формат по умолчанию: <b>{fmt.upper()}</b>",
        parse_mode=ParseMode.HTML
    )
    await query.message.reply_text("Выбери действие:", reply_markup=MAIN_REPLY_KEYBOARD)


async def handle_document(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    document = update.message.document
    filename = document.file_name.lower() if document.file_name else ""
    
    # Проверяем формат
    if not (filename.endswith(".fb2") or filename.endswith(".fb2.zip") or filename.endswith(".epub")):
        await update.message.reply_text(
            "⚠️ Принимаю только FB2 и EPUB файлы (.fb2, .fb2.zip, .epub)",
            reply_markup=MAIN_REPLY_KEYBOARD
        )
        return

    # Лимит размера для малинки 3
    if document.file_size > 10 * 1024 * 1024:
        await update.message.reply_text(
            "⚠️ Файл слишком большой (максимум 10 МБ для малинки 3)",
            reply_markup=MAIN_REPLY_KEYBOARD
        )
        return

    # Проверяем очередь
    if conversion_queue.full():
        await update.message.reply_text(
            f"⏸️ Очередь заполнена ({conversion_queue.qsize()}/5 файлов)",
            reply_markup=MAIN_REPLY_KEYBOARD
        )
        return

    # Генерируем ПРОСТЫЕ имена файлов без спецсимволов
    base_tmp = Path.cwd() / "tmp"
    simple_id = str(uuid4()).replace("-", "")[:12]  # 12 символов без дефисов
    input_ext = Path(filename).suffix or ".fb2"
    output_ext = f".{settings_db.get_preferred_format(update.effective_user.id)}"
    
    # Формируем пути с простыми именами
    input_path = base_tmp / f"in_{simple_id}{input_ext}"
    output_path = base_tmp / f"out_{simple_id}{output_ext}"
    
    task_info = {
        "task_id": simple_id,
        "user_id": update.effective_user.id,
        "file_id": document.file_id,
        "file_name": document.file_name,
        "input_path": str(input_path),
        "output_path": str(output_path),
        "output_format": output_ext[1:],
        "status": "queued",
    }
    active_tasks[simple_id] = task_info

    # Скачиваем файл
    try:
        file = await context.bot.get_file(document.file_id)
        await file.download_to_drive(task_info["input_path"])
        input_size = Path(task_info["input_path"]).stat().st_size
        if input_size == 0:
            raise ValueError("Скачанный файл пустой")
        logger.info(f"Файл скачан: {input_path.name} ({input_size / 1024:.1f} КБ)")
    except Exception as e:
        logger.error(f"Ошибка скачивания: {e}")
        await update.message.reply_text(
            "❌ Ошибка при получении файла. Попробуй заново.",
            reply_markup=MAIN_REPLY_KEYBOARD
        )
        return

    # Ставим в очередь
    await conversion_queue.put(task_info)
    position = conversion_queue.qsize()
    
    msg = await update.message.reply_text(
        f"✅ Файл добавлен в очередь\n"
        f"Формат: <b>{task_info['output_format'].upper()}</b>\n"
        f"Позиция: {position} из 5",
        parse_mode=ParseMode.HTML
    )
    task_info["message_id"] = msg.message_id


async def handle_text_commands(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    text = update.message.text.strip()
    if text == "📚 Отправить книгу":
        await update.message.reply_text(
            "📎 Прикрепи FB2 или EPUB файл (макс. 10 МБ)",
            reply_markup=MAIN_REPLY_KEYBOARD
        )
    elif text == "⚙️ Настройки":
        await settings_menu(update, context)
    elif text == "❓ Помощь":
        await help_command(update, context)
    else:
        await update.message.reply_text(
            "Используй меню внизу 👇",
            reply_markup=MAIN_REPLY_KEYBOARD
        )


async def post_init(application: Application) -> None:
    # Проверяем зависимости
    for tool in ["ebook-convert", "ebook-meta"]:
        try:
            subprocess.run([tool, "--version"], capture_output=True, timeout=5)
            logger.info(f"✅ {tool} доступен")
        except Exception as e:
            logger.error(f"❌ {tool} не установлен: {e}")
            raise RuntimeError(f"Требуется {tool}. Установи: sudo apt install calibre")
    
    asyncio.create_task(conversion_worker(application))
    logger.info("✅ Воркер конвертации запущен")


def main() -> None:
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    if not token:
        logger.error("❌ Токен не найден! Создай .env с TELEGRAM_BOT_TOKEN")
        return

    application = Application.builder().token(token).post_init(post_init).build()

    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("settings", settings_menu))
    application.add_handler(CommandHandler("help", help_command))
    application.add_handler(MessageHandler(filters.Document.ALL, handle_document))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_commands))
    application.add_handler(CallbackQueryHandler(handle_format_setting, pattern="^setfmt:"))

    logger.info("🚀 Бот запущен с минимальной конфигурацией конвертации!")
    application.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    try:
        main()
    finally:
        settings_db.close()