import asyncio
import logging
import os
import subprocess
import re
import base64
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


def extract_cover(input_path: str, cover_path: str) -> bool:
    """Надёжное извлечение обложки даже при нестандартных ID (cover.jpg)"""
    try:
        input_p = Path(input_path)
        if not input_p.exists() or input_p.stat().st_size == 0:
            return False
        
        # Шаг 1: пробуем стандартный способ
        try:
            result = subprocess.run(
                ["ebook-meta", "--get-cover", str(input_p), cover_path],
                capture_output=True,
                timeout=30
            )
            cover_p = Path(cover_path)
            if cover_p.exists() and cover_p.stat().st_size > 1000:
                logger.info(f"✅ Обложка извлечена стандартным способом: {cover_path}")
                return True
        except Exception as e:
            logger.debug(f"Стандартный способ не сработал: {e}")
        
        # Шаг 2: ручной парсинг FB2 — ищем ЛЮБОЙ <binary> с image/*
        logger.info("🔍 Стандартный способ не сработал, пробуем ручной парсинг FB2...")
        try:
            with open(input_path, "rb") as f:
                content = f.read()
            
            # Ищем все <binary> с изображениями (игнорируем ссылки и структуру)
            # Регулярка: <binary ... content-type="image/...">base64</binary>
            pattern = rb'<binary[^>]+content-type="image/[^"]+"[^>]*>([^<]+)</binary>'
            matches = re.findall(pattern, content)
            
            if not matches:
                logger.info("❌ Обложка не найдена: нет <binary> с content-type=image/*")
                return False
            
            # Берём ПЕРВЫЙ найденный (обычно это обложка)
            base64_data = matches[0].strip()
            if not base64_data:
                logger.info("❌ Обложка не найдена: пустые данные в <binary>")
                return False
            
            # Декодируем base64
            try:
                image_data = base64.b64decode(base64_data, validate=True)
            except Exception as e:
                # Пробуем без валидации (некоторые файлы имеют битый base64)
                try:
                    image_data = base64.b64decode(base64_data)
                except Exception as e2:
                    logger.warning(f"❌ Ошибка декодирования base64: {e} / {e2}")
                    return False
            
            if len(image_data) < 1000:
                logger.info(f"❌ Слишком маленькие данные ({len(image_data)} байт) — не обложка")
                return False
            
            # Определяем формат по сигнатуре
            ext = ".jpg"
            if image_data.startswith(b'\x89PNG\r\n\x1a\n'):
                ext = ".png"
            elif image_data.startswith(b'GIF87a') or image_data.startswith(b'GIF89a'):
                ext = ".gif"
            
            # Сохраняем
            with open(cover_path, "wb") as f:
                f.write(image_data)
            
            cover_p = Path(cover_path)
            if cover_p.exists() and cover_p.stat().st_size > 1000:
                logger.info(f"✅ Обложка извлечена ручным парсингом: {cover_path} ({cover_p.stat().st_size} байт)")
                return True
            else:
                logger.warning("❌ Не удалось сохранить обложку после декодирования")
                return False
                
        except Exception as e:
            logger.warning(f"❌ Ошибка ручного парсинга FB2: {e}")
            return False
            
    except Exception as e:
        logger.warning(f"❌ Общая ошибка извлечения обложки: {e}")
        return False


def extract_metadata(input_path: str) -> dict:
    """Извлекает метаданные через ebook-meta"""
    try:
        input_p = Path(input_path)
        if not input_p.exists() or input_p.stat().st_size == 0:
            return {"title": None, "authors": None}
        
        result = subprocess.run(
            ["ebook-meta", str(input_p)],
            capture_output=True,
            text=True,
            timeout=30,
            encoding='utf-8',
            errors='replace'
        )
        
        metadata = {"title": None, "authors": None}
        
        # Извлекаем автора и название
        for line in result.stdout.splitlines():
            line = line.strip()
            if line.startswith("Title:") and len(line) > 6:
                val = line[6:].strip()
                if val and val.lower() != "unknown" and val != "":
                    metadata["title"] = val
            elif line.startswith("Author(s):") and len(line) > 10:
                val = line[10:].strip()
                if val and val.lower() != "unknown" and val != "":
                    metadata["authors"] = [a.strip() for a in val.split(",")]
        
        # Fallback из имени файла
        if not metadata["title"] and input_p.name:
            fname = input_p.name
            clean = re.sub(r'\.fb2.*$', '', fname, flags=re.IGNORECASE)
            clean = re.sub(r'[._-]+', ' ', clean)
            metadata["title"] = clean.strip() or "Без названия"
        
        logger.info(f"Метаданные: title={metadata['title']}, authors={metadata['authors']}")
        return metadata
        
    except Exception as e:
        logger.warning(f"Ошибка извлечения метаданных: {e}")
        return {"title": "Без названия", "authors": None}


def convert_book(input_path: str, output_path: str, cover_path: str = None) -> tuple[bool, str]:
    """Конвертация с явным указанием обложки"""
    try:
        input_abs = str(Path(input_path).resolve())
        output_abs = str(Path(output_path).resolve())
        cover_abs = str(Path(cover_path).resolve()) if cover_path else None
        
        input_p = Path(input_abs)
        if not input_p.exists() or input_p.stat().st_size == 0:
            return False, "Файл не найден или пустой"
        
        # Формируем команду
        cmd = ["ebook-convert", input_abs, output_abs]
        
        # Добавляем обложку если найдена
        if cover_abs and Path(cover_abs).exists():
            cmd.extend(["--cover", cover_abs])
            logger.info(f"Конвертация с обложкой: {cover_abs}")
        else:
            logger.info("Конвертация без обложки")
        
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
            logger.error(f"STDERR: {result.stderr[:500]}")
            error_preview = result.stderr[:400].replace('\n', ' | ')
            return False, f"Код {result.returncode} | {error_preview}"
        
        if not output_p.exists() or output_p.stat().st_size == 0:
            return False, f"Выходной файл не создан ({output_p.stat().st_size} байт)"
        
        # Проверяем наличие обложки в результате
        has_cover = False
        try:
            meta_result = subprocess.run(
                ["ebook-meta", str(output_p)],
                capture_output=True,
                text=True,
                timeout=10,
                encoding='utf-8',
                errors='replace'
            )
            if "cover" in meta_result.stdout.lower():
                has_cover = True
        except:
            pass
        
        size_info = f"{output_p.stat().st_size / 1024:.1f} КБ"
        cover_info = " ✓ с обложкой" if has_cover else " ✗ без обложки"
        return True, f"{size_info}{cover_info}"
        
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
            
            # Извлекаем метаданные
            metadata = extract_metadata(task["input_path"])
            title = metadata["title"] or "Без названия"
            author = metadata["authors"][0] if metadata["authors"] else "Неизвестен"
            
            # Извлекаем обложку
            cover_path = f"{task['input_path']}.cover.jpg"
            has_cover = extract_cover(task["input_path"], cover_path)
            
            # Обновляем статус
            try:
                status_text = f"⏳ Конвертирую:\n<b>{title}</b>\n<i>{author}</i>"
                if has_cover:
                    status_text += "\n✅ Обложка найдена и добавлена"
                else:
                    status_text += "\n⚠️ Обложка не найдена во входном файле"
                await application.bot.edit_message_text(
                    chat_id=task["user_id"],
                    message_id=task["message_id"],
                    text=status_text,
                    parse_mode=ParseMode.HTML
                )
            except Exception as e:
                logger.warning(f"Не удалось обновить статус: {e}")
            
            # Конвертируем
            success, diag = convert_book(
                task["input_path"],
                task["output_path"],
                cover_path if has_cover else None
            )
            
            # Отправляем результат
            output_path = Path(task["output_path"])
            if success and output_path.exists():
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
            for p in [task["input_path"], task["output_path"], cover_path]:
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
        "✨ Особенности:\n"
        "• Автоматическое извлечение обложки даже из «кривых» FB2\n"
        "• Корректное сохранение автора и названия\n"
        "• Очередь обработки (макс. 5 файлов)"
    )
    await update.message.reply_text(
        message,
        parse_mode=ParseMode.HTML,
        reply_markup=MAIN_REPLY_KEYBOARD
    )


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    message = (
        "📚 <b>Как работает извлечение обложки:</b>\n\n"
        "Бот использует два способа:\n"
        "1️⃣ Стандартный — через <code>ebook-meta</code>\n"
        "2️⃣ Резервный — ручной парсинг FB2 (ищет любое изображение в теге <code>&lt;binary&gt;</code>)\n\n"
        "💡 Даже если в файле <code>id=\"cover.jpg\"</code> (с расширением), бот найдёт и добавит обложку!"
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
        f"✅ Формат по умолчанию установлен: <b>{fmt.upper()}</b>",
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
            f"⏸️ Очередь заполнена ({conversion_queue.qsize()}/5 файлов).\nПопробуй через минуту.",
            reply_markup=MAIN_REPLY_KEYBOARD
        )
        return

    # Генерируем простые имена файлов
    base_tmp = Path.cwd() / "tmp"
    simple_id = str(uuid4()).replace("-", "")[:12]
    input_ext = Path(filename).suffix or ".fb2"
    output_ext = f".{settings_db.get_preferred_format(update.effective_user.id)}"
    
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
        
        # Проверка 1: файл не пустой
        input_size = Path(task_info["input_path"]).stat().st_size
        if input_size == 0:
            raise ValueError("Файл пустой (0 байт)")
        
        # Проверка 2: для FB2 — валидный XML
        if input_ext.lower() == ".fb2":
            with open(task_info["input_path"], "rb") as f:
                header = f.read(200).decode("utf-8", errors="ignore")
                if "<?xml" not in header and "<FictionBook" not in header:
                    raise ValueError("Файл не является валидным FB2 (отсутствует XML-структура)")
        
        logger.info(f"Файл принят: {input_path.name} ({input_size / 1024:.1f} КБ)")
    except Exception as e:
        logger.error(f"Отклонён файл: {e}")
        await update.message.reply_text(
            f"❌ Некорректный файл: {str(e)}\n\nУбедись, что файл не повреждён.",
            reply_markup=MAIN_REPLY_KEYBOARD
        )
        try:
            Path(task_info["input_path"]).unlink(missing_ok=True)
        except:
            pass
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
            "📎 Прикрепи FB2 или EPUB файл (макс. 10 МБ)\n\n"
            "💡 Бот автоматически найдёт обложку даже если в файле <code>id=\"cover.jpg\"</code>",
            parse_mode=ParseMode.HTML,
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
    tools = ["ebook-convert", "ebook-meta"]
    for tool in tools:
        try:
            subprocess.run([tool, "--version"], capture_output=True, timeout=5)
            logger.info(f"✅ {tool} доступен")
        except Exception as e:
            logger.error(f"❌ {tool} не установлен: {e}")
            raise RuntimeError(f"Требуется {tool}. Выполни: sudo apt install calibre")
    
    asyncio.create_task(conversion_worker(application))
    logger.info("✅ Воркер запущен с надёжным извлечением обложек")


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

    logger.info("🚀 Бот запущен! Поддержка обложек с любыми ID (включая cover.jpg)")
    application.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    try:
        main()
    finally:
        settings_db.close()