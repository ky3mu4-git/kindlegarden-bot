import asyncio
import logging
import os
import subprocess
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

# Загружаем переменные из .env
load_dotenv()

# Настройка логирования
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
    handlers=[
        logging.FileHandler("logs/bot.log", encoding="utf-8"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Убедимся, что папки существуют
Path("logs").mkdir(exist_ok=True)
Path("tmp").mkdir(exist_ok=True)
Path("data").mkdir(exist_ok=True)

# Глобальные объекты
conversion_queue = asyncio.Queue(maxsize=5)
active_tasks = {}
settings_db = UserSettings()

# Постоянное меню внизу экрана
MAIN_REPLY_KEYBOARD = ReplyKeyboardMarkup(
    [["📚 Отправить книгу", "⚙️ Настройки", "❓ Помощь"]],
    resize_keyboard=True,
    one_time_keyboard=False
)


# ========== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ==========

def extract_metadata(input_path: str) -> dict:
    """Извлекает автора и название книги через ebook-meta"""
    try:
        if not Path(input_path).exists() or Path(input_path).stat().st_size == 0:
            return {"title": "Неизвестно", "authors": ["Неизвестен"]}
        
        result = subprocess.run(
            ["ebook-meta", input_path],
            capture_output=True,
            text=True,
            timeout=30,
            encoding='utf-8',
            errors='replace'
        )
        
        metadata = {"title": "Неизвестно", "authors": ["Неизвестен"]}
        for line in result.stdout.splitlines():
            line = line.strip()
            if line.startswith("Title:") and len(line) > 6:
                metadata["title"] = line[6:].strip() or "Неизвестно"
            elif line.startswith("Author(s):") and len(line) > 10:
                authors_raw = line[10:].strip()
                metadata["authors"] = [a.strip() for a in authors_raw.split(",")] if authors_raw else ["Неизвестен"]
        
        return metadata
        
    except Exception as e:
        logger.warning(f"Ошибка извлечения метаданных: {e}")
        return {"title": "Неизвестно", "authors": ["Неизвестен"]}


def convert_book(input_path: str, output_path: str, output_format: str) -> tuple[bool, str]:
    """Конвертирует книгу — МИНИМАЛЬНАЯ рабочая конфигурация без проблемных опций"""
    try:
        input_abs = str(Path(input_path).resolve())
        output_abs = str(Path(output_path).resolve())
        
        # Проверка файла
        input_p = Path(input_abs)
        if not input_p.exists():
            return False, f"Файл не найден: {input_abs}"
        if input_p.stat().st_size == 0:
            return False, f"Файл пустой: {input_abs} ({input_p.stat().st_size} байт)"
        
        # МИНИМАЛЬНАЯ КОМАНДА — без --extra-css и других опций, которые ломают парсинг
        cmd = [
            "ebook-convert",
            input_abs,
            output_abs,
            "--output-profile", "kindle",
        ]
        
        logger.info(f"Конвертация: {Path(input_abs).name} → {Path(output_abs).name}")
        logger.debug(f"Команда: {' '.join(cmd)}")
        
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
            # Выводим первые 500 символов ошибки для диагностики
            error_preview = result.stderr[:500].replace('\n', ' | ')
            return False, f"Код {result.returncode} | {error_preview}"
        
        if not output_p.exists() or output_p.stat().st_size == 0:
            return False, f"Выходной файл не создан ({output_p.stat().st_size if output_p.exists() else 'N/A'} байт)"
        
        return True, f"{output_p.stat().st_size / 1024:.1f} КБ"
        
    except subprocess.TimeoutExpired:
        return False, "Таймаут 180 сек"
    except Exception as e:
        return False, f"{type(e).__name__}: {str(e)[:150]}"


async def conversion_worker(application: Application):
    """Воркер конвертации"""
    logger.info("🔄 Воркер запущен")
    
    while True:
        try:
            task = await conversion_queue.get()
            task_id = task["task_id"]
            active_tasks[task_id]["status"] = "converting"
            
            # Извлекаем метаданные
            metadata = extract_metadata(task["input_path"])
            title = metadata["title"]
            author = metadata["authors"][0] if metadata["authors"] else "Неизвестен"
            
            # Обновляем статус
            try:
                await application.bot.edit_message_text(
                    chat_id=task["user_id"],
                    message_id=task["message_id"],
                    text=f"⏳ Конвертирую:\n<b>{title}</b>\n<i>{author}</i>",
                    parse_mode=ParseMode.HTML
                )
            except:
                pass
            
            # Конвертируем
            success, diag = convert_book(
                task["input_path"],
                task["output_path"],
                task["output_format"]
            )
            
            # Отправляем результат
            output_path = Path(task["output_path"])
            if success and output_path.exists():
                # Формируем имя файла
                safe_title = "".join(c for c in title if c.isalnum() or c in " ._-" and c != '/')
                safe_author = "".join(c for c in author if c.isalnum() or c in " ._-" and c != '/')
                filename = f"{safe_author[:30]} - {safe_title[:50]}.{task['output_format']}"
                
                await application.bot.send_document(
                    chat_id=task["user_id"],
                    document=open(output_path, "rb"),
                    filename=filename,
                    caption=f"✅ {task['output_format'].upper()}\n📚 {title}\n👤 {author}\n📦 {diag}",
                    parse_mode=ParseMode.HTML,
                )
                await application.bot.send_message(
                    chat_id=task["user_id"],
                    text="Файл готов для Kindle! 📚",
                    reply_markup=MAIN_REPLY_KEYBOARD
                )
            else:
                await application.bot.send_message(
                    chat_id=task["user_id"],
                    text=f"❌ Ошибка конвертации <b>{title}</b>:\n<code>{diag}</code>",
                    parse_mode=ParseMode.HTML,
                    reply_markup=MAIN_REPLY_KEYBOARD
                )
            
            # Чистим файлы
            for p in [task["input_path"], task["output_path"]]:
                try:
                    fp = Path(p)
                    if fp.exists():
                        fp.unlink()
                except:
                    pass
            
            conversion_queue.task_done()
            active_tasks.pop(task_id, None)
            
        except Exception as e:
            logger.error(f"Ошибка воркера: {e}", exc_info=True)
            await asyncio.sleep(5)


# ========== ОБРАБОТЧИКИ ==========

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(
        "📚 <b>KindleGarden Bot</b>\n\n"
        "Отправляй FB2/EPUB → получаешь книгу для Kindle!\n\n"
        "Форматы: AZW3 (рекомендуется), EPUB, MOBI",
        parse_mode=ParseMode.HTML,
        reply_markup=MAIN_REPLY_KEYBOARD
    )


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(
        "📚 <b>Инструкция:</b>\n\n"
        "1. Отправь FB2 или EPUB файл (до 10 МБ)\n"
        "2. Бот автоматически сконвертирует его\n"
        "3. Получи файл с обложкой и метаданными\n\n"
        "⚙️ Настройки → выбор формата по умолчанию",
        parse_mode=ParseMode.HTML,
        reply_markup=MAIN_REPLY_KEYBOARD
    )


async def settings_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id
    current = settings_db.get_preferred_format(user_id)
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton(f"{'✅' if f == current else ''} 📘 AZW3", callback_data=f"setfmt:{f}")] if (f := "azw3") else [],
        [InlineKeyboardButton(f"{'✅' if f == current else ''} 📖 EPUB", callback_data=f"setfmt:{f}")] if (f := "epub") else [],
        [InlineKeyboardButton(f"{'✅' if f == current else ''} 📙 MOBI", callback_data=f"setfmt:{f}")] if (f := "mobi") else [],
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
        f"✅ Формат установлен: <b>{fmt.upper()}</b>",
        parse_mode=ParseMode.HTML
    )
    await query.message.reply_text("Выбери действие:", reply_markup=MAIN_REPLY_KEYBOARD)


async def handle_document(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    doc = update.message.document
    fname = doc.file_name.lower() if doc.file_name else ""
    
    if not (fname.endswith(".fb2") or fname.endswith(".fb2.zip") or fname.endswith(".epub")):
        await update.message.reply_text("⚠️ Только FB2/EPUB файлы", reply_markup=MAIN_REPLY_KEYBOARD)
        return

    if doc.file_size > 10 * 1024 * 1024:
        await update.message.reply_text("⚠️ Максимум 10 МБ", reply_markup=MAIN_REPLY_KEYBOARD)
        return

    if conversion_queue.full():
        await update.message.reply_text(f"⏸️ Очередь заполнена ({conversion_queue.qsize()}/5)", reply_markup=MAIN_REPLY_KEYBOARD)
        return

    # Генерируем пути
    base = Path.cwd() / "tmp"
    tid = str(uuid4())
    ext_in = Path(fname).suffix or ".fb2"
    ext_out = f".{settings_db.get_preferred_format(update.effective_user.id)}"
    
    task = {
        "task_id": tid,
        "user_id": update.effective_user.id,
        "file_id": doc.file_id,
        "file_name": doc.file_name,
        "input_path": str(base / f"{tid}{ext_in}"),
        "output_path": str(base / f"{tid}{ext_out}"),
        "output_format": ext_out[1:],
        "status": "queued",
    }
    active_tasks[tid] = task

    # Скачиваем
    try:
        file = await context.bot.get_file(doc.file_id)
        await file.download_to_drive(task["input_path"])
        if Path(task["input_path"]).stat().st_size == 0:
            raise ValueError("Пустой файл")
    except Exception as e:
        logger.error(f"Ошибка скачивания: {e}")
        await update.message.reply_text("❌ Ошибка скачивания", reply_markup=MAIN_REPLY_KEYBOARD)
        return

    # В очередь
    await conversion_queue.put(task)
    msg = await update.message.reply_text(
        f"✅ В очереди ({conversion_queue.qsize()}/5)\nФормат: <b>{task['output_format'].upper()}</b>",
        parse_mode=ParseMode.HTML
    )
    task["message_id"] = msg.message_id


async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    t = update.message.text.strip()
    if t == "📚 Отправить книгу":
        await update.message.reply_text("📎 Прикрепи FB2/EPUB (до 10 МБ)", reply_markup=MAIN_REPLY_KEYBOARD)
    elif t == "⚙️ Настройки":
        await settings_menu(update, context)
    elif t == "❓ Помощь":
        await help_command(update, context)
    else:
        await update.message.reply_text("Используй меню внизу 👇", reply_markup=MAIN_REPLY_KEYBOARD)


async def post_init(app: Application) -> None:
    # Проверка зависимостей
    for tool in ["ebook-convert", "ebook-meta"]:
        try:
            subprocess.run([tool, "--version"], capture_output=True, timeout=5)
        except:
            raise RuntimeError(f"{tool} не установлен. Выполни: sudo apt install calibre")
    asyncio.create_task(conversion_worker(app))
    logger.info("✅ Бот готов")


def main() -> None:
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    if not token:
        raise RuntimeError("Нет TELEGRAM_BOT_TOKEN в .env")
    
    app = Application.builder().token(token).post_init(post_init).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("settings", settings_menu))
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(MessageHandler(filters.Document.ALL, handle_document))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))
    app.add_handler(CallbackQueryHandler(handle_format_setting, pattern="^setfmt:"))
    
    logger.info("🚀 Бот запущен")
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    try:
        main()
    finally:
        settings_db.close()