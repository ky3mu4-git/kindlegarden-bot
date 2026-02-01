import asyncio
import logging
import os
import subprocess
import re
import base64
import zipfile
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

# Проверяем наличие Pillow
try:
    from PIL import Image
    HAS_PILLOW = True
except ImportError:
    HAS_PILLOW = False
    print("⚠️  Pillow не установлен! Обложки не будут масштабироваться.")
    print("   Установи: pip install Pillow")

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

MIN_COVER_WIDTH = 330
MIN_COVER_HEIGHT = 500


def is_zip_file(path: str) -> bool:
    """Проверяет, является ли файл ZIP по сигнатуре"""
    try:
        with open(path, "rb") as f:
            return f.read(4) == b"PK\x03\x04"
    except:
        return False


def unpack_if_needed(input_path: str) -> str:
    """Распаковывает FB2.ZIP в чистый FB2"""
    input_p = Path(input_path)
    
    if not is_zip_file(input_path):
        logger.info(f"Файл не является архивом: {input_path}")
        return input_path
    
    logger.info(f"Распаковка архива: {input_path}")
    try:
        with zipfile.ZipFile(input_path, "r") as zf:
            fb2_files = [f for f in zf.namelist() if f.lower().endswith(".fb2")]
            if not fb2_files:
                raise ValueError("В архиве не найден файл .fb2")
            
            extracted_path = input_p.with_suffix(".unpacked.fb2")
            with zf.open(fb2_files[0]) as src, open(extracted_path, "wb") as dst:
                dst.write(src.read())
            
            # Проверка валидности распакованного файла
            with open(extracted_path, "rb") as f:
                header = f.read(200)
                if b"<?xml" not in header and b"<FictionBook" not in header:
                    logger.error("Распакованный файл не является валидным FB2")
                    extracted_path.unlink(missing_ok=True)
                    raise ValueError("Распакованный файл повреждён")
            
            logger.info(f"Распаковано: {extracted_path}")
            return str(extracted_path)
    except Exception as e:
        logger.error(f"Ошибка распаковки: {e}")
        return input_path


def resize_cover_if_needed(cover_path: str) -> bool:
    """Масштабирует обложку до минимальных размеров для Kindle"""
    if not HAS_PILLOW:
        logger.warning("Pillow не установлен — пропускаем масштабирование")
        return False
    
    try:
        cover_p = Path(cover_path)
        if not cover_p.exists() or cover_p.stat().st_size == 0:
            return False
        
        with Image.open(cover_path) as img:
            if img.mode in ("RGBA", "P"):
                img = img.convert("RGB")
            
            width, height = img.size
            if width >= MIN_COVER_WIDTH and height >= MIN_COVER_HEIGHT:
                return True
            
            ratio = max(MIN_COVER_WIDTH / width, MIN_COVER_HEIGHT / height)
            new_width = int(width * ratio)
            new_height = int(height * ratio)
            
            img_resized = img.resize((new_width, new_height), Image.Resampling.LANCZOS)
            img_resized.save(cover_path, "JPEG", quality=90, optimize=True)
            return True
            
    except Exception as e:
        logger.warning(f"Ошибка масштабирования: {e}")
        return False


def extract_metadata_fallback(input_path: str) -> dict:
    """Резервное извлечение метаданных через парсинг XML"""
    try:
        with open(input_path, "rb") as f:
            content = f.read()
        
        for enc in ["utf-8", "cp1251", "koi8-r"]:
            try:
                text = content.decode(enc)
                break
            except:
                continue
        else:
            text = content.decode("utf-8", errors="ignore")
        
        author = "Неизвестен"
        first = re.search(r"<first-name[^>]*>([^<]+)</first-name>", text, re.IGNORECASE)
        last = re.search(r"<last-name[^>]*>([^<]+)</last-name>", text, re.IGNORECASE)
        if first and last:
            author = f"{first.group(1).strip()} {last.group(1).strip()}"
        elif first:
            author = first.group(1).strip()
        elif last:
            author = last.group(1).strip()
        
        title = "Без названия"
        title_match = re.search(r"<book-title[^>]*>([^<]+)</book-title>", text, re.IGNORECASE)
        if title_match:
            title = title_match.group(1).strip()
        
        return {"title": title, "authors": [author] if author != "Неизвестен" else None}
        
    except Exception as e:
        logger.warning(f"Ошибка парсинга XML: {e}")
        return {"title": "Без названия", "authors": None}


def extract_metadata(input_path: str) -> dict:
    """Извлекает метаданные — сначала через ebook-meta, потом fallback"""
    try:
        result = subprocess.run(
            ["ebook-meta", input_path],
            capture_output=True,
            text=True,
            timeout=30,
            encoding='utf-8',
            errors='replace'
        )
        
        metadata = {"title": None, "authors": None}
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
        
        if not metadata["title"] or not metadata["authors"]:
            fallback = extract_metadata_fallback(input_path)
            if not metadata["title"]:
                metadata["title"] = fallback["title"]
            if not metadata["authors"]:
                metadata["authors"] = fallback["authors"]
        
        return metadata
        
    except Exception as e:
        logger.warning(f"Ошибка извлечения метаданных: {e}")
        return extract_metadata_fallback(input_path)


def extract_cover(input_path: str, cover_path: str) -> bool:
    """Извлекает обложку — сначала стандартно, потом ручной парсинг"""
    try:
        try:
            subprocess.run(
                ["ebook-meta", "--get-cover", input_path, cover_path],
                capture_output=True,
                timeout=30
            )
            cover_p = Path(cover_path)
            if cover_p.exists() and cover_p.stat().st_size > 500:
                resize_cover_if_needed(cover_path)
                return True
        except:
            pass
        
        try:
            with open(input_path, "rb") as f:
                content = f.read()
            
            pattern = rb'<binary[^>]+content-type="image/[^"]+"[^>]*>([^<]+)</binary>'
            matches = re.findall(pattern, content)
            
            if not matches:
                return False
            
            try:
                image_data = base64.b64decode(matches[0].strip(), validate=True)
            except:
                image_data = base64.b64decode(matches[0].strip())
            
            if len(image_data) < 500:
                return False
            
            with open(cover_path, "wb") as f:
                f.write(image_data)
            
            cover_p = Path(cover_path)
            if cover_p.exists() and cover_p.stat().st_size > 500:
                resize_cover_if_needed(cover_path)
                return True
                
        except Exception as e:
            logger.warning(f"Ошибка ручного парсинга: {e}")
            return False
            
    except Exception as e:
        logger.warning(f"Общая ошибка извлечения обложки: {e}")
        return False


def convert_book(input_path: str, output_path: str, cover_path: str = None) -> tuple[bool, str]:
    """Конвертация БЕЗ перехвата вывода (стабильно на малинке 3)"""
    try:
        input_abs = str(Path(input_path).resolve())
        output_abs = str(Path(output_path).resolve())
        cover_abs = str(Path(cover_path).resolve()) if cover_path else None
        
        input_p = Path(input_abs)
        if not input_p.exists():
            return False, f"Входной файл не найден: {input_abs}"
        if input_p.stat().st_size == 0:
            return False, f"Входной файл пустой: {input_abs}"
        
        cmd = ["ebook-convert", input_abs, output_abs]
        
        if cover_abs and Path(cover_abs).exists() and Path(cover_abs).stat().st_size > 500:
            cmd.extend(["--cover", cover_abs])
        
        logger.info(f"Конвертация: {Path(input_abs).name} → {Path(output_abs).name}")
        
        # 🔑 КРИТИЧЕСКОЕ ИЗМЕНЕНИЕ: не перехватываем вывод для стабильности на малинке
        result = subprocess.run(
            cmd,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            timeout=180
        )
        
        output_p = Path(output_abs)
        if result.returncode != 0:
            return False, f"Код ошибки: {result.returncode}"
        
        if not output_p.exists() or output_p.stat().st_size == 0:
            return False, f"Выходной файл не создан ({output_p.stat().st_size} байт)"
        
        # Проверяем обложку в результате
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
            has_cover = "cover" in meta_result.stdout.lower()
        except:
            pass
        
        size_info = f"{output_p.stat().st_size / 1024:.1f} КБ"
        cover_info = " ✓ обложка в библиотеке" if has_cover else " ✗ без миниатюры"
        return True, f"{size_info}{cover_info}"
        
    except subprocess.TimeoutExpired:
        return False, "Таймаут 180 сек"
    except Exception as e:
        logger.error(f"Исключение в конвертации: {e}", exc_info=True)
        return False, f"{type(e).__name__}: {str(e)[:150]}"


async def conversion_worker(application: Application):
    logger.info("🔄 Воркер запущен")
    
    while True:
        try:
            task = await conversion_queue.get()
            task_id = task["task_id"]
            active_tasks[task_id]["status"] = "converting"
            
            unpacked_path = unpack_if_needed(task["input_path"])
            cleanup_unpacked = (unpacked_path != task["input_path"])
            
            metadata = extract_metadata(unpacked_path)
            title = metadata["title"] or "Без названия"
            author = metadata["authors"][0] if metadata["authors"] else "Неизвестен"
            
            cover_path = f"{task['input_path']}.cover.jpg"
            has_cover = extract_cover(unpacked_path, cover_path)
            
            try:
                status = f"⏳ Конвертирую:\n<b>{title}</b>\n<i>{author}</i>"
                if has_cover:
                    status += "\n✅ Обложка найдена"
                else:
                    status += "\n⚠️ Обложка не найдена"
                await application.bot.edit_message_text(
                    chat_id=task["user_id"],
                    message_id=task["message_id"],
                    text=status,
                    parse_mode=ParseMode.HTML
                )
            except:
                pass
            
            success, diag = convert_book(
                unpacked_path,
                task["output_path"],
                cover_path if has_cover else None
            )
            
            output_p = Path(task["output_path"])
            if success and output_p.exists():
                safe_title = re.sub(r'[<>:"/\\|?*]', '', title)[:50]
                safe_author = re.sub(r'[<>:"/\\|?*]', '', author)[:30]
                filename = f"{safe_author} - {safe_title}{output_p.suffix}"
                
                caption = (
                    f"✅ Готово! <b>{task['output_format'].upper()}</b>\n"
                    f"📚 {title}\n"
                    f"👤 {author}\n"
                    f"📦 {diag}"
                )
                
                await application.bot.send_document(
                    chat_id=task["user_id"],
                    document=open(output_p, "rb"),
                    filename=filename,
                    caption=caption,
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
                    text=f"❌ Ошибка <b>{title}</b>:\n<code>{diag}</code>",
                    parse_mode=ParseMode.HTML,
                    reply_markup=MAIN_REPLY_KEYBOARD
                )
            
            for p in [task["input_path"], task["output_path"], cover_path]:
                try:
                    fp = Path(p)
                    if fp.exists():
                        fp.unlink()
                except:
                    pass
            if cleanup_unpacked:
                try:
                    Path(unpacked_path).unlink()
                except:
                    pass
            
            conversion_queue.task_done()
            active_tasks.pop(task_id, None)
            
        except Exception as e:
            logger.error(f"Ошибка воркера: {e}", exc_info=True)
            await asyncio.sleep(5)


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(
        "📚 <b>KindleGarden Bot</b>\n\n"
        "Отправляй FB2/EPUB → получаешь книгу для Kindle!\n\n"
        "✨ Автоматическое извлечение и масштабирование обложек",
        parse_mode=ParseMode.HTML,
        reply_markup=MAIN_REPLY_KEYBOARD
    )


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(
        "📚 <b>Совет:</b>\n"
        "Если конвертация падает с ошибкой — файл может быть повреждён.\n"
        "Распакуй его вручную через Calibre на ПК и отправь чистый FB2.",
        parse_mode=ParseMode.HTML,
        reply_markup=MAIN_REPLY_KEYBOARD
    )


async def settings_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id
    current = settings_db.get_preferred_format(user_id)
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton(f"{'✅ ' if f == current else ''}📘 AZW3", callback_data=f"setfmt:{f}")] for f in ["azw3"]
    ] + [
        [InlineKeyboardButton(f"{'✅ ' if f == current else ''}📖 EPUB", callback_data=f"setfmt:{f}")] for f in ["epub"]
    ] + [
        [InlineKeyboardButton(f"{'✅ ' if f == current else ''}📙 MOBI", callback_data=f"setfmt:{f}")] for f in ["mobi"]
    ])
    await update.message.reply_text(
        f"⚙️ Формат: <b>{current.upper()}</b>",
        parse_mode=ParseMode.HTML,
        reply_markup=kb
    )


async def handle_format_setting(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    _, fmt = query.data.split(":")
    settings_db.set_preferred_format(update.effective_user.id, fmt)
    await query.edit_message_text(f"✅ {fmt.upper()}", parse_mode=ParseMode.HTML)
    await query.message.reply_text("Выбери действие:", reply_markup=MAIN_REPLY_KEYBOARD)


async def handle_document(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    doc = update.message.document
    fname = doc.file_name.lower() if doc.file_name else ""
    
    if not (fname.endswith(".fb2") or fname.endswith(".fb2.zip") or fname.endswith(".epub")):
        await update.message.reply_text("⚠️ Только FB2/EPUB", reply_markup=MAIN_REPLY_KEYBOARD)
        return

    if doc.file_size > 10 * 1024 * 1024:
        await update.message.reply_text("⚠️ Максимум 10 МБ", reply_markup=MAIN_REPLY_KEYBOARD)
        return

    if conversion_queue.full():
        await update.message.reply_text(f"⏸️ Очередь заполнена ({conversion_queue.qsize()}/5)", reply_markup=MAIN_REPLY_KEYBOARD)
        return

    base_tmp = Path.cwd() / "tmp"
    simple_id = str(uuid4()).replace("-", "")[:12]
    input_ext = Path(fname).suffix or ".fb2"
    output_ext = f".{settings_db.get_preferred_format(update.effective_user.id)}"
    
    input_path = base_tmp / f"in_{simple_id}{input_ext}"
    output_path = base_tmp / f"out_{simple_id}{output_ext}"
    
    task = {
        "task_id": simple_id,
        "user_id": update.effective_user.id,
        "file_id": doc.file_id,
        "file_name": doc.file_name,
        "input_path": str(input_path),
        "output_path": str(output_path),
        "output_format": output_ext[1:],
        "status": "queued",
    }
    active_tasks[simple_id] = task

    try:
        file = await context.bot.get_file(doc.file_id)
        await file.download_to_drive(task["input_path"])
        
        input_size = Path(task["input_path"]).stat().st_size
        if input_size == 0:
            raise ValueError("Пустой файл")
        
        logger.info(f"Файл принят: {input_size / 1024:.1f} КБ")
    except Exception as e:
        logger.error(f"Ошибка: {e}")
        await update.message.reply_text(f"❌ {str(e)}", reply_markup=MAIN_REPLY_KEYBOARD)
        try:
            Path(task["input_path"]).unlink(missing_ok=True)
        except:
            pass
        return

    await conversion_queue.put(task)
    msg = await update.message.reply_text(
        f"✅ В очереди ({conversion_queue.qsize()}/5)\nФормат: <b>{task['output_format'].upper()}</b>",
        parse_mode=ParseMode.HTML
    )
    task["message_id"] = msg.message_id


async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    t = update.message.text.strip()
    if t == "📚 Отправить книгу":
        await update.message.reply_text("📎 FB2/EPUB до 10 МБ", reply_markup=MAIN_REPLY_KEYBOARD)
    elif t == "⚙️ Настройки":
        await settings_menu(update, context)
    elif t == "❓ Помощь":
        await help_command(update, context)
    else:
        await update.message.reply_text("Используй меню 👇", reply_markup=MAIN_REPLY_KEYBOARD)


async def post_init(app: Application) -> None:
    for tool in ["ebook-convert", "ebook-meta"]:
        try:
            subprocess.run([tool, "--version"], capture_output=True, timeout=5)
        except:
            raise RuntimeError(f"{tool} не установлен. Выполни: sudo apt install calibre")
    
    if not HAS_PILLOW:
        logger.warning("⚠️  Pillow не установлен")
    
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
    
    logger.info("🚀 Бот запущен с оптимизацией для малинки 3")
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    try:
        main()
    finally:
        settings_db.close()