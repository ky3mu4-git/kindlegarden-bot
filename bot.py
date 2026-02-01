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
from telegram.ext import Application, CommandHandler, MessageHandler, CallbackQueryHandler, filters, ContextTypes
from storage import UserSettings

try:
    from PIL import Image
    HAS_PILLOW = True
except ImportError:
    HAS_PILLOW = False
    print("⚠️  Pillow не установлен. Выполни: pip install Pillow")

load_dotenv()

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
    handlers=[logging.FileHandler("logs/bot.log", encoding="utf-8"), logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

Path("logs").mkdir(exist_ok=True)
Path("tmp").mkdir(exist_ok=True)
Path("data").mkdir(exist_ok=True)

conversion_queue = asyncio.Queue(maxsize=5)
active_tasks = {}
settings_db = UserSettings()
MAIN_REPLY_KEYBOARD = ReplyKeyboardMarkup([["📚 Отправить книгу", "⚙️ Настройки", "❓ Помощь"]], resize_keyboard=True)
MIN_COVER_WIDTH, MIN_COVER_HEIGHT = 330, 500


def is_zip_file(path: str) -> bool:
    try:
        with open(path, "rb") as f:
            return f.read(4) == b"PK\x03\x04"
    except:
        return False


def unpack_if_needed(input_path: str) -> str:
    if not is_zip_file(input_path):
        return input_path
    
    try:
        with zipfile.ZipFile(input_path, "r") as zf:
            fb2_files = [f for f in zf.namelist() if f.lower().endswith(".fb2")]
            if not fb2_files:
                raise ValueError("В архиве нет .fb2")
            
            extracted = Path(input_path).with_suffix(".unpacked.fb2")
            with zf.open(fb2_files[0]) as src, open(extracted, "wb") as dst:
                dst.write(src.read())
            
            with open(extracted, "rb") as f:
                if b"<?xml" not in f.read(200):
                    extracted.unlink()
                    raise ValueError("Битый архив")
            return str(extracted)
    except Exception as e:
        logger.error(f"Распаковка: {e}")
        return input_path


def resize_cover_if_needed(cover_path: str) -> bool:
    if not HAS_PILLOW:
        return False
    try:
        with Image.open(cover_path) as img:
            if img.mode in ("RGBA", "P"):
                img = img.convert("RGB")
            w, h = img.size
            if w >= MIN_COVER_WIDTH and h >= MIN_COVER_HEIGHT:
                return True
            ratio = max(MIN_COVER_WIDTH / w, MIN_COVER_HEIGHT / h)
            img.resize((int(w * ratio), int(h * ratio)), Image.Resampling.LANCZOS).save(cover_path, "JPEG", quality=90)
            return True
    except:
        return False


def extract_metadata_fallback(input_path: str) -> dict:
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
        
        title = "Без названия"
        title_match = re.search(r"<book-title[^>]*>([^<]+)</book-title>", text, re.IGNORECASE)
        if title_match:
            title = title_match.group(1).strip()
        
        return {"title": title, "authors": [author] if author != "Неизвестен" else None}
    except:
        return {"title": "Без названия", "authors": None}


def extract_metadata(input_path: str) -> dict:
    try:
        result = subprocess.run(["ebook-meta", input_path], capture_output=True, text=True, timeout=30, encoding='utf-8', errors='replace')
        metadata = {"title": None, "authors": None}
        for line in result.stdout.splitlines():
            if line.startswith("Title:") and len(line) > 6:
                val = line[6:].strip()
                if val and val.lower() != "unknown":
                    metadata["title"] = val
            elif line.startswith("Author(s):") and len(line) > 10:
                val = line[10:].strip()
                if val and val.lower() != "unknown":
                    metadata["authors"] = [a.strip() for a in val.split(",")]
        if not metadata["title"] or not metadata["authors"]:
            fallback = extract_metadata_fallback(input_path)
            metadata["title"] = metadata["title"] or fallback["title"]
            metadata["authors"] = metadata["authors"] or fallback["authors"]
        return metadata
    except:
        return extract_metadata_fallback(input_path)


def extract_cover(input_path: str, cover_path: str) -> bool:
    try:
        subprocess.run(["ebook-meta", "--get-cover", input_path, cover_path], capture_output=True, timeout=30)
        if Path(cover_path).exists() and Path(cover_path).stat().st_size > 500:
            resize_cover_if_needed(cover_path)
            return True
    except:
        pass
    
    try:
        with open(input_path, "rb") as f:
            content = f.read()
        matches = re.findall(rb'<binary[^>]+content-type="image/[^"]+"[^>]*>([^<]+)</binary>', content)
        if matches:
            data = base64.b64decode(matches[0].strip())
            if len(data) > 500:
                with open(cover_path, "wb") as f:
                    f.write(data)
                if Path(cover_path).exists() and Path(cover_path).stat().st_size > 500:
                    resize_cover_if_needed(cover_path)
                    return True
    except:
        pass
    return False


def convert_book(input_path: str, output_path: str, cover_path: str = None) -> tuple[bool, str]:
    try:
        input_abs = str(Path(input_path).resolve())
        output_abs = str(Path(output_path).resolve())
        
        if not Path(input_abs).exists() or Path(input_abs).stat().st_size == 0:
            return False, "Входной файл пустой или не найден"
        
        cmd = ["ebook-convert", input_abs, output_abs]
        if cover_path and Path(cover_path).exists() and Path(cover_path).stat().st_size > 500:
            cmd.extend(["--cover", cover_path])
        
        cmd.extend(["--output-profile", "kindle_pw3", "--pretty-print", "--no-inline-toc", "--cover-margin", "0"])
        
        result = subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=180)
        if result.returncode != 0:
            return False, f"Код {result.returncode}"
        
        output_p = Path(output_abs)
        if not output_p.exists() or output_p.stat().st_size == 0:
            return False, "Выходной файл не создан"
        
        has_cover = False
        try:
            meta = subprocess.run(["ebook-meta", str(output_p)], capture_output=True, text=True, timeout=10, encoding='utf-8', errors='replace')
            has_cover = "cover" in meta.stdout.lower()
        except:
            pass
        
        size = f"{output_p.stat().st_size / 1024:.1f} КБ"
        cover_info = " ✓ обложка" if has_cover else " ✗ без миниатюры"
        return True, f"{size}{cover_info}"
    except subprocess.TimeoutExpired:
        return False, "Таймаут"
    except Exception as e:
        return False, str(e)[:150]


async def conversion_worker(application: Application):
    logger.info("🔄 Воркер запущен")
    while True:
        try:
            task = await conversion_queue.get()
            task_id = task["task_id"]
            active_tasks[task_id]["status"] = "converting"
            
            unpacked = unpack_if_needed(task["input_path"])
            cleanup = (unpacked != task["input_path"])
            
            metadata = extract_metadata(unpacked)
            title = metadata["title"] or "Без названия"
            author = metadata["authors"][0] if metadata["authors"] else "Неизвестен"
            
            cover_path = f"{task['input_path']}.cover.jpg"
            has_cover = extract_cover(unpacked, cover_path)
            
            try:
                status = f"⏳ Конвертирую:\n<b>{title}</b>\n<i>{author}</i>"
                status += "\n✅ Обложка найдена" if has_cover else "\n⚠️ Обложка не найдена"
                await application.bot.edit_message_text(chat_id=task["user_id"], message_id=task["message_id"], text=status, parse_mode=ParseMode.HTML)
            except:
                pass
            
            success, diag = convert_book(unpacked, task["output_path"], cover_path if has_cover else None)
            
            output_p = Path(task["output_path"])
            if success and output_p.exists():
                safe_title = re.sub(r'[<>:"/\\|?*]', '', title)[:50]
                safe_author = re.sub(r'[<>:"/\\|?*]', '', author)[:30]
                filename = f"{safe_author} - {safe_title}{output_p.suffix}"
                
                await application.bot.send_document(
                    chat_id=task["user_id"],
                    document=open(output_p, "rb"),
                    filename=filename,
                    caption=f"✅ {task['output_format'].upper()}\n📚 {title}\n👤 {author}\n📦 {diag}",
                    parse_mode=ParseMode.HTML,
                )
                await application.bot.send_message(chat_id=task["user_id"], text="Файл готов для Kindle! 📚", reply_markup=MAIN_REPLY_KEYBOARD)
            else:
                await application.bot.send_message(
                    chat_id=task["user_id"],
                    text=f"❌ Ошибка <b>{title}</b>:\n<code>{diag}</code>",
                    parse_mode=ParseMode.HTML,
                    reply_markup=MAIN_REPLY_KEYBOARD
                )
            
            for p in [task["input_path"], task["output_path"], cover_path]:
                try:
                    Path(p).unlink(missing_ok=True)
                except:
                    pass
            if cleanup:
                try:
                    Path(unpacked).unlink()
                except:
                    pass
            
            conversion_queue.task_done()
            active_tasks.pop(task_id, None)
        except Exception as e:
            logger.error(f"Воркер: {e}", exc_info=True)
            await asyncio.sleep(5)


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(
        "📚 <b>KindleGarden Bot</b>\n\nОтправляй FB2/EPUB → Kindle-книга с обложкой!\nФорматы: AZW3 (рекомендуется), EPUB, MOBI",
        parse_mode=ParseMode.HTML,
        reply_markup=MAIN_REPLY_KEYBOARD
    )


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(
        "💡 <b>Совет:</b> Если конвертация падает — распакуй файл вручную через Calibre на ПК и отправь чистый .fb2",
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
    await update.message.reply_text(f"⚙️ Формат: <b>{current.upper()}</b>", parse_mode=ParseMode.HTML, reply_markup=kb)


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
    
    base = Path.cwd() / "tmp"
    sid = str(uuid4()).replace("-", "")[:12]
    in_ext = Path(fname).suffix or ".fb2"
    out_ext = f".{settings_db.get_preferred_format(update.effective_user.id)}"
    
    in_path = base / f"in_{sid}{in_ext}"
    out_path = base / f"out_{sid}{out_ext}"
    
    task = {
        "task_id": sid,
        "user_id": update.effective_user.id,
        "file_id": doc.file_id,
        "file_name": doc.file_name,
        "input_path": str(in_path),
        "output_path": str(out_path),
        "output_format": out_ext[1:],
        "status": "queued",
    }
    active_tasks[sid] = task
    
    try:
        file = await context.bot.get_file(doc.file_id)
        await file.download_to_drive(task["input_path"])
        if Path(task["input_path"]).stat().st_size == 0:
            raise ValueError("Пустой файл")
    except Exception as e:
        logger.error(f"Ошибка: {e}")
        await update.message.reply_text(f"❌ {str(e)}", reply_markup=MAIN_REPLY_KEYBOARD)
        Path(task["input_path"]).unlink(missing_ok=True)
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
        subprocess.run([tool, "--version"], capture_output=True, timeout=5)
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
    
    logger.info("🚀 Бот запущен")
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    try:
        main()
    finally:
        settings_db.close()