import asyncio
import logging
import os
import subprocess
import re
import base64
import zipfile
import sys
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
    """Распаковывает FB2.ZIP в чистый FB2 с проверкой целостности"""
    input_p = Path(input_path)
    
    if not is_zip_file(input_path):
        logger.info(f"Файл не является архивом: {input_path}")
        # Проверяем, что это валидный XML
        try:
            with open(input_path, "rb") as f:
                header = f.read(200)
                if b"<?xml" not in header and b"<FictionBook" not in header:
                    logger.warning("Файл не содержит валидной структуры FB2")
        except Exception as e:
            logger.warning(f"Ошибка проверки FB2: {e}")
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
            
            # 🔑 КРИТИЧЕСКАЯ ПРОВЕРКА: распакованный файл должен быть валидным XML
            with open(extracted_path, "rb") as f:
                header = f.read(200)
                if b"<?xml" not in header and b"<FictionBook" not in header:
                    logger.error("Распакованный файл не является валидным FB2 (битый архив?)")
                    extracted_path.unlink(missing_ok=True)
                    raise ValueError("Распакованный файл повреждён или не является FB2")
            
            logger.info(f"Распаковано успешно: {extracted_path}")
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
                logger.info(f"Обложка достаточно большая: {width}×{height}")
                return True
            
            ratio = max(MIN_COVER_WIDTH / width, MIN_COVER_HEIGHT / height)
            new_width = int(width * ratio)
            new_height = int(height * ratio)
            
            logger.info(f"Масштабирование: {width}×{height} → {new_width}×{new_height}")
            img_resized = img.resize((new_width, new_height), Image.Resampling.LANCZOS)
            img_resized.save(cover_path, "JPEG", quality=90, optimize=True)
            
            new_size = cover_p.stat().st_size
            logger.info(f"Обложка масштабирована: {new_width}×{new_height} ({new_size} байт)")
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
        
        logger.info(f"Fallback метаданные: автор={author}, название={title}")
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
            logger.info("Стандартный способ не дал результатов, используем парсинг XML")
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
                logger.info(f"✅ Обложка извлечена стандартным способом ({cover_p.stat().st_size} байт)")
                resize_cover_if_needed(cover_path)
                return True
        except:
            pass
        
        logger.info("🔍 Ручной парсинг обложки...")
        try:
            with open(input_path, "rb") as f:
                content = f.read()
            
            pattern = rb'<binary[^>]+content-type="image/[^"]+"[^>]*>([^<]+)</binary>'
            matches = re.findall(pattern, content)
            
            if not matches:
                logger.info("❌ Обложка не найдена: нет <binary> с изображением")
                return False
            
            try:
                image_data = base64.b64decode(matches[0].strip(), validate=True)
            except:
                image_data = base64.b64decode(matches[0].strip())
            
            if len(image_data) < 500:
                logger.info(f"❌ Слишком маленькие данные ({len(image_data)} байт)")
                return False
            
            with open(cover_path, "wb") as f:
                f.write(image_data)
            
            cover_p = Path(cover_path)
            if cover_p.exists() and cover_p.stat().st_size > 500:
                logger.info(f"✅ Обложка извлечена ручным парсингом ({cover_p.stat().st_size} байт)")
                resize_cover_if_needed(cover_path)
                return True
                
        except Exception as e:
            logger.warning(f"Ошибка ручного парсинга: {e}")
            return False
            
    except Exception as e:
        logger.warning(f"Общая ошибка извлечения обложки: {e}")
        return False


def convert_book(input_path: str, output_path: str, cover_path: str = None) -> tuple[bool, str]:
    """Конвертация с детальной диагностикой ошибок"""
    try:
        input_abs = str(Path(input_path).resolve())
        output_abs = str(Path(output_path).resolve())
        cover_abs = str(Path(cover_path).resolve()) if cover_path else None
        
        # 🔑 КРИТИЧЕСКАЯ ПРОВЕРКА: существует ли входной файл и не пустой ли он
        input_p = Path(input_abs)
        if not input_p.exists():
            return False, f"Входной файл не найден: {input_abs}"
        if input_p.stat().st_size == 0:
            return False, f"Входной файл пустой: {input_abs} ({input_p.stat().st_size} байт)"
        
        # Проверяем структуру файла (должен быть XML для FB2)
        try:
            with open(input_abs, "rb") as f:
                header = f.read(200)
                if b"<?xml" not in header and b"<FictionBook" not in header:
                    logger.warning("Входной файл не содержит валидной структуры FB2")
        except Exception as e:
            logger.warning(f"Ошибка проверки структуры: {e}")
        
        output_ext = Path(output_path).suffix.lower()
        is_kindle_format = output_ext in (".azw3", ".mobi")
        
        cmd = ["ebook-convert", input_abs, output_abs]
        
        if cover_abs and Path(cover_abs).exists():
            cover_size = Path(cover_abs).stat().st_size
            if cover_size > 500:
                cmd.extend(["--cover", cover_abs])
                logger.info(f"✅ Используем обложку ({cover_size} байт)")
            else:
                logger.warning(f"⚠️ Обложка слишком маленькая ({cover_size} байт)")
        
        if is_kindle_format:
            cmd.extend([
                "--output-profile", "kindle_pw3",
                "--pretty-print",
                "--no-inline-toc",
                "--mobi-keep-original-images",
                "--cover-margin", "0",
            ])
        else:
            cmd.extend([
                "--output-profile", "tablet",
                "--pretty-print",
            ])
        
        logger.info(f"Конвертация: {Path(input_abs).name} → {Path(output_abs).name}")
        logger.debug(f"Полная команда: {' '.join(cmd)}")
        
        # 🔑 ЗАПУСК С ДЕТАЛЬНЫМ ВЫВОДОМ ОШИБОК
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
            # 🔑 ДЕТАЛЬНАЯ ДИАГНОСТИКА
            error_msg = f"Код {result.returncode}\n"
            if result.stderr:
                error_msg += f"STDERR (первые 800 символов):\n{result.stderr[:800]}"
            else:
                error_msg += "STDERR пустой — возможно, проблема с путями или правами"
            logger.error(f"Ошибка конвертации:\n{error_msg}")
            return False, error_msg[:500]  # Обрезаем для отправки в телеграм
        
        if not output_p.exists() or output_p.stat().st_size == 0:
            return False, f"Выходной файл не создан ({output_p.stat().st_size if output_p.exists() else 'N/A'} байт)"
        
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
        return False, f"{type(e).__name__}: {str(e)[:200]}"


async def conversion_worker(application: Application):
    logger.info("🔄 Воркер запущен")
    
    while True:
        try:
            task = await conversion_queue.get()
            task_id = task["task_id"]
            active_tasks[task_id]["status"] = "converting"
            
            unpacked_path = unpack_if_needed(task["input_path"])
            cleanup_unpacked = (unpacked_path != task["input_path"])
            
            # 🔑 ДОП. ПРОВЕРКА: существует ли распакованный файл
            if not Path(unpacked_path).exists():
                raise FileNotFoundError(f"Распакованный файл не найден: {unpacked_path}")
            if Path(unpacked_path).stat().st_size == 0:
                raise ValueError(f"Распакованный файл пустой: {unpacked_path}")
            
            metadata = extract_metadata(unpacked_path)
            title = metadata["title"] or "Без названия"
            author = metadata["authors"][0] if metadata["authors"] else "Неизвестен"
            
            cover_path = f"{task['input_path']}.cover.jpg"
            has_cover = extract_cover(unpacked_path, cover_path)
            
            try:
                status = f"⏳ Конвертирую:\n<b>{title}</b>\n<i>{author}</i>"
                if has_cover:
                    status += "\n✅ Обложка найдена и подготовлена"
                else:
                    status += "\n⚠️ Обложка не найдена во входном файле"
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
                if not has_cover:
                    caption += "\n\n⚠️ Обложка отсутствовала во входном файле"
                
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
                # 🔑 ОТПРАВКА ДЕТАЛЬНОЙ ДИАГНОСТИКИ
                error_text = (
                    f"❌ Ошибка конвертации <b>{title}</b>:\n"
                    f"<code>{diag}</code>\n\n"
                    f"💡 Возможные причины:\n"
                    f"• Повреждённый архив FB2\n"
                    f"• Некорректная структура файла\n"
                    f"• Проблема с путями (слишком длинные имена)\n\n"
                    f"Попробуй:\n"
                    f"1. Распаковать файл вручную через Calibre на ПК\n"
                    f"2. Сохранить как чистый FB2 (без архивации)\n"
                    f"3. Отправить заново"
                )
                await application.bot.send_message(
                    chat_id=task["user_id"],
                    text=error_text,
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
            error_msg = f"❌ Критическая ошибка: {type(e).__name__}\n<code>{str(e)[:300]}</code>"
            try:
                await application.bot.send_message(
                    chat_id=task["user_id"],
                    text=error_msg,
                    parse_mode=ParseMode.HTML,
                    reply_markup=MAIN_REPLY_KEYBOARD
                )
            except:
                pass
            await asyncio.sleep(5)


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    message = (
        "📚 <b>KindleGarden Bot</b>\n\n"
        "Отправляй FB2/EPUB → получаешь книгу для Kindle!\n\n"
        "✨ Особенности:\n"
        "• Автоматическое извлечение и масштабирование обложек (330×500)\n"
        "• Поддержка сжатых файлов (.fb2.zip)\n"
        "• Корректное отображение миниатюры в библиотеке Kindle\n"
        "• Очередь обработки (макс. 5 файлов)"
    )
    await update.message.reply_text(
        message,
        parse_mode=ParseMode.HTML,
        reply_markup=MAIN_REPLY_KEYBOARD
    )


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    message = (
        "📚 <b>Инструкция:</b>\n\n"
        "✅ <b>Работает стабильно:</b>\n"
        "• Чистые FB2 (.fb2)\n"
        "• EPUB\n\n"
        "⚠️ <b>Требует подготовки:</b>\n"
        "• Сжатые FB2 (.fb2.zip) — бот распаковывает автоматически,\n"
        "  но некоторые архивы могут быть повреждены.\n"
        "  Если ошибка — распакуй файл вручную через Calibre на ПК\n"
        "  и отправь чистый .fb2.\n\n"
        "💡 <b>Совет:</b> Для 100% гарантии конвертации:\n"
        "1. Открой файл в Calibre (на ПК)\n"
        "2. ПКМ → «Сохранить на диск»\n"
        "3. Отправь полученный файл в бота"
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
    
    if not (filename.endswith(".fb2") or filename.endswith(".fb2.zip") or filename.endswith(".epub")):
        await update.message.reply_text(
            "⚠️ Принимаю только FB2 и EPUB файлы (.fb2, .fb2.zip, .epub)",
            reply_markup=MAIN_REPLY_KEYBOARD
        )
        return

    if document.file_size > 10 * 1024 * 1024:
        await update.message.reply_text(
            "⚠️ Файл слишком большой (максимум 10 МБ для малинки 3)",
            reply_markup=MAIN_REPLY_KEYBOARD
        )
        return

    if conversion_queue.full():
        await update.message.reply_text(
            f"⏸️ Очередь заполнена ({conversion_queue.qsize()}/5 файлов).\nПопробуй через минуту.",
            reply_markup=MAIN_REPLY_KEYBOARD
        )
        return

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

    try:
        file = await context.bot.get_file(document.file_id)
        await file.download_to_drive(task_info["input_path"])
        
        input_size = Path(task_info["input_path"]).stat().st_size
        if input_size == 0:
            raise ValueError("Файл пустой (0 байт)")
        
        # Проверка структуры для FB2
        if input_ext.lower() == ".fb2":
            with open(task_info["input_path"], "rb") as f:
                header = f.read(200)
                is_xml = b"<?xml" in header or b"<FictionBook" in header
                is_zip = header.startswith(b"PK\x03\x04")
                if not (is_xml or is_zip):
                    raise ValueError("Файл не является валидным FB2 или архивом FB2")
        
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
            "💡 Бот автоматически обработает обложку и метаданные",
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
    tools = ["ebook-convert", "ebook-meta"]
    for tool in tools:
        try:
            subprocess.run([tool, "--version"], capture_output=True, timeout=5)
            logger.info(f"✅ {tool} доступен")
        except Exception as e:
            logger.error(f"❌ {tool} не установлен: {e}")
            raise RuntimeError(f"Требуется {tool}. Выполни: sudo apt install calibre")
    
    if not HAS_PILLOW:
        logger.warning("⚠️  Pillow не установлен — обложки не будут масштабироваться")
    
    asyncio.create_task(conversion_worker(application))
    logger.info("✅ Воркер запущен с расширенной диагностикой")


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

    logger.info("🚀 Бот запущен с защитой от битых архивов и детальной диагностикой")
    application.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    try:
        main()
    finally:
        settings_db.close()