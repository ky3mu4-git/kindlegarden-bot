import asyncio
import logging
import os
import subprocess
import re
import base64
import zipfile
import shutil
from pathlib import Path
from uuid import uuid4
from PIL import Image  # Нужно установить: pip install Pillow
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


def is_zip_file(path: str) -> bool:
    """Проверяет, является ли файл ZIP по сигнатуре (а не по расширению)"""
    try:
        with open(path, "rb") as f:
            return f.read(4) == b"PK\x03\x04"
    except:
        return False


def unpack_if_needed(input_path: str) -> str:
    """Распаковывает FB2.ZIP в чистый FB2, возвращает путь к распакованному файлу"""
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
            
            logger.info(f"Распаковано: {extracted_path}")
            return str(extracted_path)
    except Exception as e:
        logger.error(f"Ошибка распаковки: {e}")
        return input_path


def extract_metadata(input_path: str) -> dict:
    """Извлекает метаданные из книги"""
    try:
        result = subprocess.run(
            ["ebook-meta", input_path],
            capture_output=True,
            text=True,
            timeout=30,
            encoding='utf-8',
            errors='replace'
        )
        
        metadata = {
            "title": "Без названия", 
            "authors": ["Неизвестен"],
            "series": None,
            "series_index": None
        }
        
        for line in result.stdout.splitlines():
            line = line.strip()
            if line.startswith("Title:"):
                val = line[6:].strip()
                if val and val.lower() != "unknown" and val:
                    metadata["title"] = val
            elif line.startswith("Author(s):"):
                val = line[10:].strip()
                if val and val.lower() != "unknown" and val:
                    metadata["authors"] = [a.strip() for a in val.split(",")]
            elif line.startswith("Series:"):
                val = line[7:].strip()
                if val and val.lower() != "unknown" and val:
                    metadata["series"] = val
            elif line.startswith("Series Index:"):
                val = line[13:].strip()
                if val:
                    metadata["series_index"] = val
        
        return metadata
        
    except Exception as e:
        logger.warning(f"Ошибка извлечения метаданных: {e}")
        return {
            "title": "Без названия", 
            "authors": ["Неизвестен"],
            "series": None,
            "series_index": None
        }


def extract_cover(input_path: str, cover_path: str) -> bool:
    """Извлекает обложку из книги"""
    try:
        # Метод 1: через ebook-meta
        try:
            result = subprocess.run(
                ["ebook-meta", input_path, "--get-cover", cover_path],
                capture_output=True,
                timeout=30
            )
            
            if Path(cover_path).exists() and Path(cover_path).stat().st_size > 1000:
                logger.info(f"Обложка извлечена ebook-meta: {Path(cover_path).stat().st_size} байт")
                return True
        except Exception as e:
            logger.debug(f"ebook-meta не сработал: {e}")
        
        # Метод 2: для FB2 - ручной парсинг
        if input_path.lower().endswith('.fb2'):
            try:
                with open(input_path, 'rb') as f:
                    content = f.read()
                
                # Пробуем разные кодировки
                text = None
                for encoding in ['utf-8', 'cp1251', 'koi8-r']:
                    try:
                        text = content.decode(encoding)
                        break
                    except:
                        continue
                
                if text is None:
                    text = content.decode('utf-8', errors='ignore')
                
                # Ищем все binary с изображениями
                pattern = r'<binary[^>]+content-type="image/[^"]+"[^>]*>([^<]+)</binary>'
                matches = re.findall(pattern, text, re.IGNORECASE)
                
                for match in matches:
                    try:
                        image_data = base64.b64decode(match.strip())
                        if len(image_data) > 5000:  # Минимальный размер
                            with open(cover_path, 'wb') as f:
                                f.write(image_data)
                            
                            if Path(cover_path).stat().st_size > 1000:
                                logger.info(f"Обложка извлечена из FB2: {Path(cover_path).stat().st_size} байт")
                                return True
                    except Exception as e:
                        logger.debug(f"Не удалось декодировать обложку: {e}")
                        continue
            except Exception as e:
                logger.debug(f"Ошибка при парсинге FB2: {e}")
        
        return False
        
    except Exception as e:
        logger.error(f"Ошибка извлечения обложки: {e}")
        return False


def optimize_cover_for_kindle(cover_path: str) -> bool:
    """Оптимизирует обложку для Kindle"""
    try:
        if not Path(cover_path).exists():
            return False
        
        with Image.open(cover_path) as img:
            # Конвертируем в RGB если нужно
            if img.mode in ('RGBA', 'LA', 'P'):
                rgb_img = Image.new('RGB', img.size, (255, 255, 255))
                if img.mode == 'P':
                    img = img.convert('RGBA')
                rgb_img.paste(img, mask=img.split()[-1] if img.mode == 'RGBA' else None)
                img = rgb_img
            
            # Оптимальные размеры для Kindle
            # Минимум 600x800 для хорошего отображения
            target_width = 800
            target_height = 1200
            
            # Сохраняем пропорции
            img.thumbnail((target_width, target_height), Image.Resampling.LANCZOS)
            
            # Сохраняем в высоком качестве
            optimized_path = cover_path.replace('.jpg', '_optimized.jpg')
            img.save(optimized_path, 'JPEG', quality=90, optimize=True)
            
            # Заменяем оригинальную обложку
            shutil.move(optimized_path, cover_path)
            
            logger.info(f"Обложка оптимизирована: {img.size[0]}x{img.size[1]}")
            return True
            
    except Exception as e:
        logger.warning(f"Не удалось оптимизировать обложку: {e}")
        return False


def convert_book_for_kindle(input_path: str, output_path: str, metadata: dict, cover_path: str = None) -> tuple[bool, str]:
    """Конвертация книги с метаданными для Kindle"""
    try:
        cmd = ["ebook-convert", input_path, output_path]
        
        # Добавляем метаданные
        if metadata.get("title"):
            # Экранируем кавычки в названии
            title = metadata["title"].replace('"', '\\"')
            cmd.extend(["--title", title])
        
        if metadata.get("authors"):
            # Объединяем авторов, экранируем кавычки
            authors = ", ".join(metadata["authors"])
            authors = authors.replace('"', '\\"')
            cmd.extend(["--authors", authors])
        
        if metadata.get("series"):
            series = metadata["series"].replace('"', '\\"')
            cmd.extend(["--series", series])
        
        if metadata.get("series_index"):
            cmd.extend(["--series-index", str(metadata["series_index"])])
        
        # Добавляем обложку если есть
        if cover_path and Path(cover_path).exists() and Path(cover_path).stat().st_size > 1000:
            # Оптимизируем обложку для Kindle
            optimize_cover_for_kindle(cover_path)
            
            cmd.extend(["--cover", cover_path])
            logger.info(f"Конвертация с обложкой ({Path(cover_path).stat().st_size} байт)")
        else:
            cmd.append("--no-default-epub-cover")
            logger.info("Конвертация без обложки")
        
        # ОПЦИИ ДЛЯ МИНИАТЮРЫ В KINDLE
        output_ext = Path(output_path).suffix.lower()
        
        if output_ext == ".mobi":
            # Критические опции для MOBI (старые Kindle)
            cmd.extend([
                "--mobi-keep-original-images",
                "--share-not-sync",           # Для отображения миниатюры
                "--personal-doc", "Y",        # Разрешаем личные документы
                "--mobi-file-type", "both",   # Для совместимости
                "--dont-compress",            # Не сжимать сильно
            ])
        elif output_ext == ".azw3":
            # Опции для AZW3 (новые Kindle)
            cmd.extend([
                "--dont-compress",
                "--no-inline-toc",            # Без встроенного оглавления
                "--disable-font-rescaling",
            ])
        
        # Общие опции для улучшения метаданных
        cmd.extend([
            "--metadata",                     # Явно указываем метаданные
            "--smarten-punctuation",          # Улучшаем пунктуацию
            "--chapter", "//h:h1",            # Главы по h1
            "--chapter-mark", "pagebreak",    # Разрывы страниц для глав
            "--page-breaks-before", "//*[name()='h1' or name()='h2']",
        ])
        
        logger.info(f"Выполняю: {' '.join(cmd[:10])}...")
        
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=300,
            encoding='utf-8',
            errors='replace'
        )
        
        # Логируем вывод для отладки
        if result.stdout:
            logger.debug(f"Stdout: {result.stdout[:200]}")
        if result.stderr:
            # Фильтруем стандартные предупреждения
            error_lines = [line for line in result.stderr.split('\n') 
                          if line.strip() and not line.startswith("Usage:")]
            if error_lines:
                logger.warning(f"Stderr: {error_lines[0][:200]}")
        
        output_p = Path(output_path)
        if result.returncode != 0 or not output_p.exists() or output_p.stat().st_size == 0:
            error_msg = f"Код ошибки: {result.returncode}"
            if result.stderr:
                for line in result.stderr.split('\n'):
                    if line.strip() and not line.startswith("Usage:"):
                        error_msg = line.strip()[:200]
                        break
            return False, error_msg
        
        # ПРОВЕРЯЕМ МЕТАДАННЫЕ В ВЫХОДНОМ ФАЙЛЕ
        meta_check = ""
        try:
            check_result = subprocess.run(
                ["ebook-meta", output_path],
                capture_output=True,
                text=True,
                timeout=30
            )
            
            # Ищем название и автора
            has_title = False
            has_author = False
            has_cover = False
            
            for line in check_result.stdout.split('\n'):
                line = line.strip()
                if line.startswith("Title:"):
                    val = line[6:].strip()
                    if val and val.lower() != "unknown" and val:
                        has_title = True
                elif line.startswith("Author(s):"):
                    val = line[10:].strip()
                    if val and val.lower() != "unknown" and val:
                        has_author = True
                elif "Cover:" in line or "Has cover:" in line:
                    if "yes" in line.lower() or "true" in line.lower():
                        has_cover = True
            
            meta_check_parts = []
            if has_title:
                meta_check_parts.append("✓ название")
            else:
                meta_check_parts.append("✗ название")
                
            if has_author:
                meta_check_parts.append("✓ автор")
            else:
                meta_check_parts.append("✗ автор")
                
            if has_cover:
                meta_check_parts.append("✓ обложка")
            else:
                meta_check_parts.append("✗ обложка")
            
            meta_check = " | " + " | ".join(meta_check_parts)
            
        except Exception as e:
            logger.debug(f"Не удалось проверить метаданные: {e}")
            meta_check = ""
        
        size_info = f"{output_p.stat().st_size / 1024 / 1024:.2f} МБ"
        return True, f"{size_info}{meta_check}"
        
    except subprocess.TimeoutExpired:
        return False, "Таймаут конвертации"
    except Exception as e:
        logger.error(f"Ошибка конвертации: {e}", exc_info=True)
        return False, str(e)[:150]


async def conversion_worker(application: Application):
    logger.info("🔄 Воркер запущен")
    
    while True:
        try:
            task = await conversion_queue.get()
            task_id = task["task_id"]
            active_tasks[task_id]["status"] = "converting"
            
            # Распаковываем если нужно
            unpacked_path = unpack_if_needed(task["input_path"])
            cleanup_unpacked = (unpacked_path != task["input_path"])
            
            # Извлекаем метаданные
            metadata = extract_metadata(unpacked_path)
            title = metadata["title"] or "Без названия"
            author = metadata["authors"][0] if metadata["authors"] else "Неизвестен"
            
            # Извлекаем обложку
            cover_path = f"{task['input_path']}.cover.jpg"
            has_cover = extract_cover(unpacked_path, cover_path)
            
            # Обновляем статус
            try:
                status = f"⏳ Конвертирую:\n<b>{title}</b>\n<i>{author}</i>"
                if has_cover:
                    cover_size = Path(cover_path).stat().st_size if Path(cover_path).exists() else 0
                    status += f"\n✅ Обложка найдена ({cover_size/1024:.1f} КБ)"
                    status += f"\n🔧 Оптимизирую для Kindle..."
                else:
                    status += "\n⚠️ Обложка не найдена"
                
                await application.bot.edit_message_text(
                    chat_id=task["user_id"],
                    message_id=task["message_id"],
                    text=status,
                    parse_mode=ParseMode.HTML
                )
            except Exception as e:
                logger.warning(f"Не удалось обновить статус: {e}")
            
            # Конвертируем с улучшенной функцией
            success, diag = convert_book_for_kindle(
                unpacked_path,
                task["output_path"],
                metadata,
                cover_path if has_cover else None
            )
            
            # Отправляем результат
            output_p = Path(task["output_path"])
            if success and output_p.exists():
                safe_title = re.sub(r'[<>:"/\\|?*]', '', title)[:50]
                safe_author = re.sub(r'[<>:"/\\|?*]', '', author)[:30]
                filename = f"{safe_author} - {safe_title}{output_p.suffix}"
                
                caption = f"✅ Конвертация завершена\n📚 {title}\n👤 {author}\n💾 {diag}"
                
                # Дополнительная информация о миниатюрах
                extra_info = ""
                if task["output_format"] == "mobi":
                    extra_info = (
                        "\n\n📱 <b>Для отображения миниатюры на Kindle:</b>\n"
                        "1. Отправьте файл на email Kindle\n"
                        "2. В теме письма добавьте <code>convert</code>\n"
                        "3. Или используйте Calibre для копирования"
                    )
                elif task["output_format"] == "azw3":
                    extra_info = (
                        "\n\n📱 <b>Для лучшего отображения:</b>\n"
                        "• Используйте кабель USB\n"
                        "• Или отправьте через email"
                    )
                
                await application.bot.send_document(
                    chat_id=task["user_id"],
                    document=open(output_p, "rb"),
                    filename=filename,
                    caption=caption,
                    parse_mode=ParseMode.HTML,
                )
                
                if extra_info:
                    await application.bot.send_message(
                        chat_id=task["user_id"],
                        text=extra_info,
                        parse_mode=ParseMode.HTML
                    )
                
                await application.bot.send_message(
                    chat_id=task["user_id"],
                    text="Файл готов для Kindle! 📚",
                    reply_markup=MAIN_REPLY_KEYBOARD
                )
            else:
                error_msg = (
                    f"❌ Ошибка конвертации <b>{title}</b>:\n"
                    f"<code>{diag}</code>\n\n"
                    f"<b>Попробуйте:</b>\n"
                    f"1. Использовать формат AZW3 вместо MOBI\n"
                    f"2. Отправить книгу заново\n"
                    f"3. Проверить исходный файл"
                )
                await application.bot.send_message(
                    chat_id=task["user_id"],
                    text=error_msg,
                    parse_mode=ParseMode.HTML,
                    reply_markup=MAIN_REPLY_KEYBOARD
                )
            
            # Чистим файлы
            cleanup_files = [
                task["input_path"],
                task["output_path"],
                cover_path,
                f"{task['input_path']}.cover.jpg",
                f"{unpacked_path}.cover.jpg"
            ]
            
            for p in cleanup_files:
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
        "📚 <b>KindleGarden Bot v3</b>\n\n"
        "<b>Улучшенная конвертация с метаданными!</b>\n\n"
        "✅ <b>Что нового:</b>\n"
        "• Правильное заполнение названия и автора\n"
        "• Оптимизация обложек для Kindle\n"
        "• Лучшая поддержка миниатюр\n\n"
        "<b>Форматы:</b>\n"
        "• FB2 / FB2.ZIP\n"
        "• EPUB\n\n"
        "<b>Совет:</b> Используйте AZW3 для новых Kindle\n"
        "для гарантированного отображения обложек.",
        parse_mode=ParseMode.HTML,
        reply_markup=MAIN_REPLY_KEYBOARD
    )


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    message = (
        "📚 <b>KindleGarden - помощь</b>\n\n"
        
        "✅ <b>Как работают метаданные и обложки:</b>\n"
        "1. Бот извлекает название, автора и обложку\n"
        "2. Оптимизирует обложку для Kindle (800x1200)\n"
        "3. Встраивает метаданные в книгу\n"
        "4. Использует специальные настройки для миниатюр\n\n"
        
        "🖼️ <b>Почему миниатюра может не отображаться:</b>\n"
        "• <b>MOBI</b>: Требуется отправка через email с темой 'convert'\n"
        "• <b>AZW3</b>: Обычно работает через USB\n"
        "• Размер обложки менее 600x800 пикселей\n"
        "• Старый Kindle (до 5 поколения)\n\n"
        
        "⚙️ <b>Рекомендации по форматам:</b>\n"
        "• <b>AZW3</b> - лучшая поддержка, новые Kindle\n"
        "• <b>MOBI</b> - старые Kindle, отправка через email\n"
        "• <b>EPUB</b> - другие устройства\n\n"
        
        "📧 <b>Для MOBI миниатюр:</b>\n"
        "Отправьте файл на email Kindle\n"
        "Тема письма: <code>convert</code>\n"
        "Или используйте Calibre\n\n"
        
        "⏱️ <b>Ограничения:</b>\n"
        "• Размер: до 50 МБ\n"
        "• Время: до 5 минут\n"
        "• Очередь: 5 файлов"
    )
    await update.message.reply_text(
        message,
        parse_mode=ParseMode.HTML,
        reply_markup=MAIN_REPLY_KEYBOARD
    )


async def settings_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id
    current = settings_db.get_preferred_format(user_id)
    
    # Описание форматов с советами по миниатюрам
    formats_info = {
        "azw3": "📘 AZW3 - лучшие миниатюры (USB, новые Kindle)",
        "mobi": "📙 MOBI - совместимость (email, старые Kindle)",
        "epub": "📖 EPUB - другие читалки"
    }
    
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton(
            f"{'✅ ' if fmt == current else ''}{desc}", 
            callback_data=f"setfmt:{fmt}"
        )] for fmt, desc in formats_info.items()
    ])
    
    await update.message.reply_text(
        f"⚙️ <b>Текущий формат:</b> {current.upper()}\n\n"
        f"<b>Советы по миниатюрам:</b>\n"
        f"• AZW3 - миниатюры через USB\n"
        f"• MOBI - миниатюры через email\n"
        f"• EPUB - без гарантий для Kindle",
        parse_mode=ParseMode.HTML,
        reply_markup=kb
    )


async def handle_format_setting(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    _, fmt = query.data.split(":")
    
    settings_db.set_preferred_format(update.effective_user.id, fmt)
    
    # Совет в зависимости от формата
    advice = {
        "mobi": "\n\n⚠️ <b>Для миниатюр MOBI:</b>\nОтправляйте файлы на email Kindle\nс темой 'convert'",
        "azw3": "\n\n✅ <b>Для миниатюр AZW3:</b>\nИспользуйте USB кабель\nили Calibre для копирования",
        "epub": "\n\n📖 <b>Для EPUB:</b>\nФормат для других устройств,\nне гарантирует миниатюры на Kindle"
    }.get(fmt, "")
    
    await query.edit_message_text(
        f"✅ Формат изменен на <b>{fmt.upper()}</b>{advice}",
        parse_mode=ParseMode.HTML
    )
    await query.message.reply_text(
        "Выберите действие:",
        reply_markup=MAIN_REPLY_KEYBOARD
    )


async def handle_document(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    doc = update.message.document
    fname = doc.file_name.lower() if doc.file_name else ""
    
    # Проверяем формат
    supported_formats = ['.fb2', '.fb2.zip', '.epub']
    if not any(fname.endswith(fmt) for fmt in supported_formats):
        await update.message.reply_text(
            "⚠️ Поддерживаются только:\n"
            "• FB2 (.fb2)\n"
            "• FB2.ZIP (.fb2.zip)\n"
            "• EPUB (.epub)",
            reply_markup=MAIN_REPLY_KEYBOARD
        )
        return

    # Проверяем размер
    if doc.file_size > 50 * 1024 * 1024:
        await update.message.reply_text(
            "⚠️ Максимальный размер файла - 50 МБ",
            reply_markup=MAIN_REPLY_KEYBOARD
        )
        return

    if conversion_queue.full():
        await update.message.reply_text(
            f"⏸️ Очередь заполнена ({conversion_queue.qsize()}/5)\nПожалуйста, подождите...",
            reply_markup=MAIN_REPLY_KEYBOARD
        )
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
        
        logger.info(f"Файл принят: {doc.file_name} ({input_size / 1024 / 1024:.2f} МБ)")
        
    except Exception as e:
        logger.error(f"Ошибка загрузки файла: {e}")
        await update.message.reply_text(
            f"❌ Ошибка загрузки файла",
            reply_markup=MAIN_REPLY_KEYBOARD
        )
        try:
            Path(task["input_path"]).unlink(missing_ok=True)
        except:
            pass
        return

    await conversion_queue.put(task)
    
    format_advice = {
        "azw3": "AZW3 (миниатюры через USB)",
        "mobi": "MOBI (миниатюры через email)",
        "epub": "EPUB (другие устройства)"
    }
    
    msg = await update.message.reply_text(
        f"✅ Добавлено в очередь ({conversion_queue.qsize()}/5)\n"
        f"Формат: <b>{task['output_format'].upper()}</b>\n"
        f"{format_advice.get(task['output_format'], '')}\n\n"
        f"⏳ Извлекаю метаданные и обложку...",
        parse_mode=ParseMode.HTML
    )
    task["message_id"] = msg.message_id


async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    t = update.message.text.strip()
    if t == "📚 Отправить книгу":
        await update.message.reply_text(
            "📎 Отправьте FB2 или EPUB файл\n"
            "Максимальный размер: 50 МБ\n\n"
            "✅ <b>Новые возможности:</b>\n"
            "• Заполнение названия и автора\n"
            "• Оптимизация обложек\n"
            "• Лучшие миниатюры для Kindle",
            parse_mode=ParseMode.HTML,
            reply_markup=MAIN_REPLY_KEYBOARD
        )
    elif t == "⚙️ Настройки":
        await settings_menu(update, context)
    elif t == "❓ Помощь":
        await help_command(update, context)
    else:
        await update.message.reply_text(
            "Используйте меню ниже 👇",
            reply_markup=MAIN_REPLY_KEYBOARD
        )


async def post_init(app: Application) -> None:
    """Проверяем наличие необходимых инструментов"""
    required_tools = ["ebook-convert", "ebook-meta"]
    
    for tool in required_tools:
        try:
            result = subprocess.run([tool, "--version"], capture_output=True, timeout=5)
            if result.returncode == 0:
                logger.info(f"✅ {tool} доступен")
            else:
                logger.error(f"❌ {tool} не работает")
                raise RuntimeError(f"{tool} не работает. Установите Calibre: sudo apt install calibre")
        except Exception as e:
            logger.error(f"❌ {tool} не найден: {e}")
            raise RuntimeError(f"{tool} не найден. Установите Calibre: sudo apt install calibre")
    
    # Проверяем наличие Pillow (PIL)
    try:
        import PIL
        logger.info("✅ Pillow (PIL) доступен")
    except ImportError:
        logger.warning("❌ Pillow не установлен. Обложки не будут оптимизированы.")
        logger.info("Установите: pip install Pillow")
    
    asyncio.create_task(conversion_worker(app))
    logger.info("✅ Бот готов к работе с улучшенной конвертацией")


def main() -> None:
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    if not token:
        raise RuntimeError("TELEGRAM_BOT_TOKEN не найден в .env файле")
    
    app = Application.builder().token(token).post_init(post_init).build()
    
    # Добавляем обработчики
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("settings", settings_menu))
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(MessageHandler(filters.Document.ALL, handle_document))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))
    app.add_handler(CallbackQueryHandler(handle_format_setting, pattern="^setfmt:"))
    
    logger.info("🚀 Бот запущен с поддержкой метаданных и оптимизированных обложек")
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Бот остановлен")
    finally:
        settings_db.close()