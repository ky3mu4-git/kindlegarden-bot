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
from xml.etree import ElementTree as ET
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


def extract_cover_improved(input_path: str, cover_path: str) -> bool:
    """Улучшенное извлечение обложки с несколькими методами"""
    try:
        # Метод 1: ebook-meta (самый надежный) - ПРАВИЛЬНЫЙ ПОРЯДОК АРГУМЕНТОВ
        try:
            subprocess.run(
                ["ebook-meta", input_path, "--get-cover", cover_path],
                capture_output=True,
                timeout=30
            )
            if Path(cover_path).exists() and Path(cover_path).stat().st_size > 1000:
                logger.info(f"✅ Обложка извлечена ebook-meta ({Path(cover_path).stat().st_size} байт)")
                return True
        except Exception as e:
            logger.warning(f"ebook-meta не сработал: {e}")
        
        # Метод 2: для FB2 - ручной парсинг
        if input_path.lower().endswith('.fb2'):
            logger.info("🔍 Ручной парсинг FB2 для обложки...")
            try:
                with open(input_path, "rb") as f:
                    content = f.read()
                
                # Декодируем с правильной кодировкой
                for enc in ["utf-8", "cp1251", "koi8-r"]:
                    try:
                        text_content = content.decode(enc)
                        break
                    except:
                        continue
                else:
                    text_content = content.decode("utf-8", errors="ignore")
                
                # Ищем coverpage
                coverpage_match = re.search(r'<coverpage>.*?<image[^>]+l:href=["\']#([^"\']+)["\'][^>]*>.*?</coverpage>', 
                                          text_content, re.DOTALL | re.IGNORECASE)
                
                if coverpage_match:
                    cover_id = coverpage_match.group(1)
                    logger.info(f"Найдена ссылка на обложку: #{cover_id}")
                    
                    # Ищем binary с этим id
                    binary_pattern = f'<binary[^>]+id=["\']{re.escape(cover_id)}["\'][^>]*>([^<]+)</binary>'
                    binary_match = re.search(binary_pattern, text_content, re.IGNORECASE)
                    
                    if binary_match:
                        try:
                            image_data = base64.b64decode(binary_match.group(1).strip())
                            with open(cover_path, "wb") as f:
                                f.write(image_data)
                            
                            if Path(cover_path).stat().st_size > 1000:
                                logger.info(f"✅ Обложка найдена по coverpage: {cover_id} ({Path(cover_path).stat().st_size} байт)")
                                return True
                        except Exception as e:
                            logger.warning(f"Ошибка декодирования обложки: {e}")
                
                # Ищем любой binary с изображением (fallback)
                binary_pattern = r'<binary[^>]+content-type=["\']image/(jpeg|jpg|png)["\'][^>]*>([^<]+)</binary>'
                all_binaries = re.findall(binary_pattern, text_content, re.IGNORECASE)
                
                for img_type, binary_data in all_binaries:
                    try:
                        image_data = base64.b64decode(binary_data.strip())
                        if len(image_data) > 10000:  # Берем только большие изображения (>10KB)
                            with open(cover_path, "wb") as f:
                                f.write(image_data)
                            
                            if Path(cover_path).stat().st_size > 1000:
                                logger.info(f"✅ Обложка найдена в binary данных ({Path(cover_path).stat().st_size} байт)")
                                return True
                    except Exception as e:
                        logger.debug(f"Не удалось декодировать binary: {e}")
                        continue
                        
            except Exception as e:
                logger.warning(f"Ошибка парсинга FB2: {e}")
        
        # Метод 3: для EPUB - используем ebook-convert
        elif input_path.lower().endswith('.epub'):
            logger.info("🔍 Извлечение обложки из EPUB...")
            try:
                # Используем ebook-convert для извлечения обложки
                temp_cover = cover_path + ".temp.jpg"
                cmd = ["ebook-convert", input_path, temp_cover, "--dont-output"]
                
                result = subprocess.run(cmd, capture_output=True, timeout=60)
                
                # Проверяем несколько возможных мест
                possible_covers = [
                    temp_cover,
                    cover_path,
                    os.path.join(os.path.dirname(input_path), "cover.jpg"),
                ]
                
                for possible_path in possible_covers:
                    if Path(possible_path).exists() and Path(possible_path).stat().st_size > 1000:
                        if possible_path != cover_path:
                            import shutil
                            shutil.copy2(possible_path, cover_path)
                        logger.info(f"✅ Обложка извлечена из EPUB ({Path(cover_path).stat().st_size} байт)")
                        # Удаляем временные файлы
                        for p in [temp_cover, os.path.join(os.path.dirname(input_path), "cover.jpg")]:
                            try:
                                if p != cover_path and Path(p).exists():
                                    Path(p).unlink()
                            except:
                                pass
                        return True
                
                # Очистка временных файлов
                for p in [temp_cover, os.path.join(os.path.dirname(input_path), "cover.jpg")]:
                    try:
                        if Path(p).exists():
                            Path(p).unlink()
                    except:
                        pass
                    
            except Exception as e:
                logger.warning(f"Ошибка извлечения из EPUB: {e}")
        
        # Метод 4: попробуем через calibre напрямую
        try:
            # Пробуем еще раз ebook-meta с правильными аргументами
            import tempfile
            with tempfile.NamedTemporaryFile(suffix='.jpg', delete=False) as tmp:
                tmp_cover = tmp.name
            
            result = subprocess.run(
                ["ebook-meta", input_path, "--get-cover", tmp_cover],
                capture_output=True,
                timeout=30
            )
            
            if Path(tmp_cover).exists() and Path(tmp_cover).stat().st_size > 1000:
                import shutil
                shutil.copy2(tmp_cover, cover_path)
                Path(tmp_cover).unlink()
                logger.info(f"✅ Обложка извлечена через temp файл ({Path(cover_path).stat().st_size} байт)")
                return True
                
        except Exception as e:
            logger.debug(f"Дополнительный метод не сработал: {e}")
        
        return False
        
    except Exception as e:
        logger.error(f"Общая ошибка извлечения обложки: {e}")
        return False


def convert_book_with_cover(input_path: str, output_path: str, cover_path: str = None) -> tuple[bool, str]:
    """Конвертация с правильной вставкой обложки"""
    try:
        input_abs = str(Path(input_path).resolve())
        output_abs = str(Path(output_path).resolve())
        
        # Формируем команду конвертации
        cmd = ["ebook-convert", input_abs, output_abs]
        
        # Критически важные опции для обложки Kindle
        output_ext = Path(output_abs).suffix.lower()
        
        if output_ext == ".mobi":
            # Для MOBI (старые Kindle) - особые настройки
            cmd.extend([
                "--mobi-keep-original-images",
                "--share-not-sync",  # Для лучшей совместимости
                "--personal-doc=Y"   # Для личных документов
            ])
        elif output_ext == ".azw3":
            # Для AZW3 (новые Kindle)
            cmd.extend([
                "--disable-font-rescaling"
            ])
        
        # Добавляем обложку если есть
        if cover_path and Path(cover_path).exists() and Path(cover_path).stat().st_size > 1000:
            cmd.extend([
                "--cover", cover_path,
                "--preserve-cover-aspect-ratio",  # Сохраняем пропорции
            ])
            logger.info(f"Конвертация с обложкой: {cover_path} ({Path(cover_path).stat().st_size} байт)")
        else:
            logger.info("Конвертация без обложки")
            # Даже без обложки добавляем опцию для лучшего результата
            cmd.append("--no-default-epub-cover")
        
        # УБИРАЕМ пустые --title= и --authors= - они вызывают ошибку
        # Вместо этого добавляем только важные опции
        cmd.extend([
            "--linearize-tables"  # Для лучшего отображения
        ])
        
        # Для FB2 добавляем опции для лучшей обработки
        if input_path.lower().endswith('.fb2'):
            cmd.extend([
                "--embed-all-fonts",
                "--subset-embedded-fonts"
            ])
        
        logger.info(f"Выполняю команду: {' '.join(cmd[:5])}...")  # Логируем только начало команды
        
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=300,  # Увеличиваем таймаут для сложных книг
            encoding='utf-8',
            errors='replace'
        )
        
        # Логируем вывод для отладки
        if result.stdout:
            logger.debug(f"Stdout конвертации: {result.stdout[:500]}")
        if result.stderr:
            logger.warning(f"Stderr конвертации: {result.stderr[:500]}")
        
        output_p = Path(output_abs)
        if result.returncode != 0 or not output_p.exists() or output_p.stat().st_size == 0:
            error_msg = f"Код {result.returncode}"
            if result.stderr:
                # Пытаемся извлечь полезную информацию из stderr
                error_lines = []
                for line in result.stderr.split('\n'):
                    line = line.strip()
                    if line and not line.startswith("Usage:") and not line.startswith("Convert"):
                        error_lines.append(line[:200])
                if error_lines:
                    error_msg += f"\n{'. '.join(error_lines[:3])}"
            return False, error_msg
        
        # Проверяем, есть ли обложка в выходном файле
        cover_check = ""
        if cover_path and Path(cover_path).exists():
            try:
                # Проверяем через ebook-meta, есть ли обложка в выходном файле
                check_result = subprocess.run(
                    ["ebook-meta", output_abs],
                    capture_output=True,
                    text=True,
                    timeout=30
                )
                if "Cover: Yes" in check_result.stdout:
                    cover_check = " ✓ обложка встроена"
                elif "Has cover: yes" in check_result.stdout:
                    cover_check = " ✓ обложка встроена"
                else:
                    cover_check = " ⚠️ обложка не встроена"
            except Exception as e:
                logger.debug(f"Не удалось проверить обложку: {e}")
                cover_check = " ? статус обложки неизвестен"
        
        size_info = f"{output_p.stat().st_size / 1024 / 1024:.2f} МБ"
        return True, f"{size_info}{cover_check}"
        
    except subprocess.TimeoutExpired:
        return False, "Таймаут конвертации (5 мин)"
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
            
            # Извлекаем обложку УЛУЧШЕННЫМ методом
            cover_path = f"{task['input_path']}.cover.jpg"
            has_cover = extract_cover_improved(unpacked_path, cover_path)
            
            # Обновляем статус
            try:
                status = f"⏳ Конвертирую:\n<b>{title}</b>\n<i>{author}</i>"
                if has_cover:
                    cover_size = Path(cover_path).stat().st_size if Path(cover_path).exists() else 0
                    status += f"\n✅ Обложка найдена ({cover_size/1024:.1f} КБ)"
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
            success, diag = convert_book_with_cover(
                unpacked_path,
                task["output_path"],
                cover_path if has_cover else None
            )
            
            # Отправляем результат
            output_p = Path(task["output_path"])
            if success and output_p.exists():
                safe_title = re.sub(r'[<>:"/\\|?*]', '', title)[:50]
                safe_author = re.sub(r'[<>:"/\\|?*]', '', author)[:30]
                filename = f"{safe_author} - {safe_title}{output_p.suffix}"
                
                # Формируем информативное сообщение
                cover_info = ""
                if has_cover:
                    cover_info = "\n🖼️ Обложка встроена" if "✓ обложка встроена" in diag else "\n⚠️ Обложка могла не встроиться"
                
                await application.bot.send_document(
                    chat_id=task["user_id"],
                    document=open(output_p, "rb"),
                    filename=filename,
                    caption=f"✅ Конвертация завершена\n📚 {title}\n👤 {author}\n💾 {diag}{cover_info}",
                    parse_mode=ParseMode.HTML,
                )
                
                # Совет по обложкам
                if output_p.suffix.lower() == ".mobi" and has_cover:
                    advice = (
                        "\n\n📝 <b>Совет по обложкам для MOBI:</b>\n"
                        "1. На Kindle отправьте файл на email устройства\n"
                        "2. В теме письма добавьте <code>convert</code>\n"
                        "3. Или используйте Calibre для прямого копирования"
                    )
                    await application.bot.send_message(
                        chat_id=task["user_id"],
                        text=advice,
                        parse_mode=ParseMode.HTML,
                        reply_markup=MAIN_REPLY_KEYBOARD
                    )
                else:
                    await application.bot.send_message(
                        chat_id=task["user_id"],
                        text="Файл готов для Kindle! 📚",
                        reply_markup=MAIN_REPLY_KEYBOARD
                    )
            else:
                error_msg = (
                    f"❌ Ошибка конвертации <b>{title}</b>:\n"
                    f"<code>{diag}</code>\n\n"
                    f"Попробуйте:\n"
                    f"1. Конвертировать в другой формат (AZW3 вместо MOBI)\n"
                    f"2. Уменьшить размер файла\n"
                    f"3. Отправить книгу без обложки"
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
                        logger.debug(f"Удален файл: {p}")
                except Exception as e:
                    logger.debug(f"Не удалось удалить {p}: {e}")
            
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
        "📚 <b>KindleGarden Bot v2</b>\n\n"
        "✅ <b>Улучшена поддержка обложек!</b>\n\n"
        "Поддерживаю:\n"
        "• FB2 / FB2.ZIP (с обложками)\n"
        "• EPUB (с обложками)\n\n"
        "<b>Совет:</b> Для лучшего отображения обложек\n"
        "используйте формат <b>AZW3</b> в настройках.",
        parse_mode=ParseMode.HTML,
        reply_markup=MAIN_REPLY_KEYBOARD
    )


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    message = (
        "📚 <b>KindleGarden - помощь</b>\n\n"
        
        "✅ <b>Как работают обложки:</b>\n"
        "• Бот извлекает обложку из исходного файла\n"
        "• Встраивает её в сконвертированную книгу\n"
        "• <b>Важно:</b> Обложка должна быть в формате JPEG/PNG\n\n"
        
        "🔄 <b>Форматы и обложки:</b>\n"
        "• <b>AZW3</b> - лучшая поддержка обложек, новые Kindle\n"
        "• <b>MOBI</b> - старые Kindle, могут быть проблемы с отображением\n"
        "• <b>EPUB</b> - другие читалки, обычно без проблем\n\n"
        
        "⚠️ <b>Почему обложка может не отображаться:</b>\n"
        "1. Исходный файл не содержит обложку\n"
        "2. Обложка слишком маленькая (< 600x800)\n"
        "3. Старый Kindle (1-5 поколение)\n"
        "4. Файл отправлен не через email\n\n"
        
        "📧 <b>Для гарантированного отображения обложки:</b>\n"
        "1. Используйте формат <b>AZW3</b>\n"
        "2. Отправляйте на email Kindle с темой <code>convert</code>\n"
        "3. Или используйте Calibre для отправки\n\n"
        
        "⚙️ <b>Ограничения:</b>\n"
        "• Максимальный размер файла: 50 МБ\n"
        "• Время конвертации: до 5 минут\n"
        "• Очередь: 5 файлов одновременно"
    )
    await update.message.reply_text(
        message,
        parse_mode=ParseMode.HTML,
        reply_markup=MAIN_REPLY_KEYBOARD
    )


async def settings_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id
    current = settings_db.get_preferred_format(user_id)
    
    # Описание форматов
    formats = {
        "azw3": "📘 AZW3 - лучшие обложки (новые Kindle)",
        "mobi": "📙 MOBI - совместимость (старые Kindle)",
        "epub": "📖 EPUB - другие читалки"
    }
    
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton(
            f"{'✅ ' if fmt == current else ''}{desc}", 
            callback_data=f"setfmt:{fmt}"
        )] for fmt, desc in formats.items()
    ])
    
    await update.message.reply_text(
        f"⚙️ <b>Текущий формат:</b> {current.upper()}\n\n"
        f"<b>Рекомендации:</b>\n"
        f"• AZW3 - лучшая поддержка обложек\n"
        f"• MOBI - для старых Kindle\n"
        f"• EPUB - для других устройств",
        parse_mode=ParseMode.HTML,
        reply_markup=kb
    )


async def handle_format_setting(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    _, fmt = query.data.split(":")
    
    settings_db.set_preferred_format(update.effective_user.id, fmt)
    
    # Совет в зависимости от формата
    advice = ""
    if fmt == "mobi":
        advice = "\n\n⚠️ Для отображения обложек MOBI отправляйте файлы на email Kindle с темой 'convert'"
    elif fmt == "azw3":
        advice = "\n\n✅ AZW3 лучше всего поддерживает обложки на новых Kindle"
    
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

    # Проверяем размер (увеличил до 50 МБ для толстых книг)
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
            f"❌ Ошибка загрузки файла:\n<code>{str(e)[:100]}</code>",
            parse_mode=ParseMode.HTML,
            reply_markup=MAIN_REPLY_KEYBOARD
        )
        try:
            Path(task["input_path"]).unlink(missing_ok=True)
        except:
            pass
        return

    await conversion_queue.put(task)
    
    # Сообщение о добавлении в очередь с информацией о формате
    format_info = {
        "azw3": "AZW3 (рекомендуется для обложек)",
        "mobi": "MOBI (для старых Kindle)",
        "epub": "EPUB (для других устройств)"
    }
    
    msg = await update.message.reply_text(
        f"✅ Добавлено в очередь ({conversion_queue.qsize()}/5)\n"
        f"Формат: <b>{task['output_format'].upper()}</b>\n"
        f"{format_info.get(task['output_format'], '')}\n\n"
        f"⏳ Ожидайте начала конвертации...",
        parse_mode=ParseMode.HTML
    )
    task["message_id"] = msg.message_id


async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    t = update.message.text.strip()
    if t == "📚 Отправить книгу":
        await update.message.reply_text(
            "📎 Отправьте FB2 или EPUB файл\n"
            "Максимальный размер: 50 МБ\n\n"
            "✅ Поддерживаются обложки",
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
    missing_tools = []
    
    for tool in required_tools:
        try:
            result = subprocess.run([tool, "--version"], capture_output=True, timeout=5)
            if result.returncode == 0:
                logger.info(f"✅ {tool} доступен")
            else:
                missing_tools.append(tool)
                logger.error(f"❌ {tool} не работает правильно")
        except Exception as e:
            missing_tools.append(tool)
            logger.error(f"❌ {tool} не найден: {e}")
    
    if missing_tools:
        error_msg = "Не установлены или не работают:\n" + "\n".join(missing_tools)
        logger.critical(error_msg)
        raise RuntimeError(
            f"{error_msg}\n"
            f"Выполните на Raspberry Pi:\n"
            f"sudo apt update && sudo apt install -y calibre"
        )
    
    # Проверяем версию Calibre
    try:
        result = subprocess.run(["ebook-convert", "--version"], capture_output=True, text=True, timeout=5)
        logger.info(f"Версия Calibre: {result.stdout.strip()}")
    except:
        logger.warning("Не удалось проверить версию Calibre")
    
    asyncio.create_task(conversion_worker(app))
    logger.info("✅ Бот готов к работе")


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
    
    logger.info("🚀 Бот запущен с улучшенной поддержкой обложек")
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Бот остановлен")
    finally:
        settings_db.close()