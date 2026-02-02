import asyncio
import logging
import os
import subprocess
import re
import base64
import zipfile
import tempfile
import shutil
from pathlib import Path
from uuid import uuid4
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
        
        metadata = {"title": "Без названия", "authors": ["Неизвестен"]}
        
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
        
        return metadata
        
    except Exception as e:
        logger.warning(f"Ошибка извлечения метаданных: {e}")
        return {"title": "Без названия", "authors": ["Неизвестен"]}


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
                for encoding in ['utf-8', 'cp1251', 'koi8-r']:
                    try:
                        text = content.decode(encoding)
                        break
                    except:
                        continue
                else:
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
                    except:
                        continue
        
        # Метод 3: попробуем конвертировать в PDF с обложкой и извлечь
        try:
            temp_output = Path(input_path).with_suffix('.temp.pdf')
            cmd = ["ebook-convert", input_path, str(temp_output)]
            
            result = subprocess.run(
                cmd,
                capture_output=True,
                timeout=60
            )
            
            # Ищем обложку в папке с файлом
            possible_covers = [
                cover_path,
                Path(input_path).parent / "cover.jpg",
                Path(input_path).parent / "cover.png",
            ]
            
            for cover in possible_covers:
                if cover.exists() and cover.stat().st_size > 1000:
                    shutil.copy2(cover, cover_path)
                    if temp_output.exists():
                        temp_output.unlink()
                    return True
            
            if temp_output.exists():
                temp_output.unlink()
                
        except Exception as e:
            logger.debug(f"Метод через PDF не сработал: {e}")
        
        return False
        
    except Exception as e:
        logger.error(f"Ошибка извлечения обложки: {e}")
        return False


def convert_book_simple(input_path: str, output_path: str, cover_path: str = None) -> tuple[bool, str]:
    """Простая конвертация книги"""
    try:
        cmd = ["ebook-convert", input_path, output_path]
        
        # Добавляем обложку если есть
        if cover_path and Path(cover_path).exists() and Path(cover_path).stat().st_size > 1000:
            cmd.extend(["--cover", cover_path])
            logger.info(f"Конвертация с обложкой")
        else:
            cmd.append("--no-default-epub-cover")
            logger.info("Конвертация без обложки")
        
        # Для MOBI добавляем специфичные опции
        if output_path.lower().endswith('.mobi'):
            cmd.extend(["--mobi-keep-original-images"])
        
        logger.debug(f"Команда: {' '.join(cmd)}")
        
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=300,
            encoding='utf-8',
            errors='replace'
        )
        
        # Логируем ошибки если есть
        if result.stderr:
            error_lines = [line.strip() for line in result.stderr.split('\n') if line.strip()]
            if error_lines and not error_lines[0].startswith("Usage:"):
                logger.warning(f"Stderr конвертации: {result.stderr[:500]}")
        
        output_p = Path(output_path)
        if result.returncode != 0 or not output_p.exists() or output_p.stat().st_size == 0:
            error_msg = f"Код ошибки: {result.returncode}"
            if result.stderr:
                # Берем первую значимую строку ошибки
                for line in result.stderr.split('\n'):
                    if line.strip() and not line.startswith("Usage:"):
                        error_msg = line.strip()[:200]
                        break
            return False, error_msg
        
        # Проверяем наличие обложки в выходном файле
        cover_check = ""
        try:
            check_result = subprocess.run(
                ["ebook-meta", output_path],
                capture_output=True,
                text=True,
                timeout=30
            )
            if "Cover:" in check_result.stdout or "Has cover:" in check_result.stdout:
                for line in check_result.stdout.split('\n'):
                    if "Cover:" in line or "Has cover:" in line:
                        if "yes" in line.lower() or "true" in line.lower():
                            cover_check = " ✓ с обложкой"
                        else:
                            cover_check = " ✗ без обложки"
                        break
        except:
            cover_check = ""
        
        size_info = f"{output_p.stat().st_size / 1024 / 1024:.2f} МБ"
        return True, f"{size_info}{cover_check}"
        
    except subprocess.TimeoutExpired:
        return False, "Таймаут конвертации"
    except Exception as e:
        logger.error(f"Ошибка конвертации: {e}")
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
            
            # Конвертируем
            success, diag = convert_book_simple(
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
                
                caption = f"✅ Конвертация завершена\n📚 {title}\n👤 {author}\n💾 {diag}"
                
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
                error_msg = (
                    f"❌ Ошибка конвертации <b>{title}</b>:\n"
                    f"<code>{diag}</code>\n\n"
                    f"Попробуйте:\n"
                    f"1. Конвертировать в другой формат (AZW3 вместо MOBI)\n"
                    f"2. Отправить книгу заново\n"
                    f"3. Проверить формат исходного файла"
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
        "📚 <b>KindleGarden Bot</b>\n\n"
        "Отправьте мне книгу в формате FB2 или EPUB, и я конвертирую её для Kindle!\n\n"
        "Поддерживаемые форматы:\n"
        "• FB2 (.fb2)\n"
        "• FB2.ZIP (.fb2.zip)\n"
        "• EPUB (.epub)\n\n"
        "Максимальный размер: 50 МБ",
        parse_mode=ParseMode.HTML,
        reply_markup=MAIN_REPLY_KEYBOARD
    )


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    message = (
        "📚 <b>Помощь по использованию бота</b>\n\n"
        
        "✅ <b>Как использовать:</b>\n"
        "1. Нажмите '📚 Отправить книгу'\n"
        "2. Выберите файл FB2 или EPUB\n"
        "3. Дождитесь конвертации\n"
        "4. Получите готовый файл\n\n"
        
        "⚙️ <b>Настройки формата:</b>\n"
        "• AZW3 - для новых Kindle\n"
        "• MOBI - для старых Kindle\n"
        "• EPUB - для других устройств\n\n"
        
        "🖼️ <b>Об обложках:</b>\n"
        "Бот пытается извлечь обложку из книги.\n"
        "Если обложка не извлекается, книга будет без неё.\n"
        "Это нормально для некоторых файлов.\n\n"
        
        "⏱️ <b>Время конвертации:</b>\n"
        "Обычно 1-3 минуты\n"
        "Очередь: 5 файлов одновременно"
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
        [InlineKeyboardButton(f"{'✅ ' if 'azw3' == current else ''}📘 AZW3", callback_data="setfmt:azw3")],
        [InlineKeyboardButton(f"{'✅ ' if 'epub' == current else ''}📖 EPUB", callback_data="setfmt:epub")],
        [InlineKeyboardButton(f"{'✅ ' if 'mobi' == current else ''}📙 MOBI", callback_data="setfmt:mobi")]
    ])
    
    await update.message.reply_text(
        f"⚙️ <b>Текущий формат:</b> {current.upper()}\n\n"
        f"Выберите формат для конвертации:",
        parse_mode=ParseMode.HTML,
        reply_markup=kb
    )


async def handle_format_setting(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    _, fmt = query.data.split(":")
    
    settings_db.set_preferred_format(update.effective_user.id, fmt)
    
    await query.edit_message_text(
        f"✅ Формат изменен на <b>{fmt.upper()}</b>",
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
    
    msg = await update.message.reply_text(
        f"✅ Добавлено в очередь ({conversion_queue.qsize()}/5)\n"
        f"Формат: <b>{task['output_format'].upper()}</b>\n\n"
        f"⏳ Ожидайте начала конвертации...",
        parse_mode=ParseMode.HTML
    )
    task["message_id"] = msg.message_id


async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    t = update.message.text.strip()
    if t == "📚 Отправить книгу":
        await update.message.reply_text(
            "📎 Отправьте FB2 или EPUB файл\n"
            "Максимальный размер: 50 МБ",
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
    
    logger.info("🚀 Бот запущен")
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Бот остановлен")
    finally:
        settings_db.close()