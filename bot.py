import asyncio
import logging
import os
import subprocess
import re
import shutil
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
        if not Path(input_path).exists():
            logger.error(f"Файл не найден для извлечения метаданных: {input_path}")
            return {"title": "Неизвестно", "authors": ["Неизвестен"]}
        
        logger.info(f"Извлечение метаданных из: {input_path}")
        result = subprocess.run(
            ["ebook-meta", input_path],
            capture_output=True,
            text=True,
            timeout=30,
            encoding='utf-8',
            errors='replace'
        )
        
        if result.returncode != 0:
            logger.warning(f"ebook-meta ошибка (код {result.returncode}):\n{result.stderr}")
            # Пытаемся извлечь хоть что-то из вывода
            title = "Неизвестно"
            authors = ["Неизвестен"]
            for line in result.stdout.splitlines():
                if line.startswith("Title:") and len(line) > 6:
                    title = line[6:].strip() or "Неизвестно"
                elif line.startswith("Author(s):") and len(line) > 10:
                    authors_raw = line[10:].strip()
                    authors = [a.strip() for a in authors_raw.split(",")] if authors_raw else ["Неизвестен"]
            return {"title": title, "authors": authors}
        
        # Парсим вывод
        metadata = {"title": "Неизвестно", "authors": ["Неизвестен"]}
        for line in result.stdout.splitlines():
            line = line.strip()
            if line.startswith("Title:") and len(line) > 6:
                metadata["title"] = line[6:].strip() or "Неизвестно"
            elif line.startswith("Author(s):") and len(line) > 10:
                authors_raw = line[10:].strip()
                metadata["authors"] = [a.strip() for a in authors_raw.split(",")] if authors_raw else ["Неизвестен"]
        
        logger.info(f"Метаданные извлечены: {metadata}")
        return metadata
        
    except subprocess.TimeoutExpired:
        logger.error("Таймаут при извлечении метаданных")
        return {"title": "Неизвестно", "authors": ["Неизвестен"]}
    except Exception as e:
        logger.error(f"Ошибка извлечения метаданных: {e}", exc_info=True)
        return {"title": "Неизвестно", "authors": ["Неизвестен"]}


def convert_book(input_path: str, output_path: str, output_format: str) -> tuple[bool, str]:
    """Конвертирует книгу. Возвращает (успех, диагностическое сообщение)"""
    try:
        # Проверяем существование входного файла
        input_p = Path(input_path)
        if not input_p.exists():
            return False, f"Входной файл не найден: {input_path}"
        
        if input_p.stat().st_size == 0:
            return False, f"Входной файл пустой: {input_path} (0 байт)"
        
        logger.info(f"Начало конвертации: {input_path} → {output_path} ({output_format})")
        logger.info(f"Размер входного файла: {input_p.stat().st_size / 1024:.1f} КБ")
        
        # Проверяем свободное место
        free_space = shutil.disk_usage("/").free
        if free_space < 50 * 1024 * 1024:  # 50 МБ
            return False, f"Мало свободного места на диске: {free_space / 1024 / 1024:.1f} МБ"
        
        # Ключевое исправление: НЕТ опции --cover (она ломает конвертацию для FB2)
        cmd = [
            "ebook-convert",
            str(input_p),
            output_path,
            "--output-profile", "kindle_pw3",
            "--preserve-cover-aspect-ratio",
            "--margin-left", "0",
            "--margin-right", "0",
            "--margin-top", "0",
            "--margin-bottom", "0",
            "--extra-css", "body { font-family: serif; line-height: 1.4; }",
            "--verbose",  # Детальный вывод для диагностики
        ]
        
        if output_format == "mobi":
            cmd.extend(["--mobi-keep-original-images", "--mobi-toc-at-start"])
        
        logger.debug(f"Команда конвертации: {' '.join(cmd)}")
        
        # Запускаем с таймаутом и перехватом вывода
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=180,
            encoding='utf-8',
            errors='replace'
        )
        
        output_p = Path(output_path)
        
        if result.returncode != 0:
            error_msg = (
                f"Код ошибки: {result.returncode}\n"
                f"STDOUT (первые 500 символов):\n{result.stdout[:500]}\n"
                f"STDERR (первые 500 символов):\n{result.stderr[:500]}"
            )
            logger.error(f"Ошибка конвертации:\n{error_msg}")
            return False, error_msg
        
        if not output_p.exists() or output_p.stat().st_size == 0:
            error_msg = f"Выходной файл не создан или пустой. Размер: {output_p.stat().st_size if output_p.exists() else 'N/A'} байт"
            logger.error(error_msg)
            return False, error_msg
        
        logger.info(f"Конвертация успешна: {output_path} ({output_p.stat().st_size / 1024:.1f} КБ)")
        return True, "OK"
        
    except subprocess.TimeoutExpired as e:
        logger.error(f"Таймаут конвертации (180 сек): {e}")
        return False, "Таймаут конвертации (более 180 секунд)"
    except MemoryError:
        logger.error("Нехватка памяти при конвертации")
        return False, "Нехватка оперативной памяти (малинка 3 имеет только 1 ГБ RAM)"
    except Exception as e:
        logger.error(f"Исключение при конвертации: {e}", exc_info=True)
        return False, f"Исключение: {type(e).__name__}: {str(e)[:200]}"


async def conversion_worker(application: Application):
    """Воркер — обрабатывает очередь без блокировок"""
    logger.info("🔄 Запущен воркер конвертации")
    
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
            await _update_status_message(
                application, task_id,
                f"⏳ Конвертирую:\n<b>{title}</b>\n<i>{author}</i>"
            )
            
            # Конвертируем с диагностикой
            success, diag_msg = convert_book(
                task["input_path"],
                task["output_path"],
                task["output_format"]
            )
            
            # Отправляем результат
            if success and Path(task["output_path"]).exists():
                safe_title = re.sub(r'[<>:"/\\|?*]', '', title)[:50]
                safe_author = re.sub(r'[<>:"/\\|?*]', '', author)[:30]
                output_filename = f"{safe_author} - {safe_title}.{task['output_format']}"
                
                await _send_result(application, task, success=True, filename=output_filename, title=title, author=author)
            else:
                await _send_result(application, task, success=False, title=title, author=author, diag_msg=diag_msg)
            
            # Чистим временные файлы
            _cleanup_temp_files(task["input_path"], task["output_path"])
            
            conversion_queue.task_done()
            active_tasks.pop(task_id, None)
            
        except Exception as e:
            logger.error(f"Ошибка в воркере: {e}", exc_info=True)
            await asyncio.sleep(5)


async def _update_status_message(application: Application, task_id: str, status_text: str):
    """Обновляет сообщение со статусом"""
    task = active_tasks.get(task_id)
    if not task or not task.get("message_id"):
        return
    
    try:
        position = conversion_queue.qsize()
        queue_info = f"\nОчередь: {position} файл(ов)" if position > 0 else ""
        
        await application.bot.edit_message_text(
            chat_id=task["user_id"],
            message_id=task["message_id"],
            text=f"{status_text}{queue_info}",
            parse_mode=ParseMode.HTML
        )
    except Exception as e:
        logger.warning(f"Не удалось обновить статус: {e}")


async def _send_result(application: Application, task: dict, success: bool, filename: str = None, title: str = None, author: str = None, diag_msg: str = None):
    """Отправляет результат конвертации"""
    try:
        if success:
            output_path = Path(task["output_path"])
            await application.bot.send_document(
                chat_id=task["user_id"],
                document=open(output_path, "rb"),
                filename=filename or f"{Path(task['file_name']).stem}.{task['output_format']}",
                caption=(
                    f"✅ Готово! Сконвертировано в <b>{task['output_format'].upper()}</b>\n\n"
                    f"📚 {title or 'Неизвестно'}\n"
                    f"👤 {author or 'Неизвестен'}\n"
                    f"📦 {output_path.stat().st_size / 1024:.1f} КБ"
                ),
                parse_mode=ParseMode.HTML,
            )
            await application.bot.send_message(
                chat_id=task["user_id"],
                text="Файл готов к отправке на Kindle! 📚\n\nОтправь ещё один FB2/EPUB для конвертации.",
                reply_markup=MAIN_REPLY_KEYBOARD
            )
        else:
            error_text = (
                f"❌ Ошибка конвертации книги:\n"
                f"<b>{title or task['file_name']}</b>\n\n"
                f"Диагностика:\n<code>{diag_msg[:300] if diag_msg else 'Неизвестная ошибка'}</code>\n\n"
                f"💡 Советы:\n"
                f"• Попробуй другой формат (EPUB вместо AZW3)\n"
                f"• Убедись, что файл не повреждён\n"
                f"• На малинке 3 конвертация больших книг (>5 МБ) может не уложиться в 1 ГБ RAM"
            )
            await application.bot.send_message(
                chat_id=task["user_id"],
                text=error_text,
                parse_mode=ParseMode.HTML,
                reply_markup=MAIN_REPLY_KEYBOARD
            )
    except Exception as e:
        logger.error(f"Ошибка отправки результата: {e}")


def _cleanup_temp_files(*paths):
    """Удаляет временные файлы"""
    for path in paths:
        try:
            p = Path(path)
            if p.exists():
                size_kb = p.stat().st_size / 1024
                p.unlink()
                logger.debug(f"Удалён временный файл: {path} ({size_kb:.1f} КБ)")
        except Exception as e:
            logger.warning(f"Не удалось удалить {path}: {e}")


def _get_inline_settings_keyboard(user_id: int) -> InlineKeyboardMarkup:
    """Кнопки выбора формата в настройках (инлайн)"""
    current = settings_db.get_preferred_format(user_id)
    formats = [
        ("📘 AZW3 (рекомендуется)", "azw3"),
        ("📖 EPUB", "epub"),
        ("📙 MOBI (устаревший)", "mobi"),
    ]
    
    buttons = []
    for label, fmt in formats:
        prefix = "✅ " if fmt == current else ""
        buttons.append([InlineKeyboardButton(f"{prefix}{label}", callback_data=f"setfmt:{fmt}")])
    
    return InlineKeyboardMarkup(buttons)


def _get_help_text() -> str:
    """Текст помощи"""
    return (
        "📚 <b>Как пользоваться ботом:</b>\n\n"
        "1. Нажми «📚 Отправить книгу» или просто прикрепи FB2/EPUB файл\n"
        "2. Бот автоматически конвертирует его в выбранный формат\n"
        "3. Получи готовый файл для Kindle с обложкой и метаданными!\n\n"
        "✨ <b>Особенности:</b>\n"
        "• Обложки и метаданные (автор/название) сохраняются автоматически\n"
        "• Файлы обрабатываются по очереди (макс. 5 одновременно)\n"
        "• Выходное имя файла: «Автор - Название.формат»\n\n"
        "⚙️ Изменить формат по умолчанию: кнопка «⚙️ Настройки»\n\n"
        "⚠️ <b>Важно для малинки 3:</b>\n"
        "• Конвертация занимает 15–60 секунд\n"
        "• Книги >5 МБ могут не конвертироваться из-за нехватки RAM (1 ГБ)\n"
        "• При ошибке попробуй формат EPUB — он легче для системы"
    )


# ========== ОБРАБОТЧИКИ ==========

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Главное меню с постоянной клавиатурой"""
    message = (
        "📚 <b>KindleGarden Bot</b>\n\n"
        "Конвертирую FB2/EPUB → Kindle-форматы с сохранением обложек и метаданных!\n\n"
        "✅ Поддерживаемые форматы:\n"
        "• <b>AZW3</b> — рекомендуется для современных Kindle\n"
        "• <b>EPUB</b> — универсальный формат (Kindle 2022+)\n"
        "• <b>MOBI</b> — для очень старых устройств\n\n"
        "Просто отправь файл или нажми «📚 Отправить книгу» 👇"
    )
    await update.message.reply_text(
        message,
        parse_mode=ParseMode.HTML,
        reply_markup=MAIN_REPLY_KEYBOARD
    )


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Команда /help"""
    await update.message.reply_text(
        _get_help_text(),
        parse_mode=ParseMode.HTML,
        reply_markup=MAIN_REPLY_KEYBOARD
    )


async def settings_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Команда /settings — выбор формата по умолчанию"""
    user_id = update.effective_user.id
    current_format = settings_db.get_preferred_format(user_id)
    format_names = {"azw3": "AZW3", "epub": "EPUB", "mobi": "MOBI"}
    
    message = (
        "⚙️ <b>Настройки</b>\n\n"
        f"Текущий формат по умолчанию: <b>{format_names.get(current_format, current_format)}</b>\n\n"
        "Выбери новый формат:"
    )
    
    await update.message.reply_text(
        message,
        parse_mode=ParseMode.HTML,
        reply_markup=_get_inline_settings_keyboard(user_id)
    )


async def handle_format_setting(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Сохранение формата по умолчанию"""
    query = update.callback_query
    await query.answer()
    
    _, fmt = query.data.split(":")
    user_id = update.effective_user.id
    settings_db.set_preferred_format(user_id, fmt)
    
    format_names = {"azw3": "AZW3", "epub": "EPUB", "mobi": "MOBI"}
    await query.edit_message_text(
        f"✅ Формат по умолчанию установлен: <b>{format_names.get(fmt, fmt)}</b>\n\n"
        "Теперь все файлы будут конвертироваться в этот формат автоматически.",
        parse_mode=ParseMode.HTML
    )
    # Возвращаем постоянное меню
    await query.message.reply_text("Выбери действие:", reply_markup=MAIN_REPLY_KEYBOARD)


async def handle_document(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Обработчик документов — сразу ставим в очередь с форматом по умолчанию"""
    document = update.message.document
    
    # Проверяем формат
    filename = document.file_name.lower() if document.file_name else ""
    if not (filename.endswith(".fb2") or filename.endswith(".fb2.zip") or filename.endswith(".epub")):
        await update.message.reply_text(
            "⚠️ Принимаю только FB2 и EPUB файлы (.fb2, .fb2.zip, .epub)",
            reply_markup=MAIN_REPLY_KEYBOARD
        )
        return

    # Ограничиваем размер (строже для малинки 3)
    if document.file_size > 10 * 1024 * 1024:  # 10 МБ вместо 20
        await update.message.reply_text(
            "⚠️ Файл слишком большой (максимум 10 МБ для малинки 3).\n"
            "Kindle и так не любит тяжёлые книги 😉",
            reply_markup=MAIN_REPLY_KEYBOARD
        )
        return

    # Проверяем очередь
    if conversion_queue.full():
        await update.message.reply_text(
            f"⏸️ Очередь заполнена ({conversion_queue.qsize()}/{conversion_queue.maxsize} файлов).\n"
            "Попробуй через минуту.",
            reply_markup=MAIN_REPLY_KEYBOARD
        )
        return

    # Получаем формат по умолчанию
    user_id = update.effective_user.id
    output_format = settings_db.get_preferred_format(user_id)
    
    # Генерируем пути
    task_id = str(uuid4())
    input_ext = Path(filename).suffix or ".fb2"
    output_ext = {"azw3": ".azw3", "epub": ".epub", "mobi": ".mobi"}[output_format]
    
    task_info = {
        "task_id": task_id,
        "user_id": user_id,
        "file_id": document.file_id,
        "file_name": document.file_name,
        "input_path": str(Path("tmp") / f"{task_id}{input_ext}"),
        "output_path": str(Path("tmp") / f"{task_id}{output_ext}"),
        "output_format": output_format,
        "status": "queued",
        "queued_at": datetime.now(),
    }
    active_tasks[task_id] = task_info

    # Скачиваем файл
    try:
        file = await context.bot.get_file(document.file_id)
        await file.download_to_drive(task_info["input_path"])
        input_size = Path(task_info["input_path"]).stat().st_size
        logger.info(f"Файл скачан: {task_info['input_path']} ({input_size / 1024:.1f} КБ)")
        
        if input_size == 0:
            raise ValueError("Скачанный файл пустой")
            
    except Exception as e:
        logger.error(f"Ошибка скачивания: {e}")
        await update.message.reply_text(
            "❌ Ошибка при скачивании файла. Попробуй заново.",
            reply_markup=MAIN_REPLY_KEYBOARD
        )
        return

    # Ставим в очередь
    await conversion_queue.put(task_info)
    position = conversion_queue.qsize()
    
    # Показываем статус
    msg = await update.message.reply_text(
        f"✅ Файл добавлен в очередь\n"
        f"Формат: <b>{output_format.upper()}</b>\n"
        f"Позиция: {position} из {conversion_queue.maxsize}",
        parse_mode=ParseMode.HTML
    )
    task_info["message_id"] = msg.message_id


async def handle_text_commands(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Обработчик текстовых кнопок меню"""
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
            "Не понимаю эту команду. Используй меню внизу 👇",
            reply_markup=MAIN_REPLY_KEYBOARD
        )


async def post_init(application: Application) -> None:
    """Запуск воркера после старта бота"""
    # Проверяем зависимости при старте
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
    """Запуск бота"""
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    if not token:
        logger.error("❌ Токен не найден! Создай .env с TELEGRAM_BOT_TOKEN")
        return

    application = Application.builder().token(token).post_init(post_init).build()

    # Регистрируем обработчики
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("settings", settings_menu))
    application.add_handler(CommandHandler("help", help_command))
    application.add_handler(MessageHandler(filters.Document.ALL, handle_document))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_commands))
    application.add_handler(CallbackQueryHandler(handle_format_setting, pattern="^setfmt:"))

    logger.info("✅ Бот запущен с расширенной диагностикой!")
    application.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    try:
        main()
    finally:
        settings_db.close()