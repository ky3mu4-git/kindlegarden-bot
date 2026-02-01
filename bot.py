import asyncio
import logging
import os
import subprocess
import re
from pathlib import Path
from uuid import uuid4
from datetime import datetime
from dotenv import load_dotenv
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
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


# ========== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ==========

def extract_metadata(input_path: str) -> dict:
    """Извлекает автора и название книги через ebook-meta"""
    try:
        result = subprocess.run(
            ["ebook-meta", input_path],
            capture_output=True,
            text=True,
            timeout=30
        )
        
        if result.returncode != 0:
            logger.warning(f"ebook-meta вернул ошибку: {result.stderr}")
            return {"title": "Неизвестно", "authors": ["Неизвестен"]}
        
        # Парсим вывод (пример: "Title: Название книги")
        metadata = {"title": "Неизвестно", "authors": ["Неизвестен"]}
        lines = result.stdout.splitlines()
        
        for line in lines:
            if line.startswith("Title:"):
                metadata["title"] = line[6:].strip() or "Неизвестно"
            elif line.startswith("Author(s):"):
                authors = line[10:].strip()
                metadata["authors"] = [a.strip() for a in authors.split(",")] if authors else ["Неизвестен"]
        
        # Логируем для отладки
        logger.info(f"Метаданные извлечены: {metadata}")
        return metadata
        
    except Exception as e:
        logger.warning(f"Ошибка извлечения метаданных: {e}")
        return {"title": "Неизвестно", "authors": ["Неизвестен"]}


def convert_book(input_path: str, output_path: str, output_format: str) -> bool:
    """Конвертирует книгу с сохранением обложки и метаданных"""
    try:
        # Опции для максимального сохранения обложки и метаданных
        cmd = [
            "ebook-convert",
            input_path,
            output_path,
            "--output-profile", "kindle_pw3",
            "--preserve-cover-aspect-ratio",  # Сохраняем пропорции обложки
            "--cover", input_path,             # Указываем исходник как источник обложки
            "--margin-left", "0",
            "--margin-right", "0",
            "--margin-top", "0",
            "--margin-bottom", "0",
            "--extra-css", "body { font-family: serif; line-height: 1.4; }",
            "--embed-font-family", "Liberation Serif",  # Встраиваем шрифт для лучшей типографики
        ]
        
        # Для MOBI добавляем совместимость
        if output_format == "mobi":
            cmd.extend([
                "--mobi-keep-original-images",
                "--mobi-toc-at-start"
            ])
        
        logger.info(f"Запуск конвертации: {' '.join(cmd)}")
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=180)
        
        if result.returncode != 0:
            logger.error(f"Ошибка конвертации (код {result.returncode}):\n{result.stderr}")
            return False
        
        logger.info(f"Конвертация успешна: {output_path}")
        return True
        
    except subprocess.TimeoutExpired:
        logger.error("Таймаут конвертации (более 180 сек)")
        return False
    except Exception as e:
        logger.error(f"Исключение при конвертации: {e}", exc_info=True)
        return False


async def conversion_worker(application: Application):
    """Воркер — обрабатывает очередь без блокировок"""
    logger.info("🔄 Запущен воркер конвертации")
    
    while True:
        try:
            task = await conversion_queue.get()
            task_id = task["task_id"]
            active_tasks[task_id]["status"] = "converting"
            
            # Извлекаем метаданные ДО конвертации
            metadata = extract_metadata(task["input_path"])
            title = metadata["title"]
            author = metadata["authors"][0] if metadata["authors"] else "Неизвестен"
            
            # Обновляем статус с метаданными
            await _update_status_message(
                application, task_id,
                f"⏳ Конвертирую:\n<b>{title}</b>\n<i>{author}</i>"
            )
            
            # Конвертируем
            success = convert_book(
                task["input_path"],
                task["output_path"],
                task["output_format"]
            )
            
            # Отправляем результат
            if success and Path(task["output_path"]).exists():
                # Формируем красивое имя файла
                safe_title = re.sub(r'[<>:"/\\|?*]', '', title)[:50]
                safe_author = re.sub(r'[<>:"/\\|?*]', '', author)[:30]
                output_filename = f"{safe_author} - {safe_title}.{task['output_format']}"
                
                await _send_result(application, task, success=True, filename=output_filename)
            else:
                await _send_result(application, task, success=False)
            
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


async def _send_result(application: Application, task: dict, success: bool, filename: str = None):
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
                    f"📦 {output_path.stat().st_size / 1024:.1f} КБ"
                ),
                parse_mode=ParseMode.HTML,
            )
            await application.bot.send_message(
                chat_id=task["user_id"],
                text="Файл готов к отправке на Kindle! 📚\n\nОтправь ещё один FB2/EPUB для конвертации."
            )
        else:
            await application.bot.send_message(
                chat_id=task["user_id"],
                text=(
                    "❌ Ошибка конвертации файла <b>{}</b>.\n\n"
                    "Возможно, повреждённый файл или нестандартное форматирование."
                ).format(task["file_name"]),
                parse_mode=ParseMode.HTML,
            )
    except Exception as e:
        logger.error(f"Ошибка отправки результата: {e}")


def _cleanup_temp_files(*paths):
    """Удаляет временные файлы"""
    for path in paths:
        try:
            p = Path(path)
            if p.exists():
                p.unlink()
        except Exception as e:
            logger.warning(f"Не удалось удалить {path}: {e}")


def _get_main_menu_keyboard() -> InlineKeyboardMarkup:
    """Главное меню"""
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("⚙️ Настройки", callback_data="menu:settings")],
        [InlineKeyboardButton("❓ Помощь", callback_data="menu:help")],
    ])


def _get_format_selection_keyboard(user_id: int) -> InlineKeyboardMarkup:
    """Кнопки выбора формата в настройках"""
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
    
    buttons.append([InlineKeyboardButton("⬅️ Назад в меню", callback_data="menu:main")])
    return InlineKeyboardMarkup(buttons)


def _get_help_text() -> str:
    """Текст помощи"""
    return (
        "📚 <b>Как пользоваться ботом:</b>\n\n"
        "1. Отправь файл в формате FB2 или EPUB (макс. 20 МБ)\n"
        "2. Бот автоматически конвертирует его в выбранный формат\n"
        "3. Получи готовый файл для Kindle\n\n"
        "✨ <b>Особенности:</b>\n"
        "• Обложки и метаданные (автор/название) сохраняются\n"
        "• Файлы обрабатываются по очереди (макс. 5 одновременно)\n"
        "• Выходное имя файла: «Автор - Название.формат»\n\n"
        "⚙️ Настроить формат по умолчанию: /settings"
    )


# ========== ОБРАБОТЧИКИ ==========

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Главное меню"""
    message = (
        "📚 <b>KindleGarden Bot</b>\n\n"
        "Конвертирую FB2/EPUB → Kindle-форматы с сохранением обложек и метаданных!\n\n"
        "✅ Поддерживаемые форматы:\n"
        "• <b>AZW3</b> — рекомендуется для современных Kindle\n"
        "• <b>EPUB</b> — универсальный формат (Kindle 2022+)\n"
        "• <b>MOBI</b> — для очень старых устройств\n\n"
        "Просто отправь файл — и получи готовую книгу! 🚀"
    )
    await update.message.reply_text(
        message,
        parse_mode=ParseMode.HTML,
        reply_markup=_get_main_menu_keyboard()
    )


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Команда /help"""
    await update.message.reply_text(
        _get_help_text(),
        parse_mode=ParseMode.HTML,
        reply_markup=_get_main_menu_keyboard()
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
        reply_markup=_get_format_selection_keyboard(user_id)
    )


async def handle_menu_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Обработчик навигации по меню"""
    query = update.callback_query
    await query.answer()
    
    data = query.data
    user_id = update.effective_user.id
    
    if data == "menu:main":
        await query.edit_message_text(
            "📚 <b>KindleGarden Bot</b>\n\nВыбери действие:",
            parse_mode=ParseMode.HTML,
            reply_markup=_get_main_menu_keyboard()
        )
    elif data == "menu:settings":
        current_format = settings_db.get_preferred_format(user_id)
        format_names = {"azw3": "AZW3", "epub": "EPUB", "mobi": "MOBI"}
        await query.edit_message_text(
            f"⚙️ <b>Настройки</b>\n\nТекущий формат: <b>{format_names.get(current_format, current_format)}</b>\n\nВыбери новый формат:",
            parse_mode=ParseMode.HTML,
            reply_markup=_get_format_selection_keyboard(user_id)
        )
    elif data == "menu:help":
        await query.edit_message_text(
            _get_help_text(),
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("⬅️ Назад в меню", callback_data="menu:main")]
            ])
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
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("⬅️ Назад в меню", callback_data="menu:main")]
        ])
    )


async def handle_document(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Обработчик документов — сразу ставим в очередь с форматом по умолчанию"""
    document = update.message.document
    
    # Проверяем формат
    filename = document.file_name.lower() if document.file_name else ""
    if not (filename.endswith(".fb2") or filename.endswith(".fb2.zip") or filename.endswith(".epub")):
        await update.message.reply_text(
            "⚠️ Принимаю только FB2 и EPUB файлы (.fb2, .fb2.zip, .epub)"
        )
        return

    # Ограничиваем размер
    if document.file_size > 20 * 1024 * 1024:
        await update.message.reply_text(
            "⚠️ Файл слишком большой (максимум 20 МБ)"
        )
        return

    # Проверяем очередь
    if conversion_queue.full():
        await update.message.reply_text(
            f"⏸️ Очередь заполнена ({conversion_queue.qsize()}/{conversion_queue.maxsize} файлов).\n"
            "Попробуй через минуту."
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
        logger.info(f"Файл скачан: {task_info['input_path']}")
    except Exception as e:
        logger.error(f"Ошибка скачивания: {e}")
        await update.message.reply_text("❌ Ошибка при скачивании файла. Попробуй заново.")
        return

    # Ставим в очередь
    await conversion_queue.put(task_info)
    position = conversion_queue.qsize()
    
    # Показываем статус
    msg = await update.message.reply_text(
        f"✅ Файл <b>{document.file_name}</b> добавлен в очередь\n"
        f"Формат: <b>{output_format.upper()}</b>\n"
        f"Позиция: {position} из {conversion_queue.maxsize}",
        parse_mode=ParseMode.HTML
    )
    task_info["message_id"] = msg.message_id


async def post_init(application: Application) -> None:
    """Запуск воркера после старта бота"""
    asyncio.create_task(conversion_worker(application))
    logger.info("✅ Воркер конвертации запущен")


def main() -> None:
    """Запуск бота"""
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    if not token:
        logger.error("❌ Токен не найден! Создай .env с TELEGRAM_BOT_TOKEN")
        return

    # Проверяем зависимости Calibre
    for tool in ["ebook-convert", "ebook-meta"]:
        try:
            subprocess.run([tool, "--version"], capture_output=True, timeout=5)
        except Exception as e:
            logger.error(f"❌ {tool} не установлен: {e}")
            logger.error("Установи: sudo apt install calibre")
            return

    application = Application.builder().token(token).post_init(post_init).build()

    # Регистрируем обработчики
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("settings", settings_menu))
    application.add_handler(CommandHandler("help", help_command))
    application.add_handler(MessageHandler(filters.Document.ALL, handle_document))
    application.add_handler(CallbackQueryHandler(handle_menu_callback, pattern="^menu:"))
    application.add_handler(CallbackQueryHandler(handle_format_setting, pattern="^setfmt:"))

    logger.info("✅ Бот запущен с умной очередью и сохранением обложек!")
    application.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    try:
        main()
    finally:
        settings_db.close()