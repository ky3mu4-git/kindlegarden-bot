import asyncio
import logging
import os
import subprocess
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

# ========== ГЛОБАЛЬНАЯ ОЧЕРЕДЬ ЗАДАЧ ==========
conversion_queue = asyncio.Queue(maxsize=5)
active_tasks = {}


# ========== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ==========

def convert_book(input_path: str, output_path: str, output_format: str) -> bool:
    """Конвертирует книгу через ebook-convert"""
    try:
        cmd = [
            "ebook-convert",
            input_path,
            output_path,
            "--output-profile", "kindle_pw3",
            "--margin-left", "0",
            "--margin-right", "0",
            "--margin-top", "0",
            "--margin-bottom", "0",
            "--extra-css", "body { font-family: serif; line-height: 1.4; }",
        ]
        
        if output_format == "mobi":
            cmd.extend(["--mobi-keep-original-images"])
        
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
    """Воркер — берёт задачи из очереди и конвертирует по одной"""
    logger.info("🔄 Запущен воркер конвертации (1 задача одновременно)")
    
    while True:
        try:
            task = await conversion_queue.get()
            task_id = task["task_id"]
            
            # Обновляем статус
            active_tasks[task_id]["status"] = "converting"
            await _update_status_message(application, task_id, "⏳ Конвертирую...")
            
            # Конвертируем
            success = convert_book(
                task["input_path"],
                task["output_path"],
                task["output_format"]
            )
            
            # Отправляем результат
            if success and Path(task["output_path"]).exists():
                await _send_result(application, task, success=True)
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
    """Обновляет сообщение со статусом для пользователя"""
    task = active_tasks.get(task_id)
    if not task or not task.get("message_id"):
        return
    
    try:
        await application.bot.edit_message_text(
            chat_id=task["user_id"],
            message_id=task["message_id"],
            text=(
                f"📚 <b>{task['file_name']}</b>\n\n"
                f"{status_text}\n"
                f"Позиция в очереди: {conversion_queue.qsize() + 1 if 'converting' not in status_text else 'обработка'}"
            ),
            parse_mode=ParseMode.HTML,
            reply_markup=_get_cancel_keyboard(task_id) if "ожидает" in status_text.lower() else None
        )
    except Exception as e:
        logger.warning(f"Не удалось обновить статус: {e}")


async def _send_result(application: Application, task: dict, success: bool):
    """Отправляет результат конвертации пользователю"""
    try:
        if success:
            output_filename = f"{Path(task['file_name']).stem}.{task['output_format']}"
            await application.bot.send_document(
                chat_id=task["user_id"],
                document=open(task["output_path"], "rb"),
                filename=output_filename,
                caption=(
                    f"✅ Готово! Сконвертировано в <b>{task['output_format'].upper()}</b>\n\n"
                    f"📚 {output_filename}\n"
                    f"📦 {Path(task['output_path']).stat().st_size / 1024:.1f} КБ"
                ),
                parse_mode=ParseMode.HTML,
            )
            await application.bot.send_message(
                chat_id=task["user_id"],
                text="Файл готов к отправке на Kindle! 🚀\n\nОтправь ещё один файл для конвертации."
            )
        else:
            await application.bot.send_message(
                chat_id=task["user_id"],
                text=(
                    "❌ Ошибка конвертации файла <b>{}</b>.\n\n"
                    "Возможные причины:\n"
                    "• Повреждённый FB2\n"
                    "• Нестандартное форматирование\n"
                    "• Слишком большой файл (>20 МБ)\n\n"
                    "Попробуй другой файл или формат."
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


def _get_cancel_keyboard(task_id: str) -> InlineKeyboardMarkup:
    """Кнопка отмены для файлов в очереди"""
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🚫 Отменить", callback_data=f"cancel:{task_id}")]
    ])


def _get_format_keyboard() -> InlineKeyboardMarkup:
    """Кнопки выбора формата"""
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("📘 AZW3 (рекомендуется)", callback_data="format:azw3"),
            InlineKeyboardButton("📖 EPUB", callback_data="format:epub"),
        ],
        [
            InlineKeyboardButton("📙 MOBI (устаревший)", callback_data="format:mobi"),
        ],
    ])


# ========== ОБРАБОТЧИКИ ==========

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Обработчик команды /start"""
    message = (
        "📚 <b>KindleGarden Bot</b>\n\n"
        "Отправляй мне книги в формате FB2 или EPUB — я конвертирую их для Kindle!\n\n"
        "✅ <b>Поддерживаемые выходные форматы:</b>\n"
        "• <b>AZW3</b> — рекомендуемый формат для современных Kindle (лучшая типографика, оглавление, шрифты)\n"
        "• <b>EPUB</b> — универсальный формат, поддерживается всеми Kindle с 2022 года\n"
        "• <b>MOBI</b> — устаревший формат для очень старых устройств (ограниченная функциональность)\n\n"
        "✨ <b>Особенности:</b>\n"
        "• Можно отправлять несколько файлов подряд — они встанут в очередь\n"
        "• Максимум 5 файлов в очереди (защита от перегрузки сервера)\n"
        "• Статус конвертации в реальном времени\n\n"
        "Просто отправь файл — и выбери нужный формат. 🚀"
    )
    await update.message.reply_text(message, parse_mode=ParseMode.HTML)


async def handle_document(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Обработчик документов — ставит файл в очередь"""
    document = update.message.document
    
    # Проверяем формат
    filename = document.file_name.lower() if document.file_name else ""
    if not (filename.endswith(".fb2") or filename.endswith(".fb2.zip") or filename.endswith(".epub")):
        await update.message.reply_text(
            "⚠️ Я принимаю только FB2 и EPUB файлы.\n"
            "Поддерживаются: .fb2, .fb2.zip, .epub"
        )
        return

    # Ограничиваем размер
    if document.file_size > 20 * 1024 * 1024:
        await update.message.reply_text(
            "⚠️ Файл слишком большой (максимум 20 МБ).\n"
            "Kindle и так не любит тяжёлые книги 😉"
        )
        return

    # Проверяем переполнение очереди
    if conversion_queue.full():
        await update.message.reply_text(
            "⏸️ Очередь заполнена (максимум 5 файлов).\n"
            f"Сейчас в обработке: {conversion_queue.qsize()} файлов\n"
            "Попробуй отправить файл через минуту."
        )
        return

    # Генерируем уникальный ID задачи
    task_id = str(uuid4())
    input_ext = Path(filename).suffix or ".fb2"
    
    # Сохраняем информацию о файле
    task_info = {
        "task_id": task_id,
        "user_id": update.effective_user.id,
        "file_id": document.file_id,
        "file_name": document.file_name,
        "mime_type": document.mime_type,
        "input_path": str(Path("tmp") / f"{task_id}{input_ext}"),
        "output_path": "",  # будет задан после выбора формата
        "output_format": None,
        "status": "awaiting_format",
        "queued_at": datetime.now(),
    }
    active_tasks[task_id] = task_info

    # Показываем кнопки выбора формата
    msg = await update.message.reply_text(
        f"✅ Получил файл: <b>{document.file_name}</b>\n\n"
        "Выбери формат для конвертации:",
        reply_markup=_get_format_keyboard(),
        parse_mode=ParseMode.HTML,
    )
    active_tasks[task_id]["message_id"] = msg.message_id


async def handle_format_choice(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Обработчик выбора формата — ставит задачу в очередь"""
    query = update.callback_query
    await query.answer()

    _, output_format = query.data.split(":")
    task_id = None

    # Ищем задачу пользователя по сообщению с кнопками
    for tid, task in active_tasks.items():
        if task.get("message_id") == query.message.id and task["status"] == "awaiting_format":
            task_id = tid
            break
    
    if not task_id:
        await query.edit_message_text("⚠️ Задача не найдена. Отправь файл заново.")
        return

    task = active_tasks[task_id]
    task["output_format"] = output_format
    task["status"] = "queued"
    
    # Обновляем путь выходного файла
    output_ext = {"azw3": ".azw3", "epub": ".epub", "mobi": ".mobi"}[output_format]
    task["output_path"] = str(Path("tmp") / f"{task_id}{output_ext}")

    # Скачиваем файл ДО постановки в очередь (чтобы не блокировать воркер)
    try:
        file = await context.bot.get_file(task["file_id"])
        await file.download_to_drive(task["input_path"])
        logger.info(f"Файл скачан: {task['input_path']}")
    except Exception as e:
        logger.error(f"Ошибка скачивания файла: {e}")
        await query.edit_message_text("❌ Ошибка при скачивании файла. Попробуй отправить заново.")
        active_tasks.pop(task_id, None)
        return

    # Ставим в очередь
    try:
        await conversion_queue.put(task)
        position = conversion_queue.qsize()
        
        await query.edit_message_text(
            f"📚 <b>{task['file_name']}</b>\n\n"
            f"✅ Выбран формат: <b>{output_format.upper()}</b>\n"
            f"В очереди: {position} файл(ов)\n"
            f"Ожидаемая задержка: ~{position * 25} сек",
            parse_mode=ParseMode.HTML,
            reply_markup=_get_cancel_keyboard(task_id)
        )
    except asyncio.QueueFull:
        await query.edit_message_text("❌ Очередь переполнена. Попробуй позже.")
        active_tasks.pop(task_id, None)


async def handle_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Отмена задачи из очереди"""
    query = update.callback_query
    await query.answer()

    _, task_id = query.data.split(":")
    task = active_tasks.get(task_id)
    
    if not task:
        await query.edit_message_text("⚠️ Задача уже обработана или удалена.")
        return

    if task["status"] == "converting":
        await query.edit_message_text(
            "⚠️ Конвертация уже началась — отмена невозможна.\n"
            "Подожди ~30 секунд для результата."
        )
        return

    # Удаляем задачу
    active_tasks.pop(task_id, None)
    _cleanup_temp_files(task["input_path"], task["output_path"])
    
    await query.edit_message_text(
        f"🚫 Конвертация <b>{task['file_name']}</b> отменена.",
        parse_mode=ParseMode.HTML
    )


async def settings(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Обработчик команды /settings"""
    await update.message.reply_text(
        f"⚙️ <b>Настройки</b>\n\n"
        f"Статус очереди:\n"
        f"В обработке: {conversion_queue.qsize()} / {conversion_queue.maxsize} файлов\n\n"
        "Пока доступна только ручная выборка формата при каждой конвертации.\n"
        "В будущем появится возможность задать формат по умолчанию.",
        parse_mode=ParseMode.HTML,
    )


# ========== ЗАПУСК БОТА ==========

async def post_init(application: Application) -> None:
    """Запускает воркер конвертации после старта бота"""
    asyncio.create_task(conversion_worker(application))
    logger.info("✅ Воркер конвертации запущен")


def main() -> None:
    """Запуск бота"""
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    if not token:
        logger.error("❌ Токен не найден! Создай файл .env с TELEGRAM_BOT_TOKEN")
        return

    # Проверяем наличие ebook-convert
    try:
        result = subprocess.run(
            ["ebook-convert", "--version"],
            capture_output=True,
            text=True,
            timeout=5
        )
        logger.info(f"Calibre обнаружен: {result.stdout.strip()}")
    except Exception as e:
        logger.error(f"❌ Calibre не установлен или недоступен: {e}")
        logger.error("Установи: sudo apt install calibre")
        return

    application = Application.builder().token(token).post_init(post_init).build()

    # Регистрируем обработчики
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("settings", settings))
    application.add_handler(MessageHandler(filters.Document.ALL, handle_document))
    application.add_handler(CallbackQueryHandler(handle_format_choice, pattern="^format:"))
    application.add_handler(CallbackQueryHandler(handle_cancel, pattern="^cancel:"))

    logger.info("✅ Бот запущен с очередью задач (макс. 5 файлов)!")
    application.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()