import logging
import os
import subprocess
import tempfile
from pathlib import Path
from uuid import uuid4
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


# ========== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ==========

def convert_book(input_path: str, output_path: str, output_format: str) -> bool:
    """
    Конвертирует книгу через ebook-convert.
    Поддерживаемые форматы: azw3, epub, mobi
    """
    try:
        # Опции конвертации для лучшего качества на Kindle
        cmd = [
            "ebook-convert",
            input_path,
            output_path,
            "--output-profile", "kindle_pw3",  # оптимизация под современные Kindle
            "--margin-left", "0",
            "--margin-right", "0",
            "--margin-top", "0",
            "--margin-bottom", "0",
            "--extra-css", "body { font-family: serif; line-height: 1.4; }",
        ]
        
        # Дополнительные опции для MOBI (устаревший формат)
        if output_format == "mobi":
            cmd.extend(["--mobi-keep-original-images"])
        
        logger.info(f"Запуск конвертации: {' '.join(cmd)}")
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
        
        if result.returncode != 0:
            logger.error(f"Ошибка конвертации (код {result.returncode}):\n{result.stderr}")
            return False
        
        logger.info(f"Конвертация успешна: {output_path}")
        return True
        
    except subprocess.TimeoutExpired:
        logger.error("Таймаут конвертации (более 120 сек)")
        return False
    except Exception as e:
        logger.error(f"Исключение при конвертации: {e}", exc_info=True)
        return False


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
        "Просто отправь файл — и выбери нужный формат из кнопок ниже. 🚀"
    )
    await update.message.reply_text(message, parse_mode=ParseMode.HTML)


async def handle_document(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Обработчик документов — показывает кнопки выбора формата"""
    document = update.message.document
    
    # Проверяем формат по расширению (MIME-type у FB2 часто неправильный)
    filename = document.file_name.lower() if document.file_name else ""
    if not (filename.endswith(".fb2") or filename.endswith(".fb2.zip") or filename.endswith(".epub")):
        await update.message.reply_text(
            "⚠️ Я принимаю только FB2 и EPUB файлы.\n"
            "Поддерживаются: .fb2, .fb2.zip, .epub"
        )
        return

    # Ограничиваем размер файла (защита от перегрузки малинки)
    if document.file_size > 20 * 1024 * 1024:  # 20 МБ
        await update.message.reply_text(
            "⚠️ Файл слишком большой (максимум 20 МБ).\n"
            "Kindle и так не любит тяжёлые книги 😉"
        )
        return

    # Сохраняем информацию о файле в контексте пользователя
    context.user_data["pending_file"] = {
        "file_id": document.file_id,
        "original_name": document.file_name,
        "mime_type": document.mime_type,
    }

    # Кнопки выбора формата
    keyboard = [
        [
            InlineKeyboardButton("📘 AZW3 (рекомендуется)", callback_data="format:azw3"),
            InlineKeyboardButton("📖 EPUB", callback_data="format:epub"),
        ],
        [
            InlineKeyboardButton("📙 MOBI (устаревший)", callback_data="format:mobi"),
        ],
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    await update.message.reply_text(
        f"✅ Получил файл: <b>{document.file_name}</b>\n\n"
        "Выбери формат для конвертации:",
        reply_markup=reply_markup,
        parse_mode=ParseMode.HTML,
    )
    logger.info(f"User {update.effective_user.id} sent file: {document.file_name}")


async def handle_format_choice(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Обработчик выбора формата через инлайн-кнопки"""
    query = update.callback_query
    await query.answer()

    # Извлекаем выбранный формат
    _, output_format = query.data.split(":")
    
    # Проверяем, есть ли сохранённый файл
    if "pending_file" not in context.user_data:
        await query.edit_message_text("⚠️ Сначала отправь файл для конвертации!")
        return

    file_info = context.user_data["pending_file"]
    original_name = file_info["original_name"]
    file_id = file_info["file_id"]

    # Генерируем имена файлов
    stem = Path(original_name).stem
    input_ext = Path(original_name).suffix
    output_ext = {"azw3": ".azw3", "epub": ".epub", "mobi": ".mobi"}[output_format]
    
    # Создаём уникальные временные пути
    input_path = Path("tmp") / f"{uuid4()}{input_ext}"
    output_path = Path("tmp") / f"{uuid4()}{output_ext}"

    try:
        # Убираем кнопки и показываем статус
        await query.edit_message_text(
            f"⏳ Конвертирую <b>{original_name}</b> в {output_format.upper()}...\n\n"
            "(на малинке 3-й серии это может занять 15–40 секунд)",
            parse_mode=ParseMode.HTML,
        )

        # Скачиваем файл
        file = await context.bot.get_file(file_id)
        await file.download_to_drive(str(input_path))
        logger.info(f"Файл скачан: {input_path}")

        # Конвертируем
        success = convert_book(str(input_path), str(output_path), output_format)

        if not success or not output_path.exists():
            await query.edit_message_text(
                "❌ Ошибка конвертации. Возможно, файл повреждён или содержит нестандартное форматирование.\n\n"
                "Попробуй другой файл или другой формат."
            )
            return

        # Отправляем результат
        output_filename = f"{stem}{output_ext}"
        await context.bot.send_document(
            chat_id=update.effective_chat.id,
            document=open(output_path, "rb"),
            filename=output_filename,
            caption=(
                f"✅ Готово! Конвертировано в <b>{output_format.upper()}</b>\n\n"
                f"📚 {output_filename}\n"
                f"📦 {output_path.stat().st_size / 1024:.1f} КБ"
            ),
            parse_mode=ParseMode.HTML,
        )

        # Обновляем сообщение с кнопками
        format_names = {"azw3": "AZW3", "epub": "EPUB", "mobi": "MOBI"}
        await query.message.reply_text(
            f"Файл успешно сконвертирован в формат <b>{format_names[output_format]}</b>! 🎉\n\n"
            "Отправь ещё один файл для конвертации.",
            parse_mode=ParseMode.HTML,
        )

    except Exception as e:
        logger.error(f"Ошибка при обработке файла: {e}", exc_info=True)
        await query.edit_message_text(
            "❌ Критическая ошибка при конвертации. Администратор уже в курсе!"
        )
    finally:
        # Чистим временные файлы
        try:
            if input_path.exists():
                input_path.unlink()
            if output_path.exists():
                output_path.unlink()
        except Exception as e:
            logger.warning(f"Не удалось удалить временные файлы: {e}")


async def settings(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Обработчик команды /settings (заглушка)"""
    await update.message.reply_text(
        "⚙️ <b>Настройки</b>\n\n"
        "Пока доступна только ручная выборка формата при каждой конвертации.\n"
        "В будущем появится возможность задать формат по умолчанию.",
        parse_mode=ParseMode.HTML,
    )


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

    application = Application.builder().token(token).build()

    # Регистрируем обработчики
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("settings", settings))
    application.add_handler(MessageHandler(filters.Document.ALL, handle_document))
    application.add_handler(CallbackQueryHandler(handle_format_choice, pattern="^format:"))

    logger.info("✅ Бот запущен и готов к конвертации!")
    application.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()