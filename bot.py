import logging
import os
from pathlib import Path
from dotenv import load_dotenv
from telegram import Update
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes

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


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Обработчик команды /start"""
    await update.message.reply_text(
        "📚 Привет! Я бот для конвертации книг в формат Kindle.\n\n"
        "Просто отправь мне файл в формате FB2 — и я преобразую его в AZW3 или EPUB.\n\n"
        "Настройки: /settings"
    )


async def handle_document(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Обработчик документов (заглушка)"""
    document = update.message.document
    
    # Проверяем формат по расширению (MIME-type у FB2 часто неправильный)
    filename = document.file_name.lower() if document.file_name else ""
    if not (filename.endswith(".fb2") or filename.endswith(".fb2.zip") or filename.endswith(".epub")):
        await update.message.reply_text(
            "⚠️ Я принимаю только FB2 и EPUB файлы.\n"
            "Поддерживаются: .fb2, .fb2.zip, .epub"
        )
        return

    await update.message.reply_text(
        f"✅ Получил файл: {document.file_name}\n\n"
        "Скоро здесь будет конвертация! 🚀"
    )
    logger.info(f"User {update.effective_user.id} sent file: {document.file_name}")


async def settings(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Обработчик команды /settings"""
    await update.message.reply_text(
        "⚙️ Настройки:\n"
        "Пока доступна только одна настройка — формат по умолчанию.\n"
        "В будущем здесь появятся дополнительные опции."
    )


def main() -> None:
    """Запуск бота"""
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    if not token:
        logger.error("❌ Токен не найден! Создай файл .env с TELEGRAM_BOT_TOKEN")
        return

    application = Application.builder().token(token).build()

    # Регистрируем обработчики
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("settings", settings))
    application.add_handler(MessageHandler(filters.Document.ALL, handle_document))

    logger.info("✅ Бот запущен!")
    application.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()