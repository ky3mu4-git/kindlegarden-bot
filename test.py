from telegram.ext import Application
import os

token = "123456:ABC-DEF1234ghIkl-zyx57W2v1u123ew11"  # фейковый токен для теста

try:
    app = Application.builder().token(token).build()
    print("✅ Application created successfully!")
except Exception as e:
    print(f"❌ Error: {e}")
