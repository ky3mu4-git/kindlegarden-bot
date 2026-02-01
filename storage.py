import sqlite3
from pathlib import Path
from typing import Optional

class UserSettings:
    def __init__(self, db_path: str = "data/settings.db"):
        Path("data").mkdir(exist_ok=True)
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self._init_db()
    
    def _init_db(self):
        cursor = self.conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS user_settings (
                user_id INTEGER PRIMARY KEY,
                preferred_format TEXT DEFAULT 'azw3',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        self.conn.commit()
    
    def get_preferred_format(self, user_id: int) -> str:
        cursor = self.conn.cursor()
        cursor.execute(
            "SELECT preferred_format FROM user_settings WHERE user_id = ?",
            (user_id,)
        )
        result = cursor.fetchone()
        return result[0] if result else "azw3"
    
    def set_preferred_format(self, user_id: int, format: str):
        cursor = self.conn.cursor()
        cursor.execute("""
            INSERT OR REPLACE INTO user_settings (user_id, preferred_format, updated_at)
            VALUES (?, ?, CURRENT_TIMESTAMP)
        """, (user_id, format))
        self.conn.commit()
    
    def close(self):
        self.conn.close()