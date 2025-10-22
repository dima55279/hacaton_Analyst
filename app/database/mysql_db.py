import mysql.connector
from mysql.connector import Error
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

class MySQLAppealsDB:
    def __init__(self, config):
        self.config = config
        self.connection = None
        self._connect()
        self._create_tables()

    def _connect(self):
        """Подключение к MySQL"""
        try:
            self.connection = mysql.connector.connect(**self.config)
            logger.info("✅ Успешное подключение к MySQL")
        except Error as e:
            logger.error(f"❌ Ошибка подключения к MySQL: {e}")
            raise

    def _create_tables(self):
        """Создание таблиц для системы обращений"""
        try:
            cursor = self.connection.cursor()

            create_appeals_table = """
            CREATE TABLE IF NOT EXISTS appeals (
                id INT AUTO_INCREMENT PRIMARY KEY,
                user_id VARCHAR(255) NOT NULL,
                text TEXT NOT NULL,
                type VARCHAR(100),
                platform VARCHAR(50),
                status VARCHAR(50) DEFAULT 'new',
                response TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                responded_at TIMESTAMP NULL,
                tags JSON,
                INDEX idx_user (user_id),
                INDEX idx_type (type),
                INDEX idx_status (status),
                INDEX idx_created (created_at)
            )
            """

            create_trends_table = """
            CREATE TABLE IF NOT EXISTS trends (
                id INT AUTO_INCREMENT PRIMARY KEY,
                keyword VARCHAR(255) NOT NULL,
                frequency INT DEFAULT 0,
                period DATE NOT NULL,
                appeal_type VARCHAR(100),
                UNIQUE KEY unique_trend (keyword, period, appeal_type)
            )
            """

            cursor.execute(create_appeals_table)
            cursor.execute(create_trends_table)
            self.connection.commit()
            cursor.close()
            logger.info("✅ Таблицы созданы успешно")

        except Error as e:
            logger.error(f"❌ Ошибка создания таблиц: {e}")
            raise

    def store_appeal(self, appeal_data):
        """Сохранение обращения в базу"""
        try:
            cursor = self.connection.cursor()
            
            query = """
            INSERT INTO appeals (user_id, text, type, platform, status, created_at)
            VALUES (%s, %s, %s, %s, %s, %s)
            """
            
            cursor.execute(query, (
                appeal_data['user_id'],
                appeal_data['text'],
                appeal_data.get('type'),
                appeal_data.get('platform'),
                appeal_data.get('status', 'new'),
                appeal_data.get('created_at')
            ))
            
            appeal_id = cursor.lastrowid
            self.connection.commit()
            cursor.close()
            
            logger.info(f"💾 Сохранено обращение ID: {appeal_id}")
            return appeal_id
            
        except Error as e:
            logger.error(f"❌ Ошибка сохранения обращения: {e}")
            self.connection.rollback()
            raise

    def update_appeal(self, appeal_id, update_data):
        """Обновление обращения"""
        try:
            cursor = self.connection.cursor()
            
            set_clause = ", ".join([f"{key} = %s" for key in update_data.keys()])
            values = list(update_data.values())
            values.append(appeal_id)
            
            query = f"UPDATE appeals SET {set_clause} WHERE id = %s"
            
            cursor.execute(query, values)
            self.connection.commit()
            cursor.close()
            
            logger.info(f"✏️ Обновлено обращение ID: {appeal_id}")
            
        except Error as e:
            logger.error(f"❌ Ошибка обновления обращения: {e}")
            self.connection.rollback()
            raise

    def get_appeals(self, filters=None, limit=100, offset=0):
        """Получение обращений с фильтрами"""
        try:
            cursor = self.connection.cursor(dictionary=True)
            
            where_clause = "WHERE 1=1"
            params = []
            
            if filters:
                if 'type' in filters:
                    where_clause += " AND type = %s"
                    params.append(filters['type'])
                if 'status' in filters:
                    where_clause += " AND status = %s"
                    params.append(filters['status'])
                if 'date_from' in filters:
                    where_clause += " AND created_at >= %s"
                    params.append(filters['date_from'])
                if 'date_to' in filters:
                    where_clause += " AND created_at <= %s"
                    params.append(filters['date_to'])
            
            query = f"""
            SELECT * FROM appeals 
            {where_clause}
            ORDER BY created_at DESC 
            LIMIT %s OFFSET %s
            """
            
            params.extend([limit, offset])
            cursor.execute(query, params)
            appeals = cursor.fetchall()
            cursor.close()
            
            return appeals
            
        except Error as e:
            logger.error(f"❌ Ошибка получения обращений: {e}")
            return []

    def get_appeals_stats(self, period_days=30):
        """Статистика по обращениям"""
        try:
            cursor = self.connection.cursor(dictionary=True)
            
            query = """
            SELECT 
                type,
                status,
                COUNT(*) as count,
                DATE(created_at) as date
            FROM appeals 
            WHERE created_at >= DATE_SUB(NOW(), INTERVAL %s DAY)
            GROUP BY type, status, DATE(created_at)
            ORDER BY date DESC, count DESC
            """
            
            cursor.execute(query, (period_days,))
            stats = cursor.fetchall()
            cursor.close()
            
            return stats
            
        except Error as e:
            logger.error(f"❌ Ошибка получения статистики: {e}")
            return []

    def close(self):
        """Закрытие соединения"""
        if self.connection:
            self.connection.close()
            logger.info("🔌 Соединение с MySQL закрыто")