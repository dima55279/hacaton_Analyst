import mysql.connector
from mysql.connector import Error
import logging
from datetime import datetime
import threading
import json

logger = logging.getLogger(__name__)

class DatabaseManager:
    _instance = None
    _lock = threading.Lock()
    
    def __new__(cls, config=None):
        with cls._lock:
            if cls._instance is None:
                cls._instance = super(DatabaseManager, cls).__new__(cls)
                if config:
                    cls._instance._initialize(config)
            return cls._instance
    
    def _initialize(self, config):
        self.config = config
        self.connection = None
        self._connect()
        self._create_tables()
    
    def _connect(self):
        """Подключение к MySQL"""
        try:
            self.connection = mysql.connector.connect(**self.config)
            self.connection.autocommit = True
            logger.info("✅ Успешное подключение к MySQL")
        except Error as e:
            logger.error(f"❌ Ошибка подключения к MySQL: {e}")
            raise
    
    def get_connection(self):
        """Получение соединения с базой данных"""
        try:
            if not self.connection or not self.connection.is_connected():
                self._connect()
            return self.connection
        except Error as e:
            logger.error(f"❌ Ошибка получения соединения: {e}")
            raise

    def _create_tables(self):
        """Создание таблиц для системы обращений и населенных пунктов"""
        try:
            cursor = self.connection.cursor()

            # Таблица обращений (обновленная с полями для адреса)
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
                settlement VARCHAR(255),
                street VARCHAR(255),
                house VARCHAR(50),
                full_address TEXT,
                municipality_contacts JSON,  -- Новое поле для хранения контактов муниципалитета
                INDEX idx_user (user_id),
                INDEX idx_type (type),
                INDEX idx_status (status),
                INDEX idx_created (created_at)
            )
            """

            # Таблица трендов
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

            # Таблица населенных пунктов (для парсера)
            create_settlements_table = """
            CREATE TABLE IF NOT EXISTS settlements (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                type VARCHAR(100) NOT NULL,
                district VARCHAR(255),
                population INT,
                latitude DECIMAL(10, 8),
                longitude DECIMAL(11, 8),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                INDEX idx_name (name),
                INDEX idx_district (district)
            )
            """
            cursor.execute(create_appeals_table)
            cursor.execute(create_trends_table)
            cursor.execute(create_settlements_table)
            self.connection.commit()
            cursor.close()
            logger.info("✅ Таблицы созданы успешно")

        except Error as e:
            logger.error(f"❌ Ошибка создания таблиц: {e}")
            raise

    def store_appeal(self, appeal_data):
        """Сохранение обращения в базу с поддержкой адреса"""
        conn = self.get_connection()
        try:
            cursor = conn.cursor()
            
            # Определяем поля и значения в зависимости от наличия адреса
            fields = ['user_id', 'text', 'type', 'platform', 'status', 'created_at']
            placeholders = ['%s'] * len(fields)
            values = [
                appeal_data['user_id'],
                appeal_data['text'],
                appeal_data.get('type'),
                appeal_data.get('platform'),
                appeal_data.get('status', 'new'),
                appeal_data.get('created_at')
            ]
            
            # Добавляем поля адреса, если они есть
            address_fields = ['settlement', 'street', 'house', 'full_address']
            for field in address_fields:
                if field in appeal_data and appeal_data[field]:
                    fields.append(field)
                    placeholders.append('%s')
                    values.append(appeal_data[field])
            
            query = f"""
            INSERT INTO appeals ({', '.join(fields)})
            VALUES ({', '.join(placeholders)})
            """
            
            cursor.execute(query, values)
            appeal_id = cursor.lastrowid
            cursor.close()
            
            logger.info(f"💾 Сохранено обращение ID: {appeal_id}")
            return appeal_id
            
        except Error as e:
            logger.error(f"❌ Ошибка сохранения обращения: {e}")
            raise

    # Остальные методы остаются без изменений...
    def update_appeal(self, appeal_id, update_data):
        """Обновление обращения"""
        conn = self.get_connection()
        try:
            cursor = conn.cursor()
            
            set_clause = ", ".join([f"{key} = %s" for key in update_data.keys()])
            values = list(update_data.values())
            values.append(appeal_id)
            
            query = f"UPDATE appeals SET {set_clause} WHERE id = %s"
            
            cursor.execute(query, values)
            cursor.close()
            
            logger.info(f"✏️ Обновлено обращение ID: {appeal_id}")
            
        except Error as e:
            logger.error(f"❌ Ошибка обновления обращения: {e}")
            raise

    def get_appeals(self, filters=None, limit=100, offset=0):
        """Получение обращений с фильтрами"""
        conn = self.get_connection()
        try:
            cursor = conn.cursor(dictionary=True)
            
            where_clause = "WHERE 1=1"
            params = []
            
            if filters:
                if 'user_id' in filters:
                    where_clause += " AND user_id = %s"
                    params.append(filters['user_id'])
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

    def get_recent_appeals(self, limit=10):
        """Получение последних обращений (актуальные данные)"""
        conn = self.get_connection()
        try:
            cursor = conn.cursor(dictionary=True)
            
            query = """
            SELECT * FROM appeals 
            ORDER BY created_at DESC 
            LIMIT %s
            """
            
            cursor.execute(query, (limit,))
            appeals = cursor.fetchall()
            cursor.close()
            
            logger.info(f"📝 Получено {len(appeals)} последних обращений")
            return appeals
            
        except Error as e:
            logger.error(f"❌ Ошибка получения последних обращений: {e}")
            return []

    def get_appeals_stats(self, period_days=30):
        """Статистика по обращениям за период"""
        conn = self.get_connection()
        try:
            cursor = conn.cursor(dictionary=True)
            
            query = """
            SELECT 
                type,
                status,
                COUNT(*) as count
            FROM appeals 
            WHERE created_at >= DATE_SUB(NOW(), INTERVAL %s DAY)
            GROUP BY type, status
            ORDER BY count DESC
            """
            
            cursor.execute(query, (period_days,))
            stats = cursor.fetchall()
            cursor.close()
            
            return stats
            
        except Error as e:
            logger.error(f"❌ Ошибка получения статистики: {e}")
            return []

    def get_real_time_stats(self):
        """Получение актуальной статистики в реальном времени"""
        conn = self.get_connection()
        try:
            cursor = conn.cursor(dictionary=True)
            
            # Общее количество обращений
            cursor.execute("SELECT COUNT(*) as total FROM appeals")
            total = cursor.fetchone()['total']
            
            # По статусам
            cursor.execute("""
                SELECT status, COUNT(*) as count 
                FROM appeals 
                GROUP BY status
            """)
            status_stats = {row['status']: row['count'] for row in cursor.fetchall()}
            
            # По типам (топ-5)
            cursor.execute("""
                SELECT type, COUNT(*) as count 
                FROM appeals 
                WHERE type IS NOT NULL 
                GROUP BY type 
                ORDER BY count DESC 
                LIMIT 5
            """)
            type_stats = cursor.fetchall()
            
            # Последние 24 часа
            cursor.execute("""
                SELECT COUNT(*) as last_24h 
                FROM appeals 
                WHERE created_at >= DATE_SUB(NOW(), INTERVAL 24 HOUR)
            """)
            last_24h = cursor.fetchone()['last_24h']
            
            cursor.close()
            
            logger.info(f"📊 Реальная статистика: всего {total}, за 24ч: {last_24h}")
            
            return {
                'total': total,
                'status_stats': status_stats,
                'type_stats': type_stats,
                'last_24h': last_24h
            }
            
        except Error as e:
            logger.error(f"❌ Ошибка получения реальной статистики: {e}")
            return {}

    def close(self):
        """Закрытие соединения"""
        if self.connection and self.connection.is_connected():
            self.connection.close()
            logger.info("🔌 Соединение с MySQL закрыто")