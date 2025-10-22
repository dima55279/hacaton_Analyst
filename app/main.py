import logging
import sys
import json
from datetime import datetime
from database.mysql_db import MySQLAppealsDB
from gigachat.api_client import GigaChatClient
from processing.analyzer import AppealsAnalyzer
from bot.citizen_bot import CitizenBot
from bot.analyst_bot import AnalystBot
from web.dashboard import create_dashboard_app

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("appeals_system.log", encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)

class AppealsProcessingSystem:
    def __init__(self, config):
        self.config = config
        self.gigachat = GigaChatClient(config['gigachat_api_key'])
        self.database = MySQLAppealsDB(config['mysql_config'])
        self.analyzer = AppealsAnalyzer(self.gigachat, self.database)
        
    def process_citizen_appeal(self, user_id, appeal_text, platform="telegram"):
        """Обработка обращения гражданина"""
        try:
            # Классификация обращения
            appeal_type = self.analyzer.classify_appeal(appeal_text)
            
            # Сохранение в базу
            appeal_id = self.database.store_appeal({
                'user_id': user_id,
                'text': appeal_text,
                'type': appeal_type,
                'platform': platform,
                'status': 'new',
                'created_at': datetime.now()
            })
            
            # Генерация ответа для типовых обращений
            if appeal_type in self.analyzer.get_common_types():
                response = self.analyzer.generate_response(appeal_id, appeal_text, appeal_type)
                self.database.update_appeal(appeal_id, {'response': response, 'status': 'answered'})
                return response
            else:
                self.database.update_appeal(appeal_id, {'status': 'requires_manual_review'})
                return "Ваше обращение принято и передано специалисту. Ответ будет предоставлен в ближайшее время."
                
        except Exception as e:
            logger.error(f"Ошибка обработки обращения: {e}")
            return "Произошла ошибка при обработке обращения. Пожалуйста, попробуйте позже."

    def get_analytics(self, period_days=30):
        """Получение аналитики за период"""
        return self.analyzer.analyze_trends(period_days)

def main():
    try:
        # Загрузка конфигурации
        with open("config.json", "r") as f:
            config = json.load(f)
        
        # Инициализация системы
        system = AppealsProcessingSystem(config)
        
        # Запуск бота для граждан
        citizen_bot = CitizenBot(
            config['telegram_bot_token'],
            system
        )
        
        # Запуск бота для аналитиков
        analyst_bot = AnalystBot(
            config['analyst_bot_token'],
            system
        )
        
        # Запуск веб-интерфейса
        web_app = create_dashboard_app(system)
        
        logger.info("Система обработки обращений запущена")
        
        # В продакшене лучше использовать отдельные процессы
        citizen_bot.run()
        # analyst_bot.run() в отдельном процессе
        # web_app.run() в отдельном процессе
        
    except Exception as e:
        logger.error(f"Ошибка запуска системы: {e}")

if __name__ == "__main__":
    main()