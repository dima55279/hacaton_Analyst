import logging
import sys
import json
import multiprocessing
from datetime import datetime
from database.database_manager import DatabaseManager
from gigachat.api_client import GigaChatClient
from processing.analyzer import AppealsAnalyzer
from bot.citizen_bot import CitizenBot
from bot.analyst_bot import AnalystBot
from web.dashboard import create_dashboard_app
from processing.data_parser import SettlementParser  # Добавляем импорт парсера

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
        # Используем единый менеджер базы данных
        self.database = DatabaseManager(config['mysql_config'])
        self.analyzer = AppealsAnalyzer(self.gigachat, self.database)
        
    def process_citizen_appeal(self, user_id, appeal_text, platform="telegram", address_info=None):
        """Обработка обращения гражданина с адресом"""
        try:
            # Классификация обращения
            appeal_type = self.analyzer.classify_appeal(appeal_text)
            
            # Формируем данные для сохранения
            appeal_data = {
                'user_id': user_id,
                'text': appeal_text,
                'type': appeal_type,
                'platform': platform,
                'status': 'new',
                'created_at': datetime.now()
            }
            
            # Добавляем информацию об адресе, если есть
            if address_info:
                appeal_data.update({
                    'settlement': address_info.get('settlement'),
                    'street': address_info.get('street'),
                    'house': address_info.get('house'),
                    'full_address': address_info.get('full_address')
                })
            
            # Сохранение в базу
            appeal_id = self.database.store_appeal(appeal_data)
            
            # Генерация ответа для типовых обращений с передачей адресной информации
            if appeal_type in self.analyzer.get_common_types():
                response = self.analyzer.generate_response(appeal_id, appeal_text, appeal_type, address_info)
                self.database.update_appeal(appeal_id, {'response': response, 'status': 'answered'})
                return response
            else:
                # Для нетиповых обращений также генерируем ответ с контактами муниципалитета
                response = self.analyzer.generate_response(appeal_id, appeal_text, appeal_type, address_info)
                self.database.update_appeal(appeal_id, {'response': response, 'status': 'requires_manual_review'})
                return response
                
        except Exception as e:
            logger.error(f"Ошибка обработки обращения: {e}")
            return "Произошла ошибка при обработке обращения. Пожалуйста, попробуйте позже."

    def get_analytics(self, period_days=30):
        """Получение аналитики за период"""
        return self.analyzer.analyze_trends(period_days)

def init_settlements_database(config):
    """Инициализация базы данных населенных пунктов"""
    try:
        from processing.data_parser import SettlementParser
        
        parser = SettlementParser(config['mysql_config'])
        success = parser.run()
        
        if success:
            logger.info("✅ База данных населенных пунктов инициализирована")
        else:
            logger.error("❌ Не удалось инициализировать базу населенных пунктов")
            
        return success
    except Exception as e:
        logger.error(f"❌ Ошибка инициализации базы населенных пунктов: {e}")
        return False

def run_citizen_bot(config):
    """Запуск бота для граждан в отдельном процессе"""
    system = AppealsProcessingSystem(config)
    citizen_bot = CitizenBot(config['telegram_bot_token'], system, config['mysql_config'])  # Передаем db_config
    logger.info("🚀 Запуск бота для граждан...")
    citizen_bot.run()

def run_analyst_bot(config):
    """Запуск бота для аналитиков в отдельном процессе"""
    system = AppealsProcessingSystem(config)
    analyst_bot = AnalystBot(config['analyst_bot_token'], system)
    logger.info("🚀 Запуск бота для аналитиков...")
    analyst_bot.run()

def run_dashboard(config):
    """Запуск веб-интерфейса в отдельном процессе"""
    system = AppealsProcessingSystem(config)
    web_app = create_dashboard_app(system)
    web_port = config.get('web_port', 5000)
    logger.info(f"🚀 Запуск веб-интерфейса на порту {web_port}...")
    web_app.run(host='0.0.0.0', port=web_port, debug=False, use_reloader=False)

def main():
    try:
        # Загрузка конфигурации
        with open("config.json", "r") as f:
            config = json.load(f)
        
        # Инициализируем единый менеджер базы данных
        DatabaseManager(config['mysql_config'])
        
        # Инициализируем базу данных населенных пунктов
        init_settlements_database(config)
        
        logger.info("✅ Система обработки обращений инициализирована")
        
        # Создание процессов для каждого компонента
        processes = []
        
        # Процесс для бота граждан
        citizen_process = multiprocessing.Process(target=run_citizen_bot, args=(config,))
        processes.append(citizen_process)
        
        # Процесс для бота аналитиков
        analyst_process = multiprocessing.Process(target=run_analyst_bot, args=(config,))
        processes.append(analyst_process)
        
        # Процесс для веб-интерфейса
        dashboard_process = multiprocessing.Process(target=run_dashboard, args=(config,))
        processes.append(dashboard_process)
        
        # Запуск всех процессов
        for process in processes:
            process.start()
        
        logger.info("✅ Все компоненты системы запущены")
        
        # Ожидание завершения процессов
        for process in processes:
            process.join()
            
    except Exception as e:
        logger.error(f"❌ Ошибка запуска системы: {e}")
    finally:
        # Закрываем соединение с базой данных при завершении
        db_manager = DatabaseManager()
        db_manager.close()

if __name__ == "__main__":
    main()