import requests
from bs4 import BeautifulSoup
import mysql.connector
import logging
import re
import urllib3

# Отключаем предупреждения о небезопасных соединениях
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logger = logging.getLogger(__name__)

class SettlementParser:
    def __init__(self, db_config):
        self.db_config = db_config
        self.target_url = "https://ru.ruwiki.ru/wiki/Населённые_пункты_Тамбовской_области"
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        }
        self.session = requests.Session()
        self.session.verify = False
    
    def parse_ruwiki_tables(self):
        """Парсинг всех таблиц с населенными пунктами с ru.ruwiki.ru"""
        try:
            logger.info(f"🔍 Загрузка страницы: {self.target_url}")
            
            response = self.session.get(
                self.target_url, 
                headers=self.headers, 
                timeout=30,
                verify=False
            )
            response.encoding = 'utf-8'
            
            if response.status_code != 200:
                logger.error(f"❌ Ошибка загрузки страницы: {response.status_code}")
                return []
            
            soup = BeautifulSoup(response.content, 'html.parser')
            all_settlements = []
            
            # Ищем все таблицы на странице
            tables = soup.find_all('table', {'class': 'standard'})
            logger.info(f"📊 Найдено таблиц: {len(tables)}")
            
            if not tables:
                logger.error("❌ Не найдено таблиц на странице")
                return []
            
            # Обрабатываем первую таблицу отдельно (городские округа)
            if len(tables) > 0:
                logger.info("🏙️ Обработка таблицы городских округов")
                urban_settlements = self.parse_urban_table_simple(tables[0])
                all_settlements.extend(urban_settlements)
                logger.info(f"✅ Городские округа: обработано {len(urban_settlements)} населенных пунктов")
            
            # Обрабатываем остальные таблицы (районы)
            for i, table in enumerate(tables[1:], 1):
                try:
                    logger.info(f"🏘️ Обработка таблицы района {i}")
                    district_settlements = self.parse_district_table_simple(table, i)
                    all_settlements.extend(district_settlements)
                    logger.info(f"✅ Таблица района {i}: обработано {len(district_settlements)} населенных пунктов")
                except Exception as e:
                    logger.error(f"❌ Ошибка обработки таблицы района {i}: {e}")
                    continue
            
            logger.info(f"🎯 Всего обработано населенных пунктов: {len(all_settlements)}")
            return all_settlements
            
        except Exception as e:
            logger.error(f"❌ Ошибка парсинга таблиц: {e}")
            return []

    def parse_urban_table_simple(self, table):
        """Простой и надежный парсинг таблицы городских округов"""
        settlements = []
        rows = table.find_all('tr')
        
        if len(rows) < 2:
            return []
        
        logger.info("🔍 Анализ структуры таблицы городских округов...")
        
        # Пропускаем заголовок и анализируем первую строку данных
        for i, row in enumerate(rows[1:], 1):
            try:
                cells = row.find_all(['td', 'th'])
                
                # Пропускаем строки с недостаточным количеством ячеек
                if len(cells) < 4:
                    continue
                
                # Логируем содержимое для отладки
                cell_contents = [self.clean_text(cell.get_text()) for cell in cells]
                logger.info(f"📋 Строка {i}: {cell_contents}")
                
                # Простая логика: предполагаем стандартную структуру
                # 0: номер, 1: название, 2: тип, 3: население, 4: муниципалитет
                if len(cells) >= 5:
                    # Стандартная структура с номером
                    name = self.clean_text(cells[1].get_text())
                    settlement_type = self.clean_text(cells[2].get_text())
                    population_text = self.clean_text(cells[3].get_text())
                    municipality_name = self.clean_text(cells[4].get_text())
                elif len(cells) == 4:
                    # Структура без номера
                    name = self.clean_text(cells[0].get_text())
                    settlement_type = self.clean_text(cells[1].get_text())
                    population_text = self.clean_text(cells[2].get_text())
                    municipality_name = self.clean_text(cells[3].get_text())
                else:
                    continue
                
                # Проверяем корректность названия
                if not self.is_valid_settlement_name(name):
                    logger.warning(f"⚠️ Пропущено некорректное название: '{name}'")
                    continue
                
                # Проверяем корректность муниципалитета
                if not municipality_name or municipality_name in ['—', '?', '']:
                    logger.warning(f"⚠️ Пропущено из-за отсутствия муниципалитета: '{name}'")
                    continue
                
                # Форматируем название муниципалитета
                district = f"Городской округ город {municipality_name}"
                
                # Преобразуем население
                population = self.parse_population(population_text)
                
                # Нормализуем тип населенного пункта
                normalized_type = self.normalize_settlement_type(settlement_type, name)
                
                # Формируем запись
                settlement = {
                    'name': name,
                    'type': normalized_type,
                    'population': population,
                    'district': district
                }
                
                settlements.append(settlement)
                logger.info(f"✅ Обработано: {normalized_type} {name}, население: {population}, муниципалитет: {district}")
                
            except Exception as e:
                logger.warning(f"⚠️ Ошибка обработки строки {i} в таблице городских округов: {e}")
                continue
        
        return settlements

    def parse_district_table_simple(self, table, table_index):
        """Простой парсинг таблицы района"""
        settlements = []
        rows = table.find_all('tr')
        
        if len(rows) < 2:
            return []
        
        # Определяем название района
        district_name = self.get_district_name(table, table_index)
        
        # ДОБАВЛЯЕМ СЛОВО "РАЙОН" ЕСЛИ ЕГО ЕЩЁ НЕТ
        if "район" not in district_name.lower() and "округ" not in district_name.lower():
            district_name = f"{district_name} район"
        
        logger.info(f"🏘️ Обработка района: {district_name}")
        
        # Пропускаем заголовок и анализируем данные
        for i, row in enumerate(rows[1:], 1):
            try:
                cells = row.find_all(['td', 'th'])
                
                # Пропускаем строки с недостаточным количеством ячеек
                if len(cells) < 4:
                    continue
                
                # Логируем содержимое для отладки
                cell_contents = [self.clean_text(cell.get_text()) for cell in cells]
                logger.info(f"📋 Строка {i} района {district_name}: {cell_contents}")
                
                # Простая логика: предполагаем стандартную структуру
                # 0: номер, 1: название, 2: тип, 3: население
                if len(cells) >= 4:
                    name = self.clean_text(cells[1].get_text())
                    settlement_type = self.clean_text(cells[2].get_text())
                    population_text = self.clean_text(cells[3].get_text())
                else:
                    continue
                
                # Проверяем корректность названия
                if not self.is_valid_settlement_name(name, district_name):
                    continue
                
                # Нормализуем тип населенного пункта
                normalized_type = self.normalize_settlement_type(settlement_type, name)
                
                # Преобразуем население
                population = self.parse_population(population_text)
                
                # Формируем запись
                settlement = {
                    'name': name,
                    'type': normalized_type,
                    'population': population,
                    'district': district_name
                }
                
                settlements.append(settlement)
                
            except Exception as e:
                logger.warning(f"⚠️ Ошибка обработки строки {i} в таблице района '{district_name}': {e}")
                continue
        
        return settlements

    def normalize_settlement_type(self, settlement_type, name):
        """Нормализация типа населенного пункта"""
        if not settlement_type or settlement_type in ['—', '?', '']:
            return self.normalize_settlement_type_by_name(name)
        
        # Нормализуем существующий тип
        type_lower = settlement_type.lower()
        
        type_mapping = {
            'город': 'город',
            'г.': 'город',
            'пгт': 'посёлок городского типа',
            'посёлок городского типа': 'посёлок городского типа',
            'поселок городского типа': 'посёлок городского типа',
            'посёлок': 'посёлок',
            'поселок': 'посёлок',
            'пос.': 'посёлок',
            'село': 'село',
            'с.': 'село',
            'деревня': 'деревня',
            'д.': 'деревня',
            'станция': 'станция',
            'ст.': 'станция',
            'хутор': 'хутор',
            'х.': 'хутор'
        }
        
        for key, value in type_mapping.items():
            if key in type_lower:
                return value
        
        return settlement_type

    def normalize_settlement_type_by_name(self, name):
        """Определение типа населенного пункта по его названию"""
        name_lower = name.lower()
        
        # Определяем по ключевым словам в названии
        if any(keyword in name_lower for keyword in ['город', 'г.']):
            return 'город'
        elif any(keyword in name_lower for keyword in ['посёлок городского типа', 'пгт']):
            return 'посёлок городского типа'
        elif any(keyword in name_lower for keyword in ['посёлок', 'пос.']):
            return 'посёлок'
        elif any(keyword in name_lower for keyword in ['село', 'с.']):
            return 'село'
        elif any(keyword in name_lower for keyword in ['деревня', 'д.']):
            return 'деревня'
        elif any(keyword in name_lower for keyword in ['станция', 'ст.']):
            return 'станция'
        elif any(keyword in name_lower for keyword in ['хутор', 'х.']):
            return 'хутор'
        else:
            return 'село'

    def is_valid_settlement_name(self, name, district_name=None):
        """Проверка, является ли название корректным названием населенного пункта"""
        if not name or name in ['—', '?', '']:
            return False
        
        # Проверяем на числа и специальные символы
        if name.replace('.', '').isdigit():
            return False
        
        # Список заголовков и некорректных названий, которые нужно исключить
        invalid_names = [
            'населённый пункт', 'населенный пункт', 'название', 
            'тип', 'статус', 'население', 'жителей', 'численность',
            'муниципальное образование', 'город областного значения',
            'административная единица', 'округ'
        ]
        
        if name.lower() in invalid_names:
            return False
        
        return True

    def get_district_name(self, table, table_index):
        """Получение названия района из таблицы"""
        try:
            # Ищем в заголовке таблицы
            caption = table.find('caption')
            if caption:
                caption_text = self.clean_district_name(caption.get_text())
                if caption_text:
                    return caption_text
            
            # Ищем в предыдущих заголовках
            prev_headers = table.find_previous_siblings(['h2', 'h3', 'h4'])
            for header in prev_headers[:2]:
                header_text = self.clean_district_name(header.get_text())
                if header_text:
                    return header_text
            
            # Если не нашли, используем общее название
            district_names = [
                "Бондарский", "Гавриловский", "Жердевский", 
                "Знаменский", "Инжавинский", "Кирсановский",
                "Мичуринский", "Мордовский", "Моршанский",
                "Мучкапский", "Никифоровский", "Первомайский",
                "Петровский", "Пичаевский", "Рассказовский",
                "Ржаксинский", "Сампурский", "Сосновский",
                "Староюрьевский", "Тамбовский", "Токарёвский",
                "Уваровский", "Умётский"
            ]
            
            if table_index - 1 < len(district_names):
                return district_names[table_index - 1]
            else:
                return f"Район {table_index}"
                
        except Exception as e:
            logger.error(f"❌ Ошибка определения названия района: {e}")
            return f"Район {table_index}"

    def clean_district_name(self, text):
        """Очистка названия района"""
        if not text:
            return ""
        
        # Удаляем [править | править код] и подобные
        text = re.sub(r'\[[^\]]*\]', '', text)
        
        # Удаляем лишние пробелы
        text = re.sub(r'\s+', ' ', text.strip())
        
        return text

    def clean_text(self, text):
        """Очистка текста"""
        if not text:
            return ""
        
        # Удаляем сноски в квадратных скобках
        text = re.sub(r'\[[^\]]*\]', '', text)
        
        # Удаляем лишние пробелы и символы
        text = re.sub(r'\s+', ' ', text.strip())
        
        return text

    def parse_population(self, population_text):
        """Парсинг численности населения"""
        if not population_text:
            return None
        
        try:
            # Удаляем все нецифровые символы, включая пробелы
            clean_text = re.sub(r'[^\d]', '', population_text)
            
            if clean_text and clean_text.isdigit():
                population = int(clean_text)
                # Проверяем разумность значения
                if 0 < population < 10000000:
                    return population
            
            return None
        except:
            return None

    def create_database(self):
        """Создание таблицы для населенных пунктов"""
        try:
            conn = mysql.connector.connect(**self.db_config)
            cursor = conn.cursor()
            
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS settlements (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    name VARCHAR(255) NOT NULL,
                    type VARCHAR(100) NOT NULL,
                    population INT,
                    district VARCHAR(255),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    INDEX idx_name (name),
                    INDEX idx_district (district)
                )
            """)
            
            conn.commit()
            cursor.close()
            conn.close()
            logger.info("✅ Таблица settlements создана успешно")
            
        except Exception as e:
            logger.error(f"❌ Ошибка создания таблицы settlements: {e}")
            raise

    def save_to_database(self, settlements):
        """Сохранение населенных пунктов в базу данных"""
        try:
            conn = mysql.connector.connect(**self.db_config)
            cursor = conn.cursor()
            
            # Очищаем таблицу перед добавлением новых данных
            cursor.execute("DELETE FROM settlements")
            
            insert_query = """
                INSERT INTO settlements (name, type, population, district)
                VALUES (%s, %s, %s, %s)
            """
            
            success_count = 0
            for settlement in settlements:
                try:
                    cursor.execute(insert_query, (
                        settlement['name'],
                        settlement['type'],
                        settlement['population'],
                        settlement['district']
                    ))
                    success_count += 1
                    logger.info(f"💾 Сохранено: {settlement['type']} {settlement['name']} -> {settlement['district']}")
                except Exception as e:
                    logger.warning(f"⚠️ Ошибка сохранения '{settlement['name']}': {e}")
                    continue
            
            conn.commit()
            cursor.close()
            conn.close()
            
            logger.info(f"💾 Успешно сохранено {success_count} населенных пунктов")
            return success_count
            
        except Exception as e:
            logger.error(f"❌ Ошибка сохранения в базу данных: {e}")
            return 0

    def run(self):
        """Запуск парсера"""
        logger.info("🚀 Запуск парсера населенных пунктов Тамбовской области...")
        self.create_database()
        
        settlements = self.parse_ruwiki_tables()
        
        if settlements:
            saved_count = self.save_to_database(settlements)
            logger.info(f"✅ Данные успешно загружены: {saved_count} записей")
            
            # Покажем примеры сохраненных данных
            logger.info("🔍 Примеры сохраненных записей:")
            for i, settlement in enumerate(settlements[:15]):
                pop_info = f", население: {settlement['population']}" if settlement['population'] else ""
                logger.info(f"   {i+1}. {settlement['type']} {settlement['name']}{pop_info}, {settlement['district']}")
            
            return True
        else:
            logger.error("❌ Не удалось получить данные с сайта")
            return False

# Функция для тестирования
def test_parser():
    """Тестирование парсера с подробным выводом"""
    import requests
    from bs4 import BeautifulSoup
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    
    url = "https://ru.ruwiki.ru/wiki/Населённые_пункты_Тамбовской_области"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    }
    
    session = requests.Session()
    session.verify = False
    
    response = session.get(url, headers=headers)
    response.encoding = 'utf-8'
    
    soup = BeautifulSoup(response.content, 'html.parser')
    
    # Ищем все таблицы
    tables = soup.find_all('table', {'class': 'standard'})
    print(f"📊 Найдено таблиц: {len(tables)}")
    
    # Тестируем первую таблицу (городские округа)
    if tables:
        print("\n🔍 ТЕСТ ПЕРВОЙ ТАБЛИЦЫ (городские округа):")
        table = tables[0]
        rows = table.find_all('tr')
        
        print(f"📏 Найдено строк: {len(rows)}")
        
        if rows:
            print("📝 ЗАГОЛОВКИ ТАБЛИЦЫ:")
            header_cells = rows[0].find_all(['th', 'td'])
            for j, cell in enumerate(header_cells):
                print(f"   Колонка {j}: '{cell.get_text().strip()}'")
            
            print("\n📋 ПЕРВЫЕ 5 СТРОК ДАННЫХ:")
            for k, row in enumerate(rows[1:6], 1):
                cells = row.find_all(['td', 'th'])
                cell_texts = [f"'{cell.get_text().strip()}'" for cell in cells]
                print(f"   Строка {k} ({len(cells)} ячеек): {cell_texts}")

if __name__ == "__main__":
    test_parser()