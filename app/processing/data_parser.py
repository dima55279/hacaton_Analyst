import requests
from bs4 import BeautifulSoup
import mysql.connector
import logging
import re

logger = logging.getLogger(__name__)

class SettlementParser:
    def __init__(self, db_config):
        self.db_config = db_config
        self.target_url = "https://geoadm.com/naselennye-punkty-tambovskoy-oblasti.html"
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        }
    
    def create_database(self):
        """Создание таблицы для населенных пунктов с указанной структурой"""
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
            logger.info("✅ Таблица settlements создана успешно с колонками: id, name, type, population, district")
            
        except Exception as e:
            logger.error(f"❌ Ошибка создания таблицы settlements: {e}")
            raise
    
    def parse_geoadm_table(self):
        """Парсинг конкретной таблицы с населенными пунктами"""
        try:
            logger.info(f"🔍 Загрузка страницы: {self.target_url}")
            response = requests.get(self.target_url, headers=self.headers, timeout=30)
            response.encoding = 'utf-8'
            
            if response.status_code != 200:
                logger.error(f"❌ Ошибка загрузки страницы: {response.status_code}")
                return []
            
            soup = BeautifulSoup(response.content, 'html.parser')
            settlements = []
            
            # Ищем заголовок "Список всех населенных пунктов Тамбовской области"
            target_header = None
            headers = soup.find_all(['h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'strong', 'b'])
            
            for header in headers:
                header_text = header.get_text().strip()
                if "Список всех населенных пунктов Тамбовской области" in header_text:
                    target_header = header
                    logger.info("✅ Найден целевой заголовок таблицы")
                    break
            
            if not target_header:
                logger.error("❌ Не найден заголовок 'Список всех населенных пунктов Тамбовской области'")
                # Попробуем найти таблицу по содержимому
                return self.find_table_by_content(soup)
            
            # Ищем таблицу после заголовка
            table = self.find_table_after_header(target_header)
            
            if not table:
                logger.error("❌ Не найдена таблица после заголовка")
                return []
            
            logger.info("✅ Таблица найдена, начинаем парсинг...")
            settlements = self.parse_settlements_table(table)
            
            logger.info(f"🎯 Всего найдено населенных пунктов: {len(settlements)}")
            return settlements
            
        except Exception as e:
            logger.error(f"❌ Ошибка парсинга таблицы: {e}")
            return []
    
    def find_table_after_header(self, header):
        """Поиск таблицы после заданного заголовка"""
        # Проверяем следующие элементы после заголовка
        current_element = header
        for _ in range(20):  # Ограничим поиск 20 следующими элементами
            current_element = current_element.find_next_sibling()
            if not current_element:
                break
            
            if current_element.name == 'table':
                logger.info("✅ Найдена таблица методом find_next_sibling")
                return current_element
            
            # Ищем таблицу внутри дивов или других контейнеров
            if current_element.find('table'):
                logger.info("✅ Найдена таблица внутри контейнера")
                return current_element.find('table')
        
        # Альтернативный метод: ищем все таблицы после заголовка
        all_tables = header.find_all_next('table')
        for table in all_tables[:5]:  # Проверим первые 5 таблиц
            table_text = table.get_text()
            if any(keyword in table_text for keyword in ['Тамбов', 'Мичуринск', 'село', 'деревня', 'город']):
                logger.info("✅ Найдена таблица по содержанию")
                return table
        
        return None
    
    def find_table_by_content(self, soup):
        """Поиск таблицы по содержимому"""
        logger.info("🔄 Используем поиск таблицы по содержимому")
        
        # Ищем все таблицы на странице
        tables = soup.find_all('table')
        logger.info(f"📊 Найдено таблиц на странице: {len(tables)}")
        
        for i, table in enumerate(tables):
            logger.info(f"🔍 Анализ таблицы {i+1}")
            
            # Проверяем, есть ли в таблице данные о населенных пунктах Тамбовской области
            table_text = table.get_text()
            
            # Ключевые слова, которые должны быть в таблице с населенными пунктами Тамбовской области
            tambov_keywords = ['Тамбов', 'Мичуринск', 'Моршанск', 'Котовск', 'Рассказово', 'Уварово']
            keyword_matches = sum(1 for keyword in tambov_keywords if keyword in table_text)
            
            if keyword_matches >= 2:  # Если есть хотя бы 2 ключевых слова Тамбовской области
                logger.info(f"✅ Таблица {i+1} похожа на таблицу населенных пунктов Тамбовской области")
                settlements = self.parse_settlements_table(table)
                if settlements:
                    return settlements
        
        return []
    
    def parse_settlements_table(self, table):
        """Парсинг таблицы с населенными пунктами"""
        settlements = []
        rows = table.find_all('tr')
        
        logger.info(f"📋 Найдено строк в таблице: {len(rows)}")
        
        if not rows or len(rows) < 2:
            logger.error("❌ В таблице недостаточно строк для парсинга")
            return []
        
        # Определяем структуру таблицы по заголовкам
        header_row = rows[0]
        headers = [self.clean_whitespace(th.get_text()) for th in header_row.find_all(['th', 'td'])]
        logger.info(f"📝 Заголовки таблицы: {headers}")
        
        # Определяем индексы колонок по ожидаемым названиям
        name_idx, type_idx, population_idx, district_idx = self.find_column_indexes(headers)
        
        logger.info(f"🔍 Определены индексы колонок: название={name_idx}, тип={type_idx}, население={population_idx}, район={district_idx}")
        
        # Если не удалось определить колонки по заголовкам, используем предположения по умолчанию
        if name_idx == -1:
            name_idx = 0
        if type_idx == -1:
            type_idx = 1 if len(headers) > 1 else 0
        if population_idx == -1:
            # Ищем колонку с числовыми данными
            for i in range(len(headers)):
                if any(char.isdigit() for char in headers[i]):
                    population_idx = i
                    break
            if population_idx == -1 and len(headers) > 2:
                population_idx = 2
        if district_idx == -1:
            # Предполагаем, что район в последней колонке
            district_idx = len(headers) - 1
        
        # Обрабатываем строки с данными (пропускаем заголовок)
        for i, row in enumerate(rows[1:], 1):
            try:
                cols = row.find_all(['td', 'th'])
                if len(cols) < max(name_idx, type_idx, population_idx, district_idx) + 1:
                    logger.warning(f"⚠️ Строка {i} имеет недостаточно колонок: {len(cols)}")
                    continue
                
                # Извлекаем данные из колонок БЕЗ ИЗМЕНЕНИЙ, кроме удаления лишних пробелов
                name = self.clean_whitespace(self.extract_text_from_col(cols, name_idx))
                settlement_type = self.clean_whitespace(self.extract_text_from_col(cols, type_idx))
                population_text = self.clean_whitespace(self.extract_text_from_col(cols, population_idx))
                district = self.clean_whitespace(self.extract_text_from_col(cols, district_idx))
                
                # Преобразуем население в число, если возможно
                population = self.parse_population(population_text)
                
                # Проверяем, что у нас есть минимально необходимые данные
                if name and settlement_type:
                    settlement = {
                        'name': name,
                        'type': settlement_type,
                        'population': population,
                        'district': district
                    }
                    settlements.append(settlement)
                    
                    # Логируем первые 5 записей для примера
                    if i <= 5:
                        logger.info(f"📝 Пример данных: {settlement}")
                
            except Exception as e:
                logger.warning(f"⚠️ Ошибка обработки строки {i}: {e}")
                continue
        
        return settlements
    
    def find_column_indexes(self, headers):
        """Определение индексов колонок по заголовкам"""
        name_idx, type_idx, population_idx, district_idx = -1, -1, -1, -1
        
        for i, header in enumerate(headers):
            header_lower = header.lower()
            
            # Ищем колонку с названием
            if any(pattern in header_lower for pattern in ['название', 'населенный пункт', 'имя', 'город']):
                name_idx = i
            
            # Ищем колонку с типом
            elif any(pattern in header_lower for pattern in ['тип', 'вид']):
                type_idx = i
            
            # Ищем колонку с населением
            elif any(pattern in header_lower for pattern in ['население', 'жители', 'численность', 'населени']):
                population_idx = i
            
            # Ищем колонку с районом
            elif any(pattern in header_lower for pattern in ['район', 'округ', 'муниципальный']):
                district_idx = i
        
        return name_idx, type_idx, population_idx, district_idx
    
    def extract_text_from_col(self, cols, index):
        """Извлечение текста из колонки по индексу"""
        if index == -1 or index >= len(cols):
            return ""
        
        return cols[index].get_text()
    
    def clean_whitespace(self, text):
        """Очистка только от лишних пробелов (без изменения содержимого)"""
        if not text:
            return ""
        
        # Удаляем лишние пробелы, табы, переносы строк
        text = re.sub(r'\s+', ' ', text.strip())
        
        return text
    
    def parse_population(self, population_text):
        """Парсинг численности населения в число"""
        if not population_text:
            return None
        
        try:
            # Удаляем все нецифровые символы, кроме пробелов (для чисел с разделителями)
            clean_text = re.sub(r'[^\d\s]', '', population_text)
            clean_text = clean_text.replace(' ', '')
            
            if clean_text and clean_text.isdigit():
                population = int(clean_text)
                # Проверяем разумность значения
                if population > 0:
                    return population
            
            return None
        except:
            return None
    
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
                except Exception as e:
                    logger.warning(f"⚠️ Ошибка сохранения {settlement['name']}: {e}")
                    continue
            
            conn.commit()
            cursor.close()
            conn.close()
            
            logger.info(f"💾 Успешно сохранено {success_count} населенных пунктов")
            
            # Покажем статистику по сохраненным данным
            self.show_save_statistics(settlements)
            
            return success_count
            
        except Exception as e:
            logger.error(f"❌ Ошибка сохранения в базу данных: {e}")
            return 0
    
    def show_save_statistics(self, settlements):
        """Показать статистику по сохраненным данным"""
        if not settlements:
            return
        
        total = len(settlements)
        with_population = sum(1 for s in settlements if s['population'] is not None)
        with_district = sum(1 for s in settlements if s['district'])
        
        logger.info(f"📊 Статистика сохраненных данных:")
        logger.info(f"   Всего записей: {total}")
        logger.info(f"   С указанием населения: {with_population} ({with_population/total*100:.1f}%)")
        logger.info(f"   С указанием района: {with_district} ({with_district/total*100:.1f}%)")
        
        # Покажем несколько примеров сохраненных данных
        logger.info("🔍 Примеры сохраненных записей:")
        for i, settlement in enumerate(settlements[:5]):
            pop_info = f", население: {settlement['population']}" if settlement['population'] else ""
            district_info = f", район: {settlement['district']}" if settlement['district'] else ""
            logger.info(f"   {i+1}. {settlement['type']} {settlement['name']}{pop_info}{district_info}")
    
    def run(self):
        """Запуск парсера"""
        logger.info("🚀 Запуск парсера таблицы населенных пунктов Тамбовской области...")
        self.create_database()
        
        settlements = self.parse_geoadm_table()
        
        if settlements:
            saved_count = self.save_to_database(settlements)
            logger.info(f"✅ Данные успешно загружены: {saved_count} записей")
            return True
        else:
            logger.error("❌ Не удалось получить данные из таблицы")
            return False

# Тестовый скрипт для проверки структуры таблицы
def inspect_table_structure():
    """Функция для анализа структуры таблицы на сайте"""
    import requests
    from bs4 import BeautifulSoup
    
    url = "https://geoadm.com/naselennye-punkty-tambovskoy-oblasti.html"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    }
    
    response = requests.get(url, headers=headers)
    response.encoding = 'utf-8'
    
    soup = BeautifulSoup(response.content, 'html.parser')
    
    # Сохраняем HTML для анализа
    with open('geoadm_detailed.html', 'w', encoding='utf-8') as f:
        f.write(soup.prettify())
    
    print("✅ HTML страницы сохранен в geoadm_detailed.html")
    
    # Ищем заголовок
    headers = soup.find_all(['h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'strong', 'b'])
    print("🔍 Найденные заголовки:")
    for header in headers:
        text = header.get_text().strip()
        if "Список всех населенных пунктов Тамбовской области" in text:
            print(f"✅ ЦЕЛЕВОЙ ЗАГОЛОВОК: {text}")
            # Найдем следующую таблицу
            table = header.find_next('table')
            if table:
                print("✅ Найдена таблица после заголовка")
                # Покажем структуру таблицы
                rows = table.find_all('tr')
                print(f"📊 Количество строк в таблице: {len(rows)}")
                
                if rows:
                    print("\n📝 ЗАГОЛОВКИ ТАБЛИЦЫ:")
                    header_cells = rows[0].find_all(['th', 'td'])
                    for i, cell in enumerate(header_cells):
                        print(f"  Колонка {i}: '{cell.get_text().strip()}'")
                    
                    print("\n📋 ПЕРВЫЕ 5 СТРОК ДАННЫХ:")
                    for i, row in enumerate(rows[1:6], 1):
                        cells = row.find_all(['td', 'th'])
                        cell_texts = [f"'{cell.get_text().strip()}'" for cell in cells]
                        print(f"  Строка {i}: {cell_texts}")
            else:
                print("❌ Таблица не найдена после заголовка")
        elif text:
            print(f"  - {text}")

if __name__ == "__main__":
    inspect_table_structure()