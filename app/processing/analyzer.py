import logging
from datetime import datetime, timedelta
import json
import re
import os

logger = logging.getLogger(__name__)

class AppealsAnalyzer:
    def __init__(self, gigachat_client, database):
        self.gigachat = gigachat_client
        self.db = database
        self.common_types = [
            "жалоба на ЖКХ",
            "предложение по благоустройству", 
            "запрос информации",
            "жалоба на дороги",
            "предложение по транспорту",
            "запрос документов",
            "жалоба на шум",
            "предложение по культуре"
        ]
        self.settlements_data = self._load_settlements_data()

    def _find_municipality_by_settlement(self, settlement_name, district_name=None):
        """Поиск муниципального образования по названию населенного пункта и району"""
        if not settlement_name or not self.settlements_data:
            return None
        
        settlement_lower = settlement_name.lower().strip()
        
        logger.info(f"🔍 Поиск муниципалитета для: {settlement_name}, район: {district_name}")
        
        # Сначала ищем по точному совпадению названия населенного пункта в муниципалитетах
        for municipality in self.settlements_data:
            mun_name_lower = municipality['name'].lower()
            
            # Проверяем различные варианты названий
            if (settlement_lower in mun_name_lower or 
                mun_name_lower in settlement_lower or
                any(word in mun_name_lower for word in settlement_lower.split())):
                
                logger.info(f"✅ Найден муниципалитет по названию: {municipality['name']}")
                return municipality
        
        # Если не нашли по названию, ищем по ключевым словам
        for municipality in self.settlements_data:
            mun_name_lower = municipality['name'].lower()
            
            # Для городских округов
            if 'городской округ' in mun_name_lower:
                if 'тамбов' in settlement_lower:
                    logger.info(f"✅ Найден городской округ для Тамбова: {municipality['name']}")
                    return municipality
                elif any(keyword in settlement_lower for keyword in ['кирсанов', 'котовск', 'мичуринск', 'моршанск', 'рассказово', 'уварово']):
                    city_name = None
                    if 'кирсанов' in settlement_lower:
                        city_name = 'Кирсанов'
                    elif 'котовск' in settlement_lower:
                        city_name = 'Котовск'
                    elif 'мичуринск' in settlement_lower:
                        city_name = 'Мичуринск'
                    elif 'моршанск' in settlement_lower:
                        city_name = 'Моршанск'
                    elif 'рассказово' in settlement_lower:
                        city_name = 'Рассказово'
                    elif 'уварово' in settlement_lower:
                        city_name = 'Уварово'
                    
                    if city_name and city_name.lower() in mun_name_lower:
                        logger.info(f"✅ Найден городской округ для {city_name}: {municipality['name']}")
                        return municipality
        
        # Муниципалитет Тамбовского района по умолчанию
        default_municipality = self._find_tambov_default()
        if default_municipality:
            logger.info(f"🔄 Использован муниципалитет по умолчанию: {default_municipality['name']}")
            return default_municipality
        
        logger.warning(f"❌ Муниципалитет для {settlement_name} не найден")
        return None

    def _find_tambov_default(self):
        """Находит муниципалитет Тамбовского района по умолчанию"""
        for municipality in self.settlements_data:
            if 'тамбовский район' in municipality['name'].lower():
                return municipality
        return None

    def _load_settlements_data(self):
        """Загрузка данных о муниципальных образованиях"""
        try:
            possible_paths = [
                'settlements.data.json',
                '../settlements.data.json',
                './data/settlements.data.json'
            ]
            
            for file_path in possible_paths:
                if os.path.exists(file_path):
                    with open(file_path, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                        logger.info(f"✅ Загружены данные по {len(data)} муниципальным образованиям из {file_path}")
                        return data
            
            logger.error("❌ Файл settlements.data.json не найден")
            return []
            
        except Exception as e:
            logger.error(f"❌ Ошибка загрузки данных муниципальных образований: {e}")
            return []

    def _find_municipality_by_settlement(self, settlement_name, district_name=None):
        """Поиск муниципального образования по названию населенного пункта и району"""
        if not settlement_name or not self.settlements_data:
            return None
        
        settlement_lower = settlement_name.lower().strip()
        district_lower = district_name.lower().strip() if district_name else None
        
        logger.info(f"🔍 Поиск муниципалитета для: {settlement_name}, район: {district_name}")
        
        # Сначала ищем точное совпадение по названию района
        if district_lower:
            for municipality in self.settlements_data:
                if district_lower in municipality['name'].lower():
                    logger.info(f"✅ Найден муниципалитет по району: {municipality['name']}")
                    return municipality
        
        # Затем ищем по другим критериям
        for municipality in self.settlements_data:
            mun_name_lower = municipality['name'].lower()
            
            if district_lower and district_lower in mun_name_lower:
                logger.info(f"✅ Найден муниципалитет по ключевому слову района: {municipality['name']}")
                return municipality
            
            # Для городских округов
            if 'городской округ' in mun_name_lower and 'тамбов' in settlement_lower:
                logger.info(f"✅ Найден городской округ для Тамбова: {municipality['name']}")
                return municipality
        
        # Муниципалитет Тамбовского района по умолчанию
        default_municipality = self._find_tambov_default()
        if default_municipality:
            logger.info(f"🔄 Использован муниципалитет по умолчанию: {default_municipality['name']}")
            return default_municipality
        
        logger.warning("❌ Муниципалитет не найден")
        return None

    def _find_tambov_default(self):
        """Находит муниципалитет Тамбовского района по умолчанию"""
        for municipality in self.settlements_data:
            if 'тамбовский район' in municipality['name'].lower():
                return municipality
        return None

    def _generate_municipality_contacts(self, municipality):
        """Генерация текста с контактами муниципального образования"""
        if not municipality:
            return ""
        
        contacts = f"""

📞 Контакты соответствующего муниципального образования:
• Название: {municipality['name']}
• Телефон: {municipality['telephone']}
• Email: {municipality['email']}
• Адрес: {municipality['address']}

Рекомендуем также обратиться напрямую для уточнения деталей."""
        
        return contacts

    def _replace_all_contact_placeholders(self, text, phone):
        """УНИВЕРСАЛЬНАЯ ЗАМЕНА ВСЕХ ВОЗМОЖНЫХ ПЛЕЙСХОЛДЕРОВ ТЕЛЕФОНА"""
        if not text or not phone:
            return text
        
        # Расширенный список шаблонов для замены (включая опечатки)
        patterns = [
            # Стандартные плейсхолдеры
            r'\[указать телефон\]',
            r'\[указать номер телефона\]',
            r'\[контактный телефон\]',
            r'\[номер телефона\]',
            r'\[телефон\]',
            r'XXX-XX-XX',
            
            # Опечатки и варианты
            r'\[диазать номер телефона\]',
            r'\[диазать телефон\]',
            r'\[укажите телефон\]',
            r'\[укажите номер телефона\]',
            r'\[введите телефон\]',
            r'\[введите номер телефона\]',
            
            # Без квадратных скобок
            r'указать номер телефона',
            r'указать телефон',
            r'контактный телефон',
            r'номер телефона',
            r'диазать номер телефона',
        ]
        
        # Заменяем все найденные плейсхолдеры
        for pattern in patterns:
            text = re.sub(pattern, f'по телефону {phone}', text, flags=re.IGNORECASE)
        
        # Дополнительная обработка: заменяем фразы в конце предложений
        text = re.sub(r'по телефону\s*\.', f'по телефону {phone}.', text)
        text = re.sub(r'по телефону\s*$', f'по телефону {phone}', text)
        
        return text

    def _ensure_phone_in_text(self, text, phone):
        """ГАРАНТИРОВАННОЕ ДОБАВЛЕНИЕ ТЕЛЕФОНА В ТЕКСТ"""
        if not text or not phone:
            return text
        
        # Если в тексте нет упоминания телефона, добавляем его
        if phone not in text:
            # Ищем подходящее место для вставки телефона
            sentences = text.split('.')
            if len(sentences) > 1:
                # Вставляем перед последним предложением
                last_sentence = sentences[-2] if sentences[-1].strip() == '' else sentences[-1]
                if 'телефон' not in last_sentence.lower() and 'звонить' not in last_sentence.lower():
                    text = text.rstrip()
                    if not text.endswith('.'):
                        text += '.'
                    text += f' По всем вопросам обращайтесь по телефону {phone}.'
            else:
                text += f' По вопросам уточнения обращайтесь по телефону {phone}.'
        
        return text

    def classify_appeal(self, appeal_text):
        """Классификация типа обращения с помощью GigaChat"""
        try:
            prompt = f"""
            Классифицируй обращение гражданина по следующим категориям: 
            {', '.join(self.common_types)}
            
            Если обращение не подходит под эти категории, верни "другое".
            
            Обращение: "{appeal_text}"
            
            Верни ТОЛЬКО название категории без дополнительных объяснений.
            """
            
            response = self.gigachat.chat_completion([
                {"role": "system", "content": "Ты классификатор обращений граждан"},
                {"role": "user", "content": prompt}
            ])
            
            appeal_type = response.strip().lower()
            if appeal_type not in [t.lower() for t in self.common_types]:
                appeal_type = "другое"
                
            logger.info(f"🎯 Классифицировано как: {appeal_type}")
            return appeal_type
            
        except Exception as e:
            logger.error(f"❌ Ошибка классификации: {e}")
            return "другое"

    def generate_response(self, appeal_id, appeal_text, appeal_type, address_info=None):
        """Генерация ответа на обращение с ГАРАНТИРОВАННОЙ подстановкой телефона"""
        try:
            # Получаем контакты муниципального образования
            municipality = None
            if address_info:
                settlement = address_info.get('settlement', '')
                district = address_info.get('district', '')
                municipality = self._find_municipality_by_settlement(settlement, district)
                if municipality:
                    logger.info(f"📍 Найдены контакты муниципалитета для {settlement}")
                else:
                    logger.warning(f"📍 Муниципалитет для {settlement} не найден")

            # Упрощенный промпт - генерируем только основную часть ответа
            prompt = f"""
            Сгенерируй официальный ответ на обращение гражданина. Текст обращения: "{appeal_text}"
            
            Требования:
            - Официально-деловой стиль
            - Вежливый тон  
            - Конкретные сроки решения (если применимо)
            - Не более 250 символов
            - НЕ упоминай телефонные номера, контакты или способы связи
            
            Ответ:
            """
            
            response = self.gigachat.chat_completion([
                {"role": "system", "content": "Ты помощник для генерации ответов гражданам. Генерируй только основную часть ответа без контактов."},
                {"role": "user", "content": prompt}
            ])
            
            # ОСНОВНАЯ ЛОГИКА: ГАРАНТИРОВАННАЯ ПОДСТАНОВКА ТЕЛЕФОНА
            final_response = response.strip()
            
            if municipality:
                phone = municipality['telephone']
                
                # 1. Заменяем ВСЕ возможные плейсхолдеры
                final_response = self._replace_all_contact_placeholders(final_response, phone)
                
                # 2. Гарантированно добавляем телефон, если его еще нет
                final_response = self._ensure_phone_in_text(final_response, phone)
                
                # 3. Добавляем блок контактов
                contacts_block = self._generate_municipality_contacts(municipality)
                final_response += contacts_block
                
                logger.info(f"✅ Телефон {phone} гарантированно добавлен в ответ")
            else:
                final_response += "\n\nПо вопросам уточнения обращайтесь в соответствующий муниципальный орган вашего района."
            
            logger.info(f"📝 Сгенерирован ответ для обращения {appeal_id}")
            return final_response
            
        except Exception as e:
            logger.error(f"❌ Ошибка генерации ответа: {e}")
            base_response = "Благодарим за обращение! Ваше сообщение принято к рассмотрению."
            
            if address_info and municipality:
                base_response += f" По вопросам уточнения обращайтесь по телефону {municipality['telephone']}."
                base_response += self._generate_municipality_contacts(municipality)
            
            return base_response

    # Остальные методы остаются без изменений...
    def analyze_trends(self, period_days=30):
        """Анализ трендов и повторяющихся проблем с актуальными данными"""
        try:
            appeals = self.db.get_appeals({
                'date_from': datetime.now() - timedelta(days=period_days)
            }, limit=1000)
            
            logger.info(f"📈 Анализ трендов: получено {len(appeals)} обращений за {period_days} дней")
            
            if not appeals:
                return {
                    'period_days': period_days,
                    'total_appeals': 0,
                    'type_distribution': {},
                    'common_themes': [],
                    'response_rate': 0,
                    'last_updated': datetime.now().isoformat()
                }
            
            themes = self._extract_themes([a['text'] for a in appeals])
            
            type_stats = {}
            for appeal in appeals:
                appeal_type = appeal['type'] or 'другое'
                type_stats[appeal_type] = type_stats.get(appeal_type, 0) + 1
            
            if not isinstance(themes, list):
                themes = []
            
            trends = {
                'period_days': period_days,
                'total_appeals': len(appeals),
                'type_distribution': type_stats,
                'common_themes': themes[:10],
                'response_rate': self._calculate_response_rate(appeals),
                'last_updated': datetime.now().isoformat()
            }
            
            logger.info(f"📊 Проанализированы актуальные тренды за {period_days} дней")
            return trends
            
        except Exception as e:
            logger.error(f"❌ Ошибка анализа трендов: {e}")
            return {}

    def _extract_themes(self, texts):
        """Извлечение частых тем из текстов обращений"""
        try:
            if not texts:
                return []
                
            combined_text = " ".join(texts[:20])
            
            prompt = f"""
            Проанализируй тексты обращений граждан и выдели 10 самых частых тем/проблем.
            Верни в формате JSON: [{{"theme": "название темы", "frequency": "высокая/средняя/низкая"}}]
            
            Тексты: {combined_text[:3000]}
            
            Важно: верни только валидный JSON без дополнительного текста.
            """
            
            response = self.gigachat.chat_completion([
                {"role": "system", "content": "Ты аналитик, выделяющий основные темы из обращений. Ты возвращаешь только валидный JSON."},
                {"role": "user", "content": prompt}
            ])
            
            response = response.strip()
            json_match = re.search(r'\[.*\]', response, re.DOTALL)
            if json_match:
                response = json_match.group(0)
            
            try:
                themes = json.loads(response)
                if isinstance(themes, list):
                    logger.info(f"✅ Успешно извлечены темы: {len(themes)}")
                    return themes
                else:
                    return self._extract_themes_fallback(texts)
            except json.JSONDecodeError:
                return self._extract_themes_fallback(texts)
            
        except Exception as e:
            return self._extract_themes_fallback(texts)

    def _extract_themes_fallback(self, texts):
        """Резервный метод извлечения тем по ключевым словам"""
        if not texts:
            return []
            
        all_text = " ".join(texts).lower()
        
        keywords = {
            'дороги': ['дорог', 'асфальт', 'яма', 'ремонт дорог', 'выбоин'],
            'ЖКХ': ['жкх', 'управляющая', 'отоплен', 'водоснабжен', 'мусор', 'канализац', 'коммунал'],
            'транспорт': ['автобус', 'остановк', 'маршрут', 'транспорт', 'обществен', 'транспорт'],
            'благоустройство': ['парк', 'сквер', 'детская площадка', 'озеленен', 'лавочк', 'скамейк'],
            'шум': ['шум', 'громко', 'тишина', 'шумн'],
            'документы': ['справк', 'документ', 'получить', 'оформлен'],
            'освещение': ['освещен', 'фонар', 'свет', 'темно', 'улиц'],
            'уборка': ['уборк', 'мусор', 'чистота', 'убрать', 'захламлен'],
            'вода': ['вод', 'напор', 'качеств', 'питьев'],
            'отопление': ['отоплен', 'батаре', 'тепл', 'холодн']
        }
        
        themes = []
        for theme, words in keywords.items():
            count = sum(1 for word in words if word in all_text)
            if count > 0:
                total_words = len(all_text.split())
                frequency_percentage = (count / total_words) * 1000
                
                if frequency_percentage > 5:
                    frequency = "высокая"
                elif frequency_percentage > 2:
                    frequency = "средняя"
                else:
                    frequency = "низкая"
                
                themes.append({
                    "theme": theme, 
                    "frequency": frequency,
                    "count": count
                })
        
        themes.sort(key=lambda x: x.get('count', 0), reverse=True)
        return themes[:10]

    def _calculate_response_rate(self, appeals):
        """Расчет процента отвеченных обращений"""
        if not appeals:
            return 0
            
        answered = sum(1 for a in appeals if a.get('status') == 'answered')
        total = len(appeals)
        return round((answered / total * 100), 2) if total > 0 else 0

    def get_common_types(self):
        """Получение списка типовых обращений"""
        return self.common_types