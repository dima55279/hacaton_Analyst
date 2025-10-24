import logging
from datetime import datetime, timedelta
import json
import re

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
            
            # Очистка ответа
            appeal_type = response.strip().lower()
            if appeal_type not in [t.lower() for t in self.common_types]:
                appeal_type = "другое"
                
            logger.info(f"🎯 Классифицировано как: {appeal_type}")
            return appeal_type
            
        except Exception as e:
            logger.error(f"❌ Ошибка классификации: {e}")
            return "другое"

    def generate_response(self, appeal_id, appeal_text, appeal_type):
        """Генерация ответа на обращение"""
        try:
            # Поиск похожих обращений для контекста
            similar_appeals = self.db.get_appeals({
                'type': appeal_type,
                'status': 'answered'
            }, limit=3)
            
            context = ""
            if similar_appeals:
                context = "Примеры похожих обращений и ответов:\n"
                for appeal in similar_appeals:
                    context += f"Обращение: {appeal['text'][:200]}...\n"
                    context += f"Ответ: {appeal['response'][:200]}...\n\n"
            
            prompt = f"""
            Сгенерируй официальный ответ на обращение гражданина.
            
            Тип обращения: {appeal_type}
            Текст обращения: "{appeal_text}"
            
            {context}
            
            Требования к ответу:
            - Официально-деловой стиль
            - Вежливый тон
            - Конкретные сроки решения проблемы (если применимо)
            - Контакты для уточнения
            - Не более 500 символов
            
            Ответ:
            """
            
            response = self.gigachat.chat_completion([
                {"role": "system", "content": "Ты помощник для генерации ответов гражданам"},
                {"role": "user", "content": prompt}
            ])
            
            logger.info(f"📝 Сгенерирован ответ для обращения {appeal_id}")
            return response.strip()
            
        except Exception as e:
            logger.error(f"❌ Ошибка генерации ответа: {e}")
            return "Благодарим за обращение! Ваше сообщение принято к рассмотрению."

    def analyze_trends(self, period_days=30):
        """Анализ трендов и повторяющихся проблем с актуальными данными"""
        try:
            # Получаем актуальные данные из базы
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
            
            # Анализ частых тем
            themes = self._extract_themes([a['text'] for a in appeals])
            
            # Статистика по типам
            type_stats = {}
            for appeal in appeals:
                appeal_type = appeal['type'] or 'другое'
                type_stats[appeal_type] = type_stats.get(appeal_type, 0) + 1
            
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
                
            combined_text = " ".join(texts[:20])  # Ограничиваем для производительности
            
            prompt = f"""
            Проанализируй тексты обращений граждан и выдели 10 самых частых тем/проблем.
            Верни в формате JSON: [{{"theme": "название темы", "frequency": "высокая/средняя/низкая"}}]
            
            Тексты: {combined_text[:3000]}
            """
            
            response = self.gigachat.chat_completion([
                {"role": "system", "content": "Ты аналитик, выделяющий основные темы из обращений"},
                {"role": "user", "content": prompt}
            ])
            
            # Парсинг JSON ответа
            try:
                themes = json.loads(response)
                return themes
            except:
                logger.warning("❌ Не удалось распарсить JSON ответ от GigaChat, используем fallback")
                return self._extract_themes_fallback(texts)
            
        except Exception as e:
            logger.error(f"❌ Ошибка извлечения тем: {e}")
            # Резервный метод по ключевым словам
            return self._extract_themes_fallback(texts)

    def _extract_themes_fallback(self, texts):
        """Резервный метод извлечения тем по ключевым словам"""
        if not texts:
            return []
            
        keywords = {
            'дороги': ['дорог', 'асфальт', 'яма', 'ремонт дорог'],
            'ЖКХ': ['жкх', 'управляющая', 'отоплен', 'водоснабжен', 'мусор'],
            'транспорт': ['автобус', 'остановк', 'маршрут', 'транспорт'],
            'благоустройство': ['парк', 'сквер', 'детская площадка', 'озеленен'],
            'шум': ['шум', 'громко', 'тишина'],
            'документы': ['справк', 'документ', 'получить']
        }
        
        themes = []
        for theme, words in keywords.items():
            count = sum(1 for text in texts if any(word in text.lower() for word in words))
            if count > 0:
                frequency = "высокая" if count > 10 else "средняя" if count > 5 else "низкая"
                themes.append({"theme": theme, "frequency": frequency, "count": count})
        
        return sorted(themes, key=lambda x: x.get('count', 0), reverse=True)

    def _calculate_response_rate(self, appeals):
        """Расчет процента отвеченных обращений"""
        if not appeals:
            return 0
            
        answered = sum(1 for a in appeals if a['status'] == 'answered')
        total = len(appeals)
        return round((answered / total * 100), 2) if total > 0 else 0

    def get_common_types(self):
        """Получение списка типовых обращений"""
        return self.common_types