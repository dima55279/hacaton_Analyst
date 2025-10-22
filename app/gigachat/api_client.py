import requests
import json
import logging
from datetime import datetime, timedelta
import uuid
import urllib3
import time
from typing import List, Optional

# Отключаем предупреждения SSL
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logger = logging.getLogger(__name__)

class GigaChatClient:
    def __init__(self, api_key):
        if not api_key:
            raise ValueError("API ключ не может быть пустым")
        
        self.api_key = api_key
        self.auth_url = "https://ngw.devices.sberbank.ru:9443/api/v2/oauth"
        self.api_base_url = "https://gigachat.devices.sberbank.ru/api/v1/"
        self.access_token = None
        self.token_expires = None
        
        logger.info("✅ GigaChatClient инициализирован")

    def _authenticate(self, max_retries=3) -> bool:
        """Аутентификация в GigaChat API с повторными попытками"""
        if self.access_token and self.token_expires and datetime.now() < self.token_expires:
            logger.debug("✅ Используется существующий токен")
            return True

        for attempt in range(max_retries):
            try:
                # Генерируем уникальный RqUID
                rq_uid = str(uuid.uuid4())
                
                headers = {
                    'Content-Type': 'application/x-www-form-urlencoded',
                    'Accept': 'application/json',
                    'RqUID': rq_uid,
                    'Authorization': f'Basic {self.api_key}'
                }

                payload = {
                    'scope': 'GIGACHAT_API_PERS'
                }

                logger.info(f"🔐 Попытка аутентификации {attempt + 1}/{max_retries}...")
                
                response = requests.post(
                    self.auth_url,
                    headers=headers,
                    data=payload,
                    verify=False,
                    timeout=30
                )

                logger.info(f"📊 Статус ответа аутентификации: {response.status_code}")
                
                if response.status_code == 200:
                    data = response.json()
                    self.access_token = data.get('access_token')
                    
                    if not self.access_token:
                        logger.error("❌ В ответе нет access_token")
                        continue
                    
                    expires_in = data.get('expires_in', 1800)
                    self.token_expires = datetime.now() + timedelta(seconds=expires_in - 300)
                    
                    logger.info("✅ Успешная аутентификация в GigaChat")
                    return True
                else:
                    logger.warning(f"⚠️ Ошибка аутентификации: {response.status_code} - {response.text}")
                    if attempt < max_retries - 1:
                        wait_time = 2 ** attempt  # Экспоненциальная задержка
                        logger.info(f"⏳ Ожидание {wait_time} секунд перед повторной попыткой...")
                        time.sleep(wait_time)

            except requests.exceptions.Timeout:
                logger.error(f"⏰ Таймаут при аутентификации (попытка {attempt + 1})")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)
                continue
                    
            except requests.exceptions.ConnectionError as e:
                logger.error(f"🔌 Ошибка соединения при аутентификации (попытка {attempt + 1}): {e}")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)
                continue
                    
            except Exception as e:
                logger.error(f"❌ Неожиданная ошибка при аутентификации (попытка {attempt + 1}): {e}")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)
                continue

        logger.error("❌ Все попытки аутентификации завершились неудачей")
        return False

    def chat_completion(self, messages, temperature=0.7, max_tokens=1024, max_retries=3) -> Optional[str]:
        """Отправка запроса к чат-модели GigaChat с повторными попытками"""
        for attempt in range(max_retries):
            try:
                if not self._authenticate():
                    return "Извините, произошла ошибка при подключении к AI-сервису."

                headers = {
                    'Authorization': f'Bearer {self.access_token}',
                    'Content-Type': 'application/json',
                    'Accept': 'application/json'
                }

                data = {
                    "model": "GigaChat",
                    "messages": messages,
                    "temperature": temperature,
                    "max_tokens": max_tokens,
                    "stream": False
                }

                logger.info(f"💬 Попытка чат-запроса {attempt + 1}/{max_retries}")
                
                response = requests.post(
                    f'{self.api_base_url}chat/completions',
                    headers=headers,
                    json=data,
                    verify=False,
                    timeout=60
                )

                logger.info(f"📊 Статус ответа чата: {response.status_code}")
                
                if response.status_code == 200:
                    result = response.json()
                    response_text = result['choices'][0]['message']['content']
                    logger.info("✅ Успешно получен ответ от GigaChat")
                    return response_text
                else:
                    logger.warning(f"⚠️ Ошибка чат-запроса: {response.status_code} - {response.text}")
                    if attempt < max_retries - 1:
                        wait_time = 2 ** attempt
                        logger.info(f"⏳ Ожидание {wait_time} секунд перед повторной попыткой...")
                        time.sleep(wait_time)
                        continue
                    else:
                        return "Извините, произошла ошибка при обработке запроса."

            except requests.exceptions.Timeout:
                logger.error(f"⏰ Таймаут при запросе к GigaChat (попытка {attempt + 1})")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)
                continue
                
            except requests.exceptions.ConnectionError as e:
                logger.error(f"🔌 Ошибка соединения с GigaChat (попытка {attempt + 1}): {e}")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)
                continue
                
            except Exception as e:
                logger.error(f"❌ Неожиданная ошибка при запросе к GigaChat (попытка {attempt + 1}): {e}")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)
                continue

        return "Извините, в настоящее время сервис недоступен. Пожалуйста, попробуйте позже."

    def test_connection(self):
        """Тестирование подключения к GigaChat"""
        logger.info("🔍 Тестируем подключение к GigaChat...")
        success = self._authenticate()
        
        if success:
            try:
                headers = {
                    'Authorization': f'Bearer {self.access_token}',
                    'Accept': 'application/json'
                }

                response = requests.get(
                    f'{self.api_base_url}models',
                    headers=headers,
                    verify=False,
                    timeout=30
                )

                if response.status_code == 200:
                    models = response.json()
                    return True, f"✅ Подключение успешно! Доступно моделей: {len(models.get('data', []))}"
                else:
                    return True, f"✅ Аутентификация успешна, но ошибка получения моделей: {response.status_code}"
                    
            except Exception as e:
                return True, f"✅ Аутентификация успешна, но ошибка теста: {e}"
        else:
            return False, "❌ Ошибка аутентификации"