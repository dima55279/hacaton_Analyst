from telegram import Update, ReplyKeyboardMarkup, ReplyKeyboardRemove
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes, ConversationHandler
import logging
import asyncio
import mysql.connector
from enum import Enum

logger = logging.getLogger(__name__)

class AddressStates(Enum):
    WAITING_FOR_ADDRESS = 1
    WAITING_FOR_SETTLEMENT = 2
    WAITING_FOR_STREET = 3
    WAITING_FOR_HOUSE = 4

class CitizenBot:
    def __init__(self, token, appeals_system, db_config):
        self.token = token
        self.system = appeals_system
        self.application = None
        self.db_config = db_config
        
    def get_settlements_db(self):
        """Подключение к базе данных населенных пунктов"""
        try:
            return mysql.connector.connect(**self.db_config)
        except Exception as e:
            logger.error(f"Ошибка подключения к базе данных: {e}")
            raise

    async def start_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Команда начала работы"""
        welcome_text = """
👋 Добро пожаловать в систему обращений граждан!

Я помогу вам:
• Подать жалобу или предложение
• Получить информацию по вопросам ЖКХ, благоустройства, транспорта
• Отправить запрос в органы власти

Для подачи обращения нажмите "Подать обращение" и следуйте инструкциям.

Для справки используйте /help
"""
        keyboard = [['Подать обращение', 'Мои обращения']]
        reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
        
        await update.message.reply_text(welcome_text, reply_markup=reply_markup)
        return ConversationHandler.END

    async def start_appeal_process(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Начало процесса подачи обращения"""
        # Сбрасываем возможные предыдущие данные
        context.user_data.clear()
        
        instruction_text = """
🏠 Для обработки вашего обращения нам нужен адрес.

Пожалуйста, введите название вашего населенного пункта (город, село, деревня):
"""
        await update.message.reply_text(instruction_text, reply_markup=ReplyKeyboardRemove())
        return AddressStates.WAITING_FOR_SETTLEMENT

    async def handle_settlement(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработка ввода населенного пункта с исправленной логикой выбора"""
        user_input = update.message.text.strip()
        
        # Если это выбор из списка (содержит скобки с районом)
        if '(' in user_input and ')' in user_input:
            # Это выбор из предложенных вариантов - извлекаем чистое название
            settlement_name = user_input.split('(')[0].strip()
            
            # Удаляем тип населенного пункта из начала, если он есть
            type_prefixes = ['город', 'посёлок', 'село', 'деревня', 'пгт', 'станция', 'хутор']
            for prefix in type_prefixes:
                if settlement_name.startswith(prefix):
                    settlement_name = settlement_name[len(prefix):].strip()
                    break
        else:
            # Это прямой ввод пользователя
            settlement_name = user_input
        
        context.user_data['settlement'] = settlement_name
        
        # Проверяем существование населенного пункта в базе
        try:
            conn = self.get_settlements_db()
            cursor = conn.cursor(dictionary=True)
            
            # Ищем точное совпадение по названию
            cursor.execute(
                "SELECT name, type, district, population FROM settlements WHERE name = %s",
                (settlement_name,)
            )
            
            exact_results = cursor.fetchall()
            
            # Если точное совпадение не найдено, ищем похожие
            if not exact_results:
                cursor.execute(
                    "SELECT name, type, district, population FROM settlements WHERE name LIKE %s",
                    (f'%{settlement_name}%',)
                )
                results = cursor.fetchall()
            else:
                results = exact_results
            
            cursor.close()
            conn.close()
            
            if results:
                # Если найдены совпадения
                if len(results) == 1:
                    # Одно совпадение - используем его
                    result = results[0]
                    context.user_data['settlement_info'] = {
                        'name': result['name'],
                        'type': result['type'],
                        'district': result['district'],
                        'population': result['population']
                    }
                    
                    await update.message.reply_text(
                        f"✅ Найден населенный пункт: {result['type']} {result['name']}, {result['district']}\n\n"
                        f"Теперь введите название улицы:"
                    )
                    return AddressStates.WAITING_FOR_STREET
                else:
                    # Несколько совпадений - предлагаем выбрать
                    keyboard = []
                    display_names = []
                    
                    for result in results:
                        display_name = f"{result['type']} {result['name']}"
                        if result['district']:
                            display_name += f" ({result['district']})"
                        keyboard.append([display_name])
                        display_names.append(display_name)
                    
                    # Сохраняем mapping для последующего поиска
                    context.user_data['settlement_mapping'] = {
                        display_names[i]: results[i] for i in range(len(display_names))
                    }
                    
                    reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
                    await update.message.reply_text(
                        f"Найдено {len(results)} населенных пунктов. Выберите подходящий:",
                        reply_markup=reply_markup
                    )
                    return AddressStates.WAITING_FOR_SETTLEMENT
            else:
                # Населенный пункт не найден, но продолжаем
                await update.message.reply_text(
                    f"⚠️ Населенный пункт '{settlement_name}' не найден в базе. "
                    f"Продолжаем с введенным названием.\n\n"
                    f"Теперь введите название улицы:"
                )
                return AddressStates.WAITING_FOR_STREET
                
        except Exception as e:
            logger.error(f"Ошибка поиска населенного пункта: {e}")
            await update.message.reply_text(
                f"Теперь введите название улицы:"
            )
            return AddressStates.WAITING_FOR_STREET

    async def handle_street(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработка ввода улицы"""
        street = update.message.text.strip()
        context.user_data['street'] = street
        
        await update.message.reply_text(
            "Теперь введите номер дома:",
            reply_markup=ReplyKeyboardRemove()
        )
        return AddressStates.WAITING_FOR_HOUSE

    async def handle_house(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработка ввода номера дома и завершение сбора адреса"""
        house = update.message.text.strip()
        context.user_data['house'] = house
        
        # Формируем полный адрес
        settlement = context.user_data.get('settlement', '')
        street = context.user_data.get('street', '')
        
        full_address = f"{settlement}, {street}, {house}"
        context.user_data['full_address'] = full_address
        
        # Сохраняем адрес для использования в обращении
        address_info = {
            'settlement': settlement,
            'street': street,
            'house': house,
            'full_address': full_address
        }
        
        if 'settlement_info' in context.user_data:
            address_info.update(context.user_data['settlement_info'])
        
        context.user_data['address_info'] = address_info
        
        await update.message.reply_text(
            f"✅ Адрес сохранен: {full_address}\n\n"
            f"Теперь опишите ваше обращение:"
        )
        return AddressStates.WAITING_FOR_ADDRESS

    async def handle_appeal_with_address(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработка обращения с прикрепленным адресом"""
        user = update.message.from_user
        appeal_text = update.message.text
        address_info = context.user_data.get('address_info', {})
        
        logger.info(f"Обращение от {user.first_name} с адресом {address_info.get('full_address', 'не указан')}: {appeal_text}")
        await update.message.chat.send_action(action="typing")

        try:
            # Обработка обращения с адресом
            response = self.system.process_citizen_appeal(
                user_id=str(user.id),
                appeal_text=appeal_text,
                platform="telegram",
                address_info=address_info
            )
            
            # Показываем клавиатуру с основными командами
            keyboard = [['Подать обращение', 'Мои обращения']]
            reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
            
            await update.message.reply_text(response, reply_markup=reply_markup)
            logger.info(f"Ответ отправлен пользователю {user.first_name}")

        except Exception as e:
            logger.error(f"Ошибка обработки обращения: {e}")
            error_message = "Извините, произошла ошибка при обработке обращения. Пожалуйста, попробуйте позже."
            await update.message.reply_text(error_message)
        
        # Очищаем данные адреса
        if 'address_info' in context.user_data:
            del context.user_data['address_info']
        
        return ConversationHandler.END

    async def cancel_address(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Отмена процесса ввода адреса"""
        await update.message.reply_text(
            "Ввод адреса отменен.",
            reply_markup=ReplyKeyboardMarkup([['Подать обращение', 'Мои обращения']], resize_keyboard=True)
        )
        context.user_data.clear()
        return ConversationHandler.END

    async def help_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Команда помощи"""
        help_text = """
📋 Как пользоваться ботом:

1. Нажмите "Подать обращение"
2. Введите адрес по шагам:
   - Населенный пункт
   - Улица
   - Номер дома
3. Опишите ваше обращение

Примеры обращений:
• "Во дворе разбита дорога, требуется ремонт"
• "Не работает уличное освещение"
• "Предлагаю установить новые лавочки"

Ваши обращения сохраняются в системе с привязкой к адресу.
"""
        await update.message.reply_text(help_text)

    async def show_my_appeals(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Показать обращения пользователя"""
        user = update.message.from_user
        
        try:
            appeals = self.system.database.get_appeals({
                'user_id': str(user.id)
            }, limit=5)
            
            if not appeals:
                await update.message.reply_text("У вас пока нет обращений.")
                return
            
            response = "📋 Ваши последние обращения:\n\n"
            for i, appeal in enumerate(appeals, 1):
                status_emoji = "✅" if appeal['status'] == 'answered' else "⏳" if appeal['status'] == 'in_progress' else "📝"
                address = appeal.get('full_address', 'адрес не указан')
                response += f"{i}. {status_emoji} {appeal['text'][:50]}...\n"
                response += f"   📍 Адрес: {address}\n"
                response += f"   Статус: {appeal['status']}\n"
                if appeal.get('response'):
                    response += f"   Ответ: {appeal['response'][:100]}...\n"
                response += "\n"
            
            await update.message.reply_text(response)
            
        except Exception as e:
            logger.error(f"Ошибка получения обращений: {e}")
            await update.message.reply_text("Ошибка при получении ваших обращений.")

    async def handle_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработка текстовых сообщений"""
        text = update.message.text
        
        if text == 'Подать обращение':
            await self.start_appeal_process(update, context)
        elif text == 'Мои обращения':
            await self.show_my_appeals(update, context)
        else:
            # Если не в процессе ввода адреса, предлагаем начать обращение
            keyboard = [['Подать обращение', 'Мои обращения']]
            reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
            await update.message.reply_text(
                "Для подачи обращения нажмите 'Подать обращение'",
                reply_markup=reply_markup
            )

    def setup_handlers(self):
        """Настройка обработчиков с ConversationHandler для адреса"""
        # ConversationHandler для процесса ввода адреса
        address_conv_handler = ConversationHandler(
            entry_points=[MessageHandler(filters.Regex('^Подать обращение$'), self.start_appeal_process)],
            states={
                AddressStates.WAITING_FOR_SETTLEMENT: [
                    MessageHandler(filters.TEXT & ~filters.COMMAND, self.handle_settlement)
                ],
                AddressStates.WAITING_FOR_STREET: [
                    MessageHandler(filters.TEXT & ~filters.COMMAND, self.handle_street)
                ],
                AddressStates.WAITING_FOR_HOUSE: [
                    MessageHandler(filters.TEXT & ~filters.COMMAND, self.handle_house)
                ],
                AddressStates.WAITING_FOR_ADDRESS: [
                    MessageHandler(filters.TEXT & ~filters.COMMAND, self.handle_appeal_with_address)
                ],
            },
            fallbacks=[CommandHandler('cancel', self.cancel_address)]
        )

        self.application.add_handler(CommandHandler("start", self.start_command))
        self.application.add_handler(CommandHandler("help", self.help_command))
        self.application.add_handler(address_conv_handler)
        self.application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, self.handle_message))

    def run(self):
        """Запуск бота с созданием нового event loop"""
        try:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            
            self.application = Application.builder().token(self.token).build()
            self.setup_handlers()

            logger.info("🤖 Бот для граждан запущен...")
            self.application.run_polling()

        except Exception as e:
            logger.error(f"❌ Ошибка запуска бота для граждан: {e}")