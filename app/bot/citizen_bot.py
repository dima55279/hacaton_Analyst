from telegram import Update, ReplyKeyboardMarkup
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes
import logging

logger = logging.getLogger(__name__)

class CitizenBot:
    def __init__(self, token, appeals_system):
        self.token = token
        self.system = appeals_system
        self.application = None

    async def start_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Команда начала работы"""
        welcome_text = """
👋 Добро пожаловать в систему обращений граждан!

Я помогу вам:
• Подать жалобу или предложение
• Получить информацию по вопросам ЖКХ, благоустройства, транспорта
• Отправить запрос в органы власти

Просто напишите ваше обращение, и я его обработаю.

Для справки используйте /help
"""
        keyboard = [['Подать обращение', 'Мои обращения']]
        reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
        
        await update.message.reply_text(welcome_text, reply_markup=reply_markup)

    async def help_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Команда помощи"""
        help_text = """
📋 Как пользоваться ботом:

1. Напишите ваше обращение в свободной форме
2. Система автоматически определит тип обращения
3. Для типовых вопросов вы получите ответ сразу
4. Сложные обращения передаются специалистам

Примеры обращений:
• "Во дворе дома №5 по улице Ленина разбита дорога"
• "Предлагаю установить новые лавочки в парке Горького"
• "Как получить справку о составе семьи?"

Ваши обращения сохраняются в системе.
"""
        await update.message.reply_text(help_text)

    async def handle_appeal(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработка обращения гражданина"""
        user = update.message.from_user
        appeal_text = update.message.text

        logger.info(f"Обращение от {user.first_name} ({user.id}): {appeal_text}")
        await update.message.chat.send_action(action="typing")

        try:
            # Обработка обращения
            response = self.system.process_citizen_appeal(
                user_id=str(user.id),
                appeal_text=appeal_text,
                platform="telegram"
            )
            
            await update.message.reply_text(response)
            logger.info(f"Ответ отправлен пользователю {user.first_name}")

        except Exception as e:
            logger.error(f"Ошибка обработки обращения: {e}")
            error_message = "Извините, произошла ошибка при обработке обращения. Пожалуйста, попробуйте позже."
            await update.message.reply_text(error_message)

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
                response += f"{i}. {status_emoji} {appeal['text'][:50]}...\n"
                response += f"   Статус: {appeal['status']}\n"
                if appeal['response']:
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
            await update.message.reply_text("Напишите ваше обращение:")
        elif text == 'Мои обращения':
            await self.show_my_appeals(update, context)
        else:
            await self.handle_appeal(update, context)

    def setup_handlers(self):
        """Настройка обработчиков"""
        self.application.add_handler(CommandHandler("start", self.start_command))
        self.application.add_handler(CommandHandler("help", self.help_command))
        self.application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, self.handle_message))

    def run(self):
        """Запуск бота"""
        try:
            self.application = Application.builder().token(self.token).build()
            self.setup_handlers()

            logger.info("🤖 Бот для граждан запущен...")
            self.application.run_polling()

        except Exception as e:
            logger.error(f"Ошибка запуска бота: {e}")