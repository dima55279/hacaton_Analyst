from telegram import Update, ReplyKeyboardMarkup
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes
import logging
import asyncio
from datetime import datetime

logger = logging.getLogger(__name__)

class AnalystBot:
    def __init__(self, token, appeals_system):
        self.token = token
        self.system = appeals_system
        self.application = None

    async def start_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Команда начала работы для аналитиков"""
        welcome_text = """
📊 Панель аналитика системы обращений

Доступные команды:
/stats - Статистика обращений
/trends - Анализ трендов
/appeals - Последние обращения
/help - Справка

Используйте кнопки для быстрого доступа к функциям.
"""
        keyboard = [
            ['📈 Статистика', '📊 Тренды'],
            ['📝 Обращения', '🔄 Обновить']
        ]
        reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
        
        await update.message.reply_text(welcome_text, reply_markup=reply_markup)

    async def show_stats(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Показать статистику"""
        try:
            stats = self.system.database.get_appeals_stats(30)
            
            if not stats:
                await update.message.reply_text("Нет данных за указанный период.")
                return
            
            # Агрегация статистики
            type_counts = {}
            status_counts = {}
            total = 0
            
            for stat in stats:
                appeal_type = stat['type'] or 'не определен'
                type_counts[appeal_type] = type_counts.get(appeal_type, 0) + stat['count']
                status_counts[stat['status']] = status_counts.get(stat['status'], 0) + stat['count']
                total += stat['count']
            
            response = f"📈 Статистика за 30 дней:\n\n"
            response += f"Всего обращений: {total}\n\n"
            
            response += "По типам:\n"
            for appeal_type, count in sorted(type_counts.items(), key=lambda x: x[1], reverse=True)[:5]:
                response += f"• {appeal_type}: {count}\n"
            
            response += "\nПо статусам:\n"
            for status, count in status_counts.items():
                response += f"• {status}: {count}\n"
            
            await update.message.reply_text(response)
            
        except Exception as e:
            logger.error(f"Ошибка получения статистики: {e}")
            await update.message.reply_text("Ошибка при получении статистики.")

    async def show_trends(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Показать тренды"""
        try:
            trends = self.system.get_analytics(30)
            
            response = f"📊 Анализ трендов за 30 дней:\n\n"
            response += f"Всего обращений: {trends['total_appeals']}\n"
            response += f"Процент ответов: {trends['response_rate']}%\n\n"
            
            response += "Распределение по типам:\n"
            for appeal_type, count in trends['type_distribution'].items():
                response += f"• {appeal_type}: {count}\n"
            
            response += "\nЧастые темы:\n"
            for theme in trends['common_themes'][:5]:
                response += f"• {theme['theme']} ({theme['frequency']})\n"
            
            await update.message.reply_text(response)
            
        except Exception as e:
            logger.error(f"Ошибка получения трендов: {e}")
            await update.message.reply_text("Ошибка при получении трендов.")

    async def show_recent_appeals(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Показать последние обращения"""
        try:
            appeals = self.system.database.get_appeals(limit=5)
            
            if not appeals:
                await update.message.reply_text("Нет обращений для отображения.")
                return
            
            response = "📝 Последние обращения:\n\n"
            for i, appeal in enumerate(appeals, 1):
                status_emoji = "✅" if appeal['status'] == 'answered' else "⏳"
                response += f"{i}. {status_emoji} {appeal['type'] or 'не определен'}\n"
                response += f"   {appeal['text'][:100]}...\n"
                response += f"   Статус: {appeal['status']}\n\n"
            
            await update.message.reply_text(response)
            
        except Exception as e:
            logger.error(f"Ошибка получения обращений: {e}")
            await update.message.reply_text("Ошибка при получении обращений.")

    async def help_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Команда помощи для аналитиков"""
        help_text = """
📋 Панель аналитика:

/stats - Статистика обращений за последние 30 дней
/trends - Анализ трендов и частых тем
/appeals - Просмотр последних обращений

Используйте кнопки для быстрого доступа к функциям.
"""
        await update.message.reply_text(help_text)

    async def handle_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработка текстовых сообщений"""
        text = update.message.text
        
        if text == '📈 Статистика':
            await self.show_stats(update, context)
        elif text == '📊 Тренды':
            await self.show_trends(update, context)
        elif text == '📝 Обращения':
            await self.show_recent_appeals(update, context)
        elif text == '🔄 Обновить':
            await update.message.reply_text("Данные обновлены!")

    def setup_handlers(self):
        """Настройка обработчиков"""
        self.application.add_handler(CommandHandler("start", self.start_command))
        self.application.add_handler(CommandHandler("stats", self.show_stats))
        self.application.add_handler(CommandHandler("trends", self.show_trends))
        self.application.add_handler(CommandHandler("appeals", self.show_recent_appeals))
        self.application.add_handler(CommandHandler("help", self.help_command))
        self.application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, self.handle_message))

    def run(self):
        """Запуск бота с созданием нового event loop"""
        try:
            # Создаем новый event loop для этого процесса
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            
            self.application = Application.builder().token(self.token).build()
            self.setup_handlers()

            logger.info("🤖 Бот для аналитиков запущен...")
            self.application.run_polling()

        except Exception as e:
            logger.error(f"❌ Ошибка запуска бота аналитиков: {e}")