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
/stats - Актуальная статистика
/trends - Анализ трендов
/appeals - Последние обращения
/refresh - Обновить данные
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
        """Показать актуальную статистику в реальном времени"""
        try:
            # Получаем актуальные данные из базы
            stats = self.system.database.get_real_time_stats()
            
            if not stats:
                await update.message.reply_text("❌ Нет данных для отображения.")
                return
            
            response = f"📈 АКТУАЛЬНАЯ СТАТИСТИКА\n\n"
            response += f"📊 Всего обращений: {stats['total']}\n"
            response += f"🕐 За последние 24 часа: {stats['last_24h']}\n\n"
            
            response += "📋 По статусам:\n"
            for status, count in stats['status_stats'].items():
                status_emoji = self._get_status_emoji(status)
                response += f"  {status_emoji} {status}: {count}\n"
            
            response += "\n🏷️ Топ-5 по типам:\n"
            for i, type_stat in enumerate(stats['type_stats'], 1):
                appeal_type = type_stat['type'] or 'не определен'
                response += f"  {i}. {appeal_type}: {type_stat['count']}\n"
            
            response += f"\n⏰ Обновлено: {datetime.now().strftime('%H:%M:%S')}"
            
            await update.message.reply_text(response)
            
        except Exception as e:
            logger.error(f"❌ Ошибка получения статистики: {e}")
            await update.message.reply_text("❌ Ошибка при получении статистики.")

    async def show_trends(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Показать тренды с актуальными данными"""
        try:
            trends = self.system.get_analytics(30)
            
            if not trends:
                await update.message.reply_text("❌ Нет данных для анализа трендов.")
                return
            
            response = f"📊 АНАЛИЗ ТРЕНДОВ (30 дней)\n\n"
            response += f"📈 Всего обращений: {trends['total_appeals']}\n"
            response += f"📞 Процент ответов: {trends['response_rate']}%\n\n"
            
            response += "🏷️ Распределение по типам:\n"
            for appeal_type, count in trends['type_distribution'].items():
                response += f"  • {appeal_type}: {count}\n"
            
            response += "\n🔍 Частые темы:\n"
            for theme in trends.get('common_themes', [])[:5]:
                theme_name = theme.get('theme', 'не определена')
                frequency = theme.get('frequency', 'неизвестно')
                response += f"  • {theme_name} ({frequency})\n"
            
            response += f"\n⏰ Обновлено: {datetime.now().strftime('%H:%M:%S')}"
            
            await update.message.reply_text(response)
            
        except Exception as e:
            logger.error(f"❌ Ошибка получения трендов: {e}")
            await update.message.reply_text("❌ Ошибка при получении трендов.")

    async def show_recent_appeals(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Показать последние обращения (актуальные данные)"""
        try:
            # Получаем актуальные данные из базы
            appeals = self.system.database.get_recent_appeals(5)
            
            if not appeals:
                await update.message.reply_text("📭 Нет обращений для отображения.")
                return
            
            response = "📝 ПОСЛЕДНИЕ ОБРАЩЕНИЯ\n\n"
            for i, appeal in enumerate(appeals, 1):
                status_emoji = self._get_status_emoji(appeal['status'])
                appeal_type = appeal['type'] or 'не определен'
                created_time = appeal['created_at'].strftime('%H:%M') if isinstance(appeal['created_at'], datetime) else appeal['created_at']
                
                response += f"{i}. {status_emoji} *{appeal_type}*\n"
                response += f"   📄 {appeal['text'][:80]}...\n"
                response += f"   🏷️ Статус: {appeal['status']}\n"
                response += f"   ⏰ {created_time}\n\n"
            
            response += f"🔄 Автоматически обновляется при запросе"
            
            await update.message.reply_text(response, parse_mode='Markdown')
            
        except Exception as e:
            logger.error(f"❌ Ошибка получения обращений: {e}")
            await update.message.reply_text("❌ Ошибка при получении обращений.")

    async def refresh_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Принудительное обновление данных"""
        try:
            # Просто отправляем сообщение, что данные актуальны
            # Фактическое обновление происходит при каждом запросе к базе
            response = "🔄 Данные успешно обновлены!\n\n"
            response += "Все команды теперь показывают актуальную информацию из базы данных в реальном времени."
            
            await update.message.reply_text(response)
            
        except Exception as e:
            logger.error(f"❌ Ошибка обновления: {e}")
            await update.message.reply_text("❌ Ошибка при обновлении данных.")

    async def help_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Команда помощи для аналитиков"""
        help_text = """
📋 ПАНЕЛЬ АНАЛИТИКА

*/stats* - Актуальная статистика в реальном времени
*/trends* - Анализ трендов за 30 дней
*/appeals* - Просмотр последних обращений
*/refresh* - Принудительное обновление данных
*/help* - Эта справка

💡 *Особенности:*
• Все данные обновляются автоматически при каждом запросе
• Статистика показывает текущее состояние системы
• Время обновления указывается в каждом отчете

Используйте кнопки для быстрого доступа к функциям.
"""
        await update.message.reply_text(help_text, parse_mode='Markdown')

    def _get_status_emoji(self, status):
        """Получить emoji для статуса"""
        status_emojis = {
            'new': '🆕',
            'answered': '✅',
            'in_progress': '🔄',
            'requires_manual_review': '👨‍💼',
            'closed': '🔒'
        }
        return status_emojis.get(status, '📄')

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
            await self.refresh_command(update, context)

    def setup_handlers(self):
        """Настройка обработчиков"""
        self.application.add_handler(CommandHandler("start", self.start_command))
        self.application.add_handler(CommandHandler("stats", self.show_stats))
        self.application.add_handler(CommandHandler("trends", self.show_trends))
        self.application.add_handler(CommandHandler("appeals", self.show_recent_appeals))
        self.application.add_handler(CommandHandler("refresh", self.refresh_command))
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