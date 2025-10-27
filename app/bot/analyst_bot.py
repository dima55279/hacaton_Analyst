from telegram import Update, ReplyKeyboardMarkup
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes
import logging
import asyncio
from datetime import datetime
import matplotlib.pyplot as plt
import matplotlib
matplotlib.use('Agg')  # Для работы без GUI
import io
import numpy as np
import seaborn as sns
from datetime import timedelta

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
/charts - Графики и диаграммы
/refresh - Обновить данные
/help - Справка

Используйте кнопки для быстрого доступа к функциям.
"""
        keyboard = [
            ['📈 Статистика', '📊 Тренды'],
            ['📝 Обращения', '📊 Графики'],
            ['🔄 Обновить', 'ℹ️ Помощь']
        ]
        reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
        
        await update.message.reply_text(welcome_text, reply_markup=reply_markup)

    async def show_municipality_stats(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Показать статистику по муниципалитетам"""
        try:
            # Получаем статистику по муниципалитетам
            stats = self.system.database.get_municipality_stats(30)
            
            if not stats:
                await update.message.reply_text("❌ Нет данных по муниципалитетам за указанный период.")
                return
            
            response = "🏛️ СТАТИСТИКА ПО МУНИЦИПАЛИТЕТАМ (30 дней)\n\n"
            
            # Топ-10 муниципалитетов по количеству обращений
            response += "📊 Топ-10 по количеству обращений:\n"
            for i, municipality in enumerate(stats[:10], 1):
                response += f"{i}. {municipality['municipality']}: {municipality['appeal_count']} обращений\n"
                response += f"   ✅ Отвечено: {municipality['answered_count']} "
                response += f"({municipality['response_rate']}%)\n"
                response += f"   🆕 Новых: {municipality['new_count']} | "
                response += f"🔄 В работе: {municipality['in_progress_count']}\n\n"
            
            # Общая статистика
            total_appeals = sum(m['appeal_count'] for m in stats)
            municipalities_with_data = len([m for m in stats if m['municipality'] != 'Не указан'])
            
            response += f"📈 Общая статистика:\n"
            response += f"• Всего обращений с указанием муниципалитета: {total_appeals}\n"
            response += f"• Количество муниципалитетов: {municipalities_with_data}\n"
            
            # ИСПРАВЛЕНИЕ: Проверяем, чтобы не делить на ноль
            if municipalities_with_data > 0:
                avg_appeals = total_appeals / municipalities_with_data
                response += f"• Среднее количество обращений на муниципалитет: {avg_appeals:.1f}\n"
            else:
                response += f"• Среднее количество обращений на муниципалитет: нет данных\n"
            
            response += f"\n⏰ Обновлено: {datetime.now().strftime('%H:%M:%S')}"
            
            await update.message.reply_text(response)
            
        except Exception as e:
            logger.error(f"❌ Ошибка получения статистики по муниципалитетам: {e}")
            await update.message.reply_text("❌ Ошибка при получении статистики по муниципалитетам.")

    async def show_municipality_charts(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Показать графики по муниципалитетам"""
        try:
            await update.message.reply_text("🏛️ Генерирую графики по муниципалитетам...")
            
            # Получаем данные
            stats = self.system.database.get_municipality_stats(30)
            type_stats = self.system.database.get_municipality_type_stats(30)
            
            logger.info(f"📊 Получено {len(stats)} записей статистики по муниципалитетам")
            logger.info(f"📊 Получено {len(type_stats)} записей по типам обращений")
            
            if not stats:
                await update.message.reply_text("❌ Недостаточно данных для построения графиков по муниципалитетам.")
                return

            # Создаем графики
            charts = await self._generate_municipality_charts(stats, type_stats)
            
            if not charts:
                await update.message.reply_text("❌ Не удалось сгенерировать графики по муниципалитетам.")
                return

            # Отправляем графики
            for chart_data in charts:
                try:
                    photo_buffer = io.BytesIO()
                    chart_data['figure'].savefig(photo_buffer, format='PNG', dpi=100, bbox_inches='tight')
                    photo_buffer.seek(0)
                    
                    await update.message.reply_photo(
                        photo=photo_buffer,
                        caption=chart_data['caption']
                    )
                    
                    # Закрываем figure для освобождения памяти
                    plt.close(chart_data['figure'])
                    
                except Exception as e:
                    logger.error(f"❌ Ошибка отправки графика: {e}")
                    continue
                    
        except Exception as e:
            logger.error(f"❌ Ошибка генерации графиков по муниципалитетам: {e}")
            await update.message.reply_text("❌ Ошибка при генерации графиков по муниципалитетам.")

    def _generate_municipality_charts(self, stats, type_stats):
        """Генерация графиков по муниципалитетам"""
        charts = []
        
        try:
            logger.info(f"🔄 Начинаем генерацию графиков по муниципалитетам. stats: {len(stats)}")
            
            # Фильтруем статистику - убираем "Не указан"
            valid_stats = [m for m in stats if m['municipality'] != 'Не указан']
            logger.info(f"📊 Валидных муниципалитетов: {len(valid_stats)}")
            
            if not valid_stats:
                return charts

            # 1. Столбчатая диаграмма по количеству обращений
            logger.info("📊 Создаем столбчатую диаграмму...")
            bar_chart = self._create_municipality_bar_chart(valid_stats)
            if bar_chart:
                charts.append({
                    'figure': bar_chart,
                    'caption': "📊 Количество обращений по муниципалитетам (топ-10)"
                })

            # 2. Круговая диаграмма распределения обращений
            logger.info("📈 Создаем круговую диаграмму...")
            pie_chart = self._create_municipality_pie_chart(valid_stats)
            if pie_chart:
                charts.append({
                    'figure': pie_chart,
                    'caption': "📈 Распределение обращений по муниципалитетам"
                })

            # 3. Heatmap по типам обращений в муниципалитетах
            if type_stats:
                logger.info("🔥 Создаем тепловую карту...")
                heatmap = self._create_municipality_heatmap(type_stats)
                if heatmap:
                    charts.append({
                        'figure': heatmap,
                        'caption': "🔥 Распределение типов обращений по муниципалитетам"
                    })
            
            logger.info(f"✅ Успешно создано {len(charts)} графиков по муниципалитетам")
                
        except Exception as e:
            logger.error(f"❌ Ошибка создания графиков по муниципалитетам: {e}")
            
        return charts

    def _create_municipality_bar_chart(self, stats):
        """Создание столбчатой диаграммы по муниципалитетам с полными названиями"""
        try:
            # Берем топ-10 муниципалитетов
            top_municipalities = stats[:10]
            
            logger.info(f"📊 Создаем барчарт для {len(top_municipalities)} муниципалитетов")
            
            if not top_municipalities:
                return None
            
            municipalities = []
            counts = []
            
            for m in top_municipalities:
                name = m['municipality']
                # Сохраняем полное название без сокращений
                municipalities.append(name)
                counts.append(m['appeal_count'])
            
            # Настраиваем стиль
            plt.style.use('seaborn-v0_8')
            
            # Увеличиваем размер фигуры для лучшего отображения длинных названий
            fig, ax = plt.subplots(figsize=(16, 10))
            
            # Используем разные цвета для лучшей визуализации
            colors = plt.cm.viridis(np.linspace(0, 1, len(municipalities)))
            
            bars = ax.bar(municipalities, counts, color=colors, edgecolor='black', alpha=0.8)
            
            # Добавляем значения на столбцы
            for bar, count in zip(bars, counts):
                height = bar.get_height()
                ax.text(bar.get_x() + bar.get_width()/2., height + 0.1,
                    f'{count}', ha='center', va='bottom', fontweight='bold', fontsize=10)
            
            ax.set_ylabel('Количество обращений', fontsize=12, fontweight='bold')
            ax.set_xlabel('Муниципалитеты', fontsize=12, fontweight='bold')
            ax.set_title('Топ-10 муниципалитетов по количеству обращений', 
                        fontsize=16, fontweight='bold', pad=20)
            
            # Улучшаем отображение подписей - увеличиваем угол и уменьшаем шрифт для длинных названий
            plt.xticks(
                rotation=45, 
                ha='right',
                fontsize=9,  # Уменьшаем шрифт для лучшего размещения
                wrap=True    # Разрешаем перенос слов
            )
            plt.yticks(fontsize=10)
            
            # Добавляем сетку
            ax.grid(True, axis='y', alpha=0.3)
            ax.set_axisbelow(True)
            
            # Автоматически настраиваем layout для предотвращения обрезания
            plt.tight_layout()
            
            return fig
            
        except Exception as e:
            logger.error(f"❌ Ошибка создания столбчатой диаграммы: {e}")
            return None

    def _create_municipality_heatmap(self, type_stats):
        """Создание тепловой карты по типам обращений в муниципалитетах с полными названиями"""
        try:
            # Группируем данные для тепловой карты
            municipalities = list(set([s['municipality'] for s in type_stats]))
            appeal_types = list(set([s['appeal_type'] for s in type_stats]))
            
            # Создаем матрицу
            data_matrix = np.zeros((len(municipalities), len(appeal_types)))
            
            for stat in type_stats:
                i = municipalities.index(stat['municipality'])
                j = appeal_types.index(stat['appeal_type'])
                data_matrix[i, j] = stat['type_count']
            
            # Ограничиваем количество для читаемости, но сохраняем полные названия
            if len(municipalities) > 15:
                # Сортируем по общему количеству обращений и берем топ-15
                municipality_totals = data_matrix.sum(axis=1)
                top_indices = np.argsort(municipality_totals)[-15:][::-1]
                municipalities = [municipalities[i] for i in top_indices]
                data_matrix = data_matrix[top_indices, :]
            
            if len(appeal_types) > 10:
                # Сортируем типы по частоте и берем топ-10
                type_totals = data_matrix.sum(axis=0)
                top_type_indices = np.argsort(type_totals)[-10:][::-1]
                appeal_types = [appeal_types[i] for i in top_type_indices]
                data_matrix = data_matrix[:, top_type_indices]
            
            # Создаем тепловую карту с увеличенным размером
            fig, ax = plt.subplots(figsize=(16, 12))
            im = ax.imshow(data_matrix, cmap='YlOrRd', aspect='auto')
            
            # Настройки осей с полными названиями
            ax.set_xticks(np.arange(len(appeal_types)))
            ax.set_yticks(np.arange(len(municipalities)))
            
            # Устанавливаем подписи с полными названиями и настраиваем отображение
            ax.set_xticklabels(appeal_types, rotation=45, ha='right', fontsize=9)
            ax.set_yticklabels(municipalities, fontsize=9)  # Полные названия без сокращений
            
            # Добавляем значения в ячейки
            for i in range(len(municipalities)):
                for j in range(len(appeal_types)):
                    if data_matrix[i, j] > 0:
                        text = ax.text(j, i, int(data_matrix[i, j]),
                                      ha="center", va="center", color="black", fontsize=8)
            
            ax.set_title("Тепловая карта: типы обращений по муниципалитетам", 
                        fontsize=16, fontweight='bold', pad=20)
            plt.colorbar(im, ax=ax, label='Количество обращений')
            
            # Увеличиваем отступы для предотвращения обрезания
            plt.tight_layout()
            
            return fig
            
        except Exception as e:
            logger.error(f"❌ Ошибка создания тепловой карты: {e}")
            return None

    def _create_municipality_pie_chart(self, stats):
        """Создание круговой диаграммы распределения по муниципалитетам с полными названиями"""
        # Фильтруем муниципалитеты, убирая "Не указан"
        valid_stats = [m for m in stats if m['municipality'] != 'Не указан']
        
        if not valid_stats:
            fig, ax = plt.subplots(figsize=(10, 8))
            ax.text(0.5, 0.5, 'Нет данных по муниципалитетам\n(все обращения без указания района)', 
                    ha='center', va='center', transform=ax.transAxes, fontsize=12)
            ax.set_title('Распределение по муниципалитетам', fontsize=14)
            return fig
        
        # Берем топ-8 муниципалитетов, остальные объединяем в "Другие"
        if len(valid_stats) > 8:
            top_municipalities = valid_stats[:8]
            other_count = sum(m['appeal_count'] for m in valid_stats[8:])
            top_municipalities.append({
                'municipality': 'Другие',
                'appeal_count': other_count
            })
        else:
            top_municipalities = valid_stats
        
        labels = [m['municipality'] for m in top_municipalities]
        sizes = [m['appeal_count'] for m in top_municipalities]
        
        # Если есть длинные названия, разбиваем их на несколько строк
        formatted_labels = []
        for label in labels:
            if len(label) > 20:  # Если название длиннее 20 символов
                # Разбиваем по словам и пытаемся найти хорошее место для переноса
                words = label.split()
                if len(words) > 1:
                    # Разбиваем на две примерно равные части
                    mid = len(words) // 2
                    formatted_label = ' '.join(words[:mid]) + '\n' + ' '.join(words[mid:])
                    formatted_labels.append(formatted_label)
                else:
                    # Если это одно длинное слово, просто разбиваем пополам
                    mid = len(label) // 2
                    formatted_labels.append(label[:mid] + '\n' + label[mid:])
            else:
                formatted_labels.append(label)
        
        # Цветовая схема
        colors = plt.cm.Paired(np.linspace(0, 1, len(labels)))
        
        # Создаем диаграмму с увеличенным размером для лучшего отображения
        fig, ax = plt.subplots(figsize=(14, 10))
        wedges, texts, autotexts = ax.pie(
            sizes, 
            labels=formatted_labels,  # Используем форматированные подписи
            colors=colors,
            autopct='%1.1f%%',
            startangle=90,
            textprops={'fontsize': 9}  # Уменьшаем шрифт для лучшего размещения
        )
        
        # Улучшаем отображение
        for autotext in autotexts:
            autotext.set_color('white')
            autotext.set_fontweight('bold')
            autotext.set_fontsize(8)
        
        ax.set_title('Распределение обращений по муниципалитетам', 
                    fontsize=16, fontweight='bold', pad=20)
        plt.tight_layout()
        
        return fig

    async def show_charts(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Показать все графики включая муниципалитеты"""
        try:
            await update.message.reply_text("📊 Генерирую все графики...")
            
            # Получаем данные за последние 30 дней
            stats = self.system.database.get_appeals_stats(30)
            municipality_stats = self.system.database.get_municipality_stats(30)
            municipality_type_stats = self.system.database.get_municipality_type_stats(30)
            
            logger.info(f"📊 Получено {len(municipality_stats)} записей статистики по муниципалитетам")
            logger.info(f"📊 Получено {len(municipality_type_stats)} записей по типам обращений")
            
            # Создаем все графики
            charts = await self._generate_all_charts(stats, municipality_stats, municipality_type_stats)
            
            if not charts:
                await update.message.reply_text("❌ Недостаточно данных для построения графиков.")
                return

            # Отправляем графики
            for chart_data in charts:
                try:
                    photo_buffer = io.BytesIO()
                    chart_data['figure'].savefig(photo_buffer, format='PNG', dpi=100, bbox_inches='tight')
                    photo_buffer.seek(0)
                    
                    await update.message.reply_photo(
                        photo=photo_buffer,
                        caption=chart_data['caption']
                    )
                    
                    # Закрываем figure для освобождения памяти
                    plt.close(chart_data['figure'])
                    
                except Exception as e:
                    logger.error(f"❌ Ошибка отправки графика: {e}")
                    continue
                    
        except Exception as e:
            logger.error(f"❌ Ошибка генерации графиков: {e}")
            await update.message.reply_text("❌ Ошибка при генерации графиков.")

    async def _generate_all_charts(self, stats, municipality_stats, municipality_type_stats):
        """Генерация всех графиков включая муниципалитеты"""
        charts = []
        
        try:
            # 1. Обычные графики
            regular_charts = await self._generate_charts(stats)
            charts.extend(regular_charts)
            
            # 2. Графики по муниципалитетам, если есть данные
            if municipality_stats and any(m['municipality'] != 'Не указан' for m in municipality_stats):
                municipality_charts = self._generate_municipality_charts(municipality_stats, municipality_type_stats)
                charts.extend(municipality_charts)
            else:
                # График с информацией о недостатке данных
                fig, ax = plt.subplots(figsize=(10, 6))
                ax.text(0.5, 0.5, 'Недостаточно данных по муниципалитетам\n\nДля отображения графиков необходимо:\n1. Создать обращения с указанием населенных пунктов\n2. Убедиться, что бот корректно определяет муниципалитеты', 
                        ha='center', va='center', transform=ax.transAxes, fontsize=12, 
                        bbox=dict(boxstyle="round,pad=0.3", facecolor="lightblue", alpha=0.7))
                ax.set_title('Данные по муниципалитетам отсутствуют', fontsize=14, pad=20)
                ax.set_xticks([])
                ax.set_yticks([])
                
                charts.append({
                    'figure': fig,
                    'caption': "ℹ️ Для отображения графиков по муниципалитетам нужны данные"
                })
            
        except Exception as e:
            logger.error(f"❌ Ошибка создания всех графиков: {e}")
            
        return charts

    def _generate_municipality_charts(self, stats, type_stats):
        """Генерация графиков по муниципалитетам"""
        charts = []
        
        try:
            logger.info(f"🔄 Начинаем генерацию графиков по муниципалитетам. stats: {len(stats)}")
            
            # Фильтруем статистику - убираем "Не указан"
            valid_stats = [m for m in stats if m['municipality'] != 'Не указан']
            logger.info(f"📊 Валидных муниципалитетов: {len(valid_stats)}")
            
            if not valid_stats:
                return charts

            # 1. Столбчатая диаграмма по количеству обращений
            logger.info("📊 Создаем столбчатую диаграмму...")
            bar_chart = self._create_municipality_bar_chart(valid_stats)
            if bar_chart:
                charts.append({
                    'figure': bar_chart,
                    'caption': "📊 Количество обращений по муниципалитетам (топ-10)"
                })

            # 2. Круговая диаграмма распределения обращений
            logger.info("📈 Создаем круговую диаграмму...")
            pie_chart = self._create_municipality_pie_chart(valid_stats)
            if pie_chart:
                charts.append({
                    'figure': pie_chart,
                    'caption': "📈 Распределение обращений по муниципалитетам"
                })

            # 3. Heatmap по типам обращений в муниципалитетах
            if type_stats:
                logger.info("🔥 Создаем тепловую карту...")
                heatmap = self._create_municipality_heatmap(type_stats)
                if heatmap:
                    charts.append({
                        'figure': heatmap,
                        'caption': "🔥 Распределение типов обращений по муниципалитетам"
                    })
            
            logger.info(f"✅ Успешно создано {len(charts)} графиков по муниципалитетам")
                
        except Exception as e:
            logger.error(f"❌ Ошибка создания графиков по муниципалитетам: {e}")
            
        return charts

    async def _generate_charts(self, stats):
        """Генерация графиков на основе статистики"""
        charts = []
        
        try:
            # 1. Круговая диаграмма распределения по типам
            type_chart = self._create_type_pie_chart(stats)
            charts.append({
                'figure': type_chart,
                'caption': "📊 Распределение обращений по типам (за 30 дней)"
            })
            
            # 2. Столбчатая диаграмма распределения по статусам
            status_chart = self._create_status_bar_chart(stats)
            charts.append({
                'figure': status_chart,
                'caption': "📈 Распределение обращений по статусам (за 30 дней)"
            })
            
            # 3. График динамики обращений по дням
            timeline_chart = self._create_timeline_chart(stats)
            if timeline_chart:
                charts.append({
                    'figure': timeline_chart,
                    'caption': "📅 Динамика обращений по дням (за 30 дней)"
                })
            
        except Exception as e:
            logger.error(f"❌ Ошибка создания графиков: {e}")
            
        return charts

    def _create_type_pie_chart(self, stats):
        """Создание круговой диаграммы по типам обращений"""
        # Агрегируем данные по типам
        type_counts = {}
        for stat in stats:
            appeal_type = stat['type'] or 'Не определен'
            type_counts[appeal_type] = type_counts.get(appeal_type, 0) + stat['count']
        
        # Подготавливаем данные для диаграммы
        if not type_counts:
            # Создаем пустую диаграмму если нет данных
            fig, ax = plt.subplots(figsize=(10, 8))
            ax.text(0.5, 0.5, 'Нет данных', ha='center', va='center', transform=ax.transAxes)
            ax.set_title('Распределение по типам обращений')
            return fig
        
        types = list(type_counts.keys())
        counts = list(type_counts.values())
        
        # Цветовая схема
        colors = plt.cm.Set3(np.linspace(0, 1, len(types)))
        
        # Создаем диаграмму
        fig, ax = plt.subplots(figsize=(12, 8))
        wedges, texts, autotexts = ax.pie(
            counts, 
            labels=types, 
            colors=colors,
            autopct='%1.1f%%',
            startangle=90,
            textprops={'fontsize': 10}
        )
        
        # Улучшаем отображение процентов
        for autotext in autotexts:
            autotext.set_color('white')
            autotext.set_fontweight('bold')
        
        ax.set_title('Распределение обращений по типам', fontsize=16, fontweight='bold', pad=20)
        plt.tight_layout()
        
        return fig

    def _create_status_bar_chart(self, stats):
        """Создание цветной столбчатой диаграммы по статусам обращений"""
        # Агрегируем данные по статусам
        status_counts = {}
        for stat in stats:
            status = stat['status'] or 'не определен'
            status_counts[status] = status_counts.get(status, 0) + stat['count']
        
        # Подготавливаем данные для диаграммы
        if not status_counts:
            # Создаем пустую диаграмму если нет данных
            fig, ax = plt.subplots(figsize=(10, 6))
            ax.text(0.5, 0.5, 'Нет данных', ha='center', va='center', transform=ax.transAxes, fontsize=16)
            ax.set_title('Статусы обращений', fontsize=18, fontweight='bold', pad=20)
            ax.set_xticks([])
            ax.set_yticks([])
            return fig
        
        statuses = list(status_counts.keys())
        counts = list(status_counts.values())
        
        # Яркая цветовая палитра для статусов
        status_colors = {
            'новое': '#FF6B6B',        # Ярко-красный
            'отвечено': '#4ECDC4',      # Бирюзовый
            'в работе': '#45B7D1',      # Голубой
            'требует проверки': '#FFA07A',  # Светло-коралловый
            'закрыто': '#98D8C8',       # Мятный
            'не определен': '#F7DC6F'   # Желтый
        }
        
        # Создаем цвета для каждого статуса
        colors = [status_colors.get(status, '#BB8FCE') for status in statuses]
        
        # Создаем диаграмму с улучшенным дизайном
        plt.style.use('seaborn-v0_8-darkgrid')
        fig, ax = plt.subplots(figsize=(14, 8))
        
        # Создаем столбцы с градиентом и тенью
        bars = ax.bar(statuses, counts, color=colors, edgecolor='white', linewidth=2, 
                    alpha=0.85, zorder=3)
        
        # Добавляем градиент к столбцам
        for bar, color in zip(bars, colors):
            bar.set_color(color)
            # Добавляем легкий градиент
            gradient = np.linspace(0.85, 1.0, 100).reshape(1, -1)
            gradient = np.vstack((gradient, gradient))
            bar.set_zorder(4)
        
        # Добавляем значения на столбцы с улучшенным стилем
        for bar, count in zip(bars, counts):
            height = bar.get_height()
            ax.text(bar.get_x() + bar.get_width()/2., height + 0.1,
                f'{count}', ha='center', va='bottom', 
                fontweight='bold', fontsize=14, color='#2C3E50',
                bbox=dict(boxstyle="round,pad=0.3", facecolor='white', alpha=0.8, edgecolor='none'))
        
        # Настраиваем оси и заголовок
        ax.set_ylabel('Количество обращений', fontsize=14, fontweight='bold', color='#2C3E50')
        ax.set_xlabel('Статусы обращений', fontsize=14, fontweight='bold', color='#2C3E50')
        ax.set_title('Распределение обращений по статусам', 
                    fontsize=18, fontweight='bold', pad=25, color='#2C3E50')
        
        # Улучшаем отображение подписей
        plt.xticks(rotation=15, ha='right', fontsize=12, fontweight='bold')
        plt.yticks(fontsize=12)
        
        # Добавляем сетку
        ax.grid(True, axis='y', alpha=0.3, zorder=0)
        ax.grid(True, axis='x', alpha=0.1, zorder=0)
        ax.set_axisbelow(True)
        
        # Устанавливаем цвет фона
        ax.set_facecolor('#F8F9F9')
        fig.patch.set_facecolor('#FFFFFF')
        
        # Добавляем легкую тень вокруг диаграммы
        for spine in ax.spines.values():
            spine.set_color('#BDC3C7')
            spine.set_linewidth(1.5)
        
        # Настраиваем пределы осей для лучшего отображения
        ax.set_ylim(0, max(counts) * 1.15)
        
        # Добавляем легкий градиентный фон
        ax.imshow([[0, 0], [1, 1]], cmap=plt.cm.Blues, extent=ax.get_xlim() + ax.get_ylim(), 
                alpha=0.02, aspect='auto', zorder=1)
        
        plt.tight_layout()
        
        return fig

    def _create_timeline_chart(self, stats):
        """Создание графика динамики обращений по дням"""
        try:
            # Получаем более детальные данные для временной шкалы
            appeals = self.system.database.get_appeals({
                'date_from': datetime.now() - timedelta(days=30)
            }, limit=1000)
            
            if not appeals:
                return None
            
            # Группируем по дням
            daily_counts = {}
            for appeal in appeals:
                if isinstance(appeal['created_at'], datetime):
                    date_key = appeal['created_at'].date()
                else:
                    # Если это строка, преобразуем в datetime
                    date_key = datetime.strptime(appeal['created_at'], '%Y-%m-%d %H:%M:%S').date()
                
                daily_counts[date_key] = daily_counts.get(date_key, 0) + 1
            
            # Сортируем по дате
            dates = sorted(daily_counts.keys())
            counts = [daily_counts[date] for date in dates]
            
            # Форматируем даты для подписей
            date_labels = [date.strftime('%d.%m') for date in dates]
            
            # Создаем график
            fig, ax = plt.subplots(figsize=(14, 6))
            ax.plot(date_labels, counts, marker='o', linewidth=2, markersize=6, color='#FF6384')
            ax.fill_between(date_labels, counts, alpha=0.3, color='#FF6384')
            
            ax.set_ylabel('Количество обращений', fontsize=12)
            ax.set_xlabel('Дата', fontsize=12)
            ax.set_title('Динамика обращений по дням', fontsize=16, fontweight='bold')
            
            # Поворачиваем подписи дат
            plt.xticks(rotation=45, ha='right')
            
            # Добавляем сетку
            ax.grid(True, alpha=0.3)
            
            plt.tight_layout()
            return fig
            
        except Exception as e:
            logger.error(f"❌ Ошибка создания графика динамики: {e}")
            return None

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
                response += f"   🏛️ Муниципалитет: {appeal.get('district', 'не указан')}\n"
                response += f"   ⏰ {created_time}\n\n"
            
            response += f"🔄 Автоматически обновляется при запросе"
            
            await update.message.reply_text(response, parse_mode='Markdown')
            
        except Exception as e:
            logger.error(f"❌ Ошибка получения обращений: {e}")
            await update.message.reply_text("❌ Ошибка при получении обращений.")

    async def refresh_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Принудительное обновление данных"""
        try:
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
*/charts* - Графики и диаграммы
*/refresh* - Принудительное обновление данных
*/help* - Эта справка

🏛️ *Статистика по муниципалитетам:*
• Топ-10 муниципалитетов по обращениям
• Процент ответов по муниципалитетам
• Распределение типов обращений
• Тепловые карты активности

📊 *Графики включают:*
• Распределение по типам обращений
• Распределение по статусам
• Динамику обращений по дням
• Статистику по муниципалитетам

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
            'новое': '🆕',
            'отвечено': '✅',
            'в работе': '🔄',
            'требует проверки': '👨‍💼',
            'закрыто': '🔒'
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
        elif text == '📊 Графики':
            await self.show_charts(update, context)  # Теперь включает муниципалитеты
        elif text == '🔄 Обновить':
            await self.refresh_command(update, context)
        elif text == 'ℹ️ Помощь':
            await self.help_command(update, context)

    def setup_handlers(self):
        """Настройка обработчиков - УДАЛЕНЫ муниципальные команды"""
        self.application.add_handler(CommandHandler("start", self.start_command))
        self.application.add_handler(CommandHandler("stats", self.show_stats))
        self.application.add_handler(CommandHandler("trends", self.show_trends))
        self.application.add_handler(CommandHandler("appeals", self.show_recent_appeals))
        self.application.add_handler(CommandHandler("charts", self.show_charts))  # Теперь включает всё
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