import json
import logging
from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes

logger = logging.getLogger(__name__)

class KnowledgeBase:
    def __init__(self, knowledge_file='knowledge_base.json'):
        self.knowledge_file = knowledge_file
        self.data = self.load_knowledge_base()
    
    def load_knowledge_base(self):
        """Загрузка базы знаний из JSON файла"""
        try:
            with open(self.knowledge_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"❌ Ошибка загрузки базы знаний: {e}")
            return {"knowledge_base": {"categories": []}}
    
    def get_categories_keyboard(self):
        """Клавиатура с категориями"""
        categories = self.data['knowledge_base']['categories']
        keyboard = []
        
        for category in categories:
            keyboard.append([
                InlineKeyboardButton(
                    category['name'], 
                    callback_data=f"category_{category['id']}"
                )
            ])
        
        # Добавляем кнопку возврата в главное меню
        keyboard.append([
            InlineKeyboardButton("🔙 Главное меню", callback_data="main_menu")
        ])
        
        return InlineKeyboardMarkup(keyboard)
    
    def get_questions_keyboard(self, category_id):
        """Клавиатура с вопросами для категории"""
        category = self.get_category_by_id(category_id)
        if not category:
            return None
        
        keyboard = []
        for question in category['questions']:
            # Обрезаем длинные вопросы для кнопки
            question_text = question['question']
            if len(question_text) > 35:
                question_text = question_text[:35] + "..."
            
            keyboard.append([
                InlineKeyboardButton(
                    f"❓ {question_text}",
                    callback_data=f"question_{question['id']}"
                )
            ])
        
        # Кнопки навигации
        keyboard.append([
            InlineKeyboardButton("🔙 Назад к категориям", callback_data="back_to_categories"),
            InlineKeyboardButton("🔙 Главное меню", callback_data="main_menu")
        ])
        
        return InlineKeyboardMarkup(keyboard)
    
    def get_category_by_id(self, category_id):
        """Получение категории по ID"""
        for category in self.data['knowledge_base']['categories']:
            if category['id'] == category_id:
                return category
        return None
    
    def get_question_by_id(self, question_id):
        """Получение вопроса по ID"""
        for category in self.data['knowledge_base']['categories']:
            for question in category['questions']:
                if question['id'] == question_id:
                    return question, category
        return None, None
    
    def format_answer(self, question, category):
        """Форматирование ответа"""
        answer_text = f"""
{category['name']}

**❓ Вопрос:** {question['question']}

**💡 Ответ:** {question['answer']}

---
*Для дополнительной помощи используйте команду /help*
"""
        return answer_text
    
    def search_questions(self, search_term):
        """Поиск вопросов по ключевому слову"""
        results = []
        search_term = search_term.lower()
        
        for category in self.data['knowledge_base']['categories']:
            for question in category['questions']:
                if (search_term in question['question'].lower() or 
                    search_term in question['answer'].lower()):
                    results.append((question, category))
        
        return results

# Создаем глобальный экземпляр базы знаний
knowledge_base = KnowledgeBase()