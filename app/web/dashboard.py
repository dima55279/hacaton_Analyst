from flask import Flask, render_template, jsonify, request
import json
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

def create_dashboard_app(appeals_system):
    app = Flask(__name__)
    system = appeals_system

    @app.route('/')
    def index():
        logger.info("🌐 Запрос главной страницы дашборда")
        return render_template('dashboard.html')

    @app.route('/api/stats')
    def get_stats():
        try:
            period = request.args.get('period', 30, type=int)
            logger.info(f"📊 Запрос статистики за {period} дней")
            stats = system.database.get_appeals_stats(period)
            
            # Если статистика пустая, возвращаем тестовые данные для демонстрации
            if not stats:
                logger.warning("📊 Статистика пустая, возвращаем демо-данные")
                stats = [
                    {'type': 'жалоба на ЖКХ', 'status': 'new', 'count': 5},
                    {'type': 'предложение по благоустройству', 'status': 'answered', 'count': 3},
                    {'type': 'запрос информации', 'status': 'in_progress', 'count': 2},
                    {'type': 'жалоба на дороги', 'status': 'new', 'count': 4},
                    {'type': 'другое', 'status': 'requires_manual_review', 'count': 1}
                ]
            
            logger.info(f"📊 Возвращаем статистику: {len(stats)} записей")
            return jsonify(stats)
            
        except Exception as e:
            logger.error(f"❌ Ошибка получения статистики: {e}")
            return jsonify({"error": "Ошибка получения статистики"}), 500

    @app.route('/api/trends')
    def get_trends():
        try:
            period = request.args.get('period', 30, type=int)
            logger.info(f"📈 Запрос трендов за {period} дней")
            trends = system.get_analytics(period)
            
            # Если тренды пустые, возвращаем демо-данные
            if not trends:
                logger.warning("📈 Тренды пустые, возвращаем демо-данные")
                trends = {
                    'period_days': period,
                    'total_appeals': 15,
                    'type_distribution': {
                        'жалоба на ЖКХ': 5,
                        'предложение по благоустройству': 3,
                        'запрос информации': 2,
                        'жалоба на дороги': 4,
                        'другое': 1
                    },
                    'common_themes': [
                        {'theme': 'ремонт дорог', 'frequency': 'высокая'},
                        {'theme': 'уборка территорий', 'frequency': 'средняя'},
                        {'theme': 'освещение улиц', 'frequency': 'низкая'}
                    ],
                    'response_rate': 60.0,
                    'last_updated': datetime.now().isoformat()
                }
            
            logger.info(f"📈 Возвращаем тренды: {trends['total_appeals']} обращений")
            return jsonify(trends)
            
        except Exception as e:
            logger.error(f"❌ Ошибка получения трендов: {e}")
            return jsonify({"error": "Ошибка получения трендов"}), 500

    @app.route('/api/appeals')
    def get_appeals():
        try:
            page = request.args.get('page', 1, type=int)
            limit = request.args.get('limit', 10, type=int)
            offset = (page - 1) * limit
            
            filters = {}
            if 'type' in request.args:
                filters['type'] = request.args.get('type')
            if 'status' in request.args:
                filters['status'] = request.args.get('status')
            
            logger.info(f"📝 Запрос обращений (страница {page}, лимит {limit})")
            appeals = system.database.get_appeals(filters, limit, offset)
            
            # Если обращения пустые, возвращаем демо-данные
            if not appeals:
                logger.warning("📝 Обращения пустые, возвращаем демо-данные")
                from datetime import datetime, timedelta
                appeals = [
                    {
                        'id': 1,
                        'user_id': 'demo_user_1',
                        'text': 'Во дворе дома №5 по улице Ленина разбита дорога, нужен ремонт.',
                        'type': 'жалоба на дороги',
                        'status': 'new',
                        'created_at': datetime.now() - timedelta(hours=2)
                    },
                    {
                        'id': 2,
                        'user_id': 'demo_user_2',
                        'text': 'Предлагаю установить новые лавочки в парке Горького для отдыха граждан.',
                        'type': 'предложение по благоустройству',
                        'status': 'answered',
                        'response': 'Ваше предложение принято к рассмотрению. Срок рассмотрения 30 дней.',
                        'created_at': datetime.now() - timedelta(days=1)
                    },
                    {
                        'id': 3,
                        'user_id': 'demo_user_3',
                        'text': 'Как получить справку о составе семьи и какие документы нужны?',
                        'type': 'запрос информации',
                        'status': 'in_progress',
                        'created_at': datetime.now() - timedelta(days=3)
                    }
                ]
            
            logger.info(f"📝 Возвращаем обращения: {len(appeals)} записей")
            return jsonify(appeals)
            
        except Exception as e:
            logger.error(f"❌ Ошибка получения обращений: {e}")
            return jsonify({"error": "Ошибка получения обращений"}), 500

    @app.route('/api/realtime_stats')
    def get_realtime_stats():
        """Новый endpoint для получения реальной статистики"""
        try:
            logger.info("🔄 Запрос реальной статистики")
            stats = system.database.get_real_time_stats()
            
            # Если статистика пустая, возвращаем демо-данные
            if not stats:
                stats = {
                    'total': 15,
                    'status_stats': {
                        'new': 8,
                        'answered': 5,
                        'in_progress': 2
                    },
                    'type_stats': [
                        {'type': 'жалоба на ЖКХ', 'count': 5},
                        {'type': 'жалоба на дороги', 'count': 4},
                        {'type': 'предложение по благоустройству', 'count': 3},
                        {'type': 'запрос информации', 'count': 2},
                        {'type': 'другое', 'count': 1}
                    ],
                    'last_24h': 3
                }
            
            logger.info(f"🔄 Реальная статистика: всего {stats.get('total', 0)} обращений")
            return jsonify(stats)
            
        except Exception as e:
            logger.error(f"❌ Ошибка получения реальной статистики: {e}")
            return jsonify({"error": "Ошибка получения реальной статистики"}), 500

    @app.route('/api/recent_appeals')
    def get_recent_appeals():
        """Новый endpoint для получения последних обращений"""
        try:
            limit = request.args.get('limit', 10, type=int)
            logger.info(f"🆕 Запрос последних {limit} обращений")
            appeals = system.database.get_recent_appeals(limit)
            
            # Если обращения пустые, возвращаем демо-данные
            if not appeals:
                logger.warning("🆕 Последние обращения пустые, возвращаем демо-данные")
                from datetime import datetime, timedelta
                appeals = [
                    {
                        'id': 1,
                        'user_id': 'demo_user_1',
                        'text': 'Новое обращение о проблеме с дорогой',
                        'type': 'жалоба на дороги',
                        'status': 'new',
                        'created_at': datetime.now() - timedelta(hours=1)
                    },
                    {
                        'id': 2,
                        'user_id': 'demo_user_2', 
                        'text': 'Предложение по улучшению парковой зоны',
                        'type': 'предложение по благоустройству',
                        'status': 'new',
                        'created_at': datetime.now() - timedelta(hours=3)
                    }
                ]
            
            logger.info(f"🆕 Возвращаем последние обращения: {len(appeals)} записей")
            return jsonify(appeals)
            
        except Exception as e:
            logger.error(f"❌ Ошибка получения последних обращений: {e}")
            return jsonify({"error": "Ошибка получения последних обращений"}), 500

    @app.route('/api/update_appeal/<int:appeal_id>', methods=['POST'])
    def update_appeal(appeal_id):
        try:
            data = request.json
            logger.info(f"✏️ Обновление обращения {appeal_id}")
            system.database.update_appeal(appeal_id, data)
            return jsonify({'status': 'success'})
        except Exception as e:
            logger.error(f"❌ Ошибка обновления обращения: {e}")
            return jsonify({"error": "Ошибка обновления обращения"}), 500

    return app