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
            return jsonify(appeals)
        except Exception as e:
            logger.error(f"❌ Ошибка получения обращений: {e}")
            return jsonify({"error": "Ошибка получения обращений"}), 500

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