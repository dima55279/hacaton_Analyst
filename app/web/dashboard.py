from flask import Flask, render_template, jsonify, request
import json
from datetime import datetime, timedelta

def create_dashboard_app(appeals_system):
    app = Flask(__name__)
    system = appeals_system

    @app.route('/')
    def index():
        return render_template('dashboard.html')

    @app.route('/api/stats')
    def get_stats():
        period = request.args.get('period', 30, type=int)
        stats = system.database.get_appeals_stats(period)
        return jsonify(stats)

    @app.route('/api/trends')
    def get_trends():
        period = request.args.get('period', 30, type=int)
        trends = system.get_analytics(period)
        return jsonify(trends)

    @app.route('/api/appeals')
    def get_appeals():
        page = request.args.get('page', 1, type=int)
        limit = request.args.get('limit', 10, type=int)
        offset = (page - 1) * limit
        
        filters = {}
        if 'type' in request.args:
            filters['type'] = request.args.get('type')
        if 'status' in request.args:
            filters['status'] = request.args.get('status')
        
        appeals = system.database.get_appeals(filters, limit, offset)
        return jsonify(appeals)

    @app.route('/api/update_appeal/<int:appeal_id>', methods=['POST'])
    def update_appeal(appeal_id):
        data = request.json
        system.database.update_appeal(appeal_id, data)
        return jsonify({'status': 'success'})

    return app