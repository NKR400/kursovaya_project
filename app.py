from flask import Flask, render_template, request, jsonify, flash, redirect, url_for
from database import db, init_db
from database import (
    get_all_complaints,
    add_new_complaint,
    get_products,
    get_reasons,
    get_dashboard_stats,
    get_complaints_by_reason,
    get_complaints_by_month
)
from etl import run_etl, import_from_csv
import plotly
import plotly.express as px
import json
import os
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
import pandas as pd

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'postgresql://postgres:9009@localhost/complaints_db'
app.config['SECRET_KEY'] = 'dev-secret-key'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

# Инициализация БД
db.init_app(app)


@app.route('/')
def index():
    """Главная страница"""
    stats = get_dashboard_stats()
    recent_complaints = get_all_complaints(limit=10)
    return render_template('index.html',
                           stats=stats,
                           complaints=recent_complaints)


@app.route('/add', methods=['GET', 'POST'])
def add_complaint():
    """Добавить новую рекламацию"""
    if request.method == 'POST':
        # Получаем данные из формы
        product_id = request.form.get('product_id')
        reason_id = request.form.get('reason_id')
        customer_name = request.form.get('customer_name')
        description = request.form.get('description')

        # Создаем номер рекламации
        from datetime import datetime
        complaint_number = f"CMP-{datetime.now().strftime('%Y%m%d-%H%M%S')}"

        # Сохраняем в БД
        success = add_new_complaint(
            complaint_number=complaint_number,
            product_id=product_id,
            reason_id=reason_id,
            customer_name=customer_name,
            description=description
        )

        if success:
            flash('Рекламация успешно добавлена!', 'success')
            return redirect(url_for('index'))
        else:
            flash('Ошибка при добавлении рекламации', 'error')

    # Для GET запроса показываем форму
    products = get_products()
    reasons = get_reasons()
    return render_template('add.html', products=products, reasons=reasons)


@app.route('/dashboard')
def dashboard():
    """Дашборд с аналитикой"""
    return render_template('dashboard.html')


@app.route('/api/complaints')
def api_complaints():
    """API для получения рекламаций"""
    complaints = get_all_complaints()
    return jsonify([{
        'id': c.id,
        'number': c.complaint_number,
        'product': c.product_id,
        'reason': c.reason_id,
        'customer': c.customer_name,
        'date': c.complaint_date.strftime('%Y-%m-%d %H:%M'),
        'status': c.status
    } for c in complaints])


@app.route('/api/charts/top_reasons')
def chart_top_reasons():
    """График топ причин возвратов"""
    data = get_complaints_by_reason(limit=10)

    if not data:
        return jsonify({'error': 'Нет данных'})

    df = pd.DataFrame(data, columns=['reason_name', 'count'])
    fig = px.bar(
        df,
        x='reason_name',
        y='count',
        title='Топ причин возвратов',
        color='count',
        color_continuous_scale='blues'
    )
    fig.update_layout(
        xaxis_title='Причина возврата',
        yaxis_title='Количество',
        showlegend=False
    )

    return json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)


@app.route('/api/charts/monthly_trend')
def chart_monthly_trend():
    """График по месяцам"""
    data = get_complaints_by_month()

    if not data:
        return jsonify({'error': 'Нет данных'})

    df = pd.DataFrame(data, columns=['month', 'count'])
    fig = px.line(
        df,
        x='month',
        y='count',
        title='Динамика рекламаций по месяцам',
        markers=True
    )
    fig.update_layout(
        xaxis_title='Месяц',
        yaxis_title='Количество рекламаций'
    )

    return json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)


@app.route('/api/charts/products')
def chart_products():
    """График по продуктам"""
    try:
        from sqlalchemy import text

        # Получаем данные о рекламациях по продуктам
        query = text("""
            SELECT p.name as product_name, COUNT(c.id) as count
            FROM complaints c
            JOIN products p ON c.product_id = p.id
            GROUP BY p.name
            ORDER BY count DESC
            LIMIT 10
        """)

        from database import db
        result = db.session.execute(query)
        data = result.fetchall()

        if not data:
            # Возвращаем пустой график
            fig = px.bar(title='Нет данных по продуктам')
            fig.update_layout(
                xaxis_title='Продукт',
                yaxis_title='Количество рекламаций'
            )
            return json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)

        df = pd.DataFrame(data, columns=['product_name', 'count'])
        df = df.sort_values('count', ascending=False)

        fig = px.bar(
            df,
            x='product_name',
            y='count',
            title='Рекламации по продуктам',
            color='count',
            color_continuous_scale='reds'
        )
        fig.update_layout(
            xaxis_title='Продукт',
            yaxis_title='Количество рекламаций',
            showlegend=False,
            xaxis_tickangle=-45
        )

        return json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)

    except Exception as e:
        print(f"Ошибка в chart_products: {e}")
        fig = px.bar(title=f'Ошибка: {str(e)}')
        return json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)


@app.route('/api/stats')
def api_stats():
    """API для статистики"""
    stats = get_dashboard_stats()
    return jsonify(stats)


@app.route('/run_etl', methods=['POST'])
def run_etl_process():
    """Запуск ETL процесса"""
    try:
        # Запускаем ETL
        result = run_etl()

        # Импортируем тестовые данные из CSV если есть
        csv_added = 0
        if os.path.exists('sample_data.csv'):
            csv_added = import_from_csv('sample_data.csv')

        total_added = result + csv_added

        return jsonify({
            'status': 'success',
            'message': f'ETL выполнен успешно. Добавлено записей: {total_added}'
        })

    except IntegrityError as e:
        db.session.rollback()
        return jsonify({
            'status': 'error',
            'message': f'Ошибка целостности данных: {str(e)}'
        }), 500

    except SQLAlchemyError as e:
        db.session.rollback()
        return jsonify({
            'status': 'error',
            'message': f'Ошибка базы данных: {str(e)}'
        }), 500

    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': f'Ошибка ETL: {str(e)}'
        }), 500


@app.route('/init_db')
def init_database():
    """Инициализация БД (для первого запуска)"""
    with app.app_context():
        init_db()
        flash('База данных инициализирована!', 'success')
    return redirect(url_for('index'))


if __name__ == '__main__':
    # Создаем таблицы при первом запуске
    with app.app_context():
        db.create_all()

    app.run(debug=True, host='0.0.0.0', port=8080)
