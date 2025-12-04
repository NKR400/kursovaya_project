from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import text, func
from datetime import datetime
import pandas as pd

db = SQLAlchemy()

# Модели данных


class Product(db.Model):
    __tablename__ = 'products'

    id = db.Column(db.Integer, primary_key=True)
    sku = db.Column(db.String(50), nullable=False, unique=True)
    name = db.Column(db.String(100), nullable=False)
    category = db.Column(db.String(50))
    price = db.Column(db.Float)
    created_at = db.Column(db.DateTime, default=datetime.now)

    def to_dict(self):
        return {
            'id': self.id,
            'sku': self.sku,
            'name': self.name,
            'category': self.category,
            'price': self.price
        }


class ReturnReason(db.Model):
    __tablename__ = 'return_reasons'

    id = db.Column(db.Integer, primary_key=True)
    code = db.Column(db.String(20), nullable=False, unique=True)
    name = db.Column(db.String(100), nullable=False)
    severity = db.Column(db.Integer, default=1)  # 1-5
    category = db.Column(db.String(50))

    def to_dict(self):
        return {
            'id': self.id,
            'code': self.code,
            'name': self.name,
            'severity': self.severity,
            'category': self.category
        }


class Complaint(db.Model):
    __tablename__ = 'complaints'

    id = db.Column(db.Integer, primary_key=True)
    complaint_number = db.Column(db.String(50), nullable=False, unique=True)
    product_id = db.Column(db.Integer, nullable=False)
    reason_id = db.Column(db.Integer, nullable=False)
    customer_name = db.Column(db.String(100))
    customer_region = db.Column(db.String(50))
    complaint_date = db.Column(db.DateTime, default=datetime.now)
    description = db.Column(db.Text)
    # new, in_progress, resolved
    status = db.Column(db.String(20), default='new')
    created_at = db.Column(db.DateTime, default=datetime.now)

    def to_dict(self):
        return {
            'id': self.id,
            'complaint_number': self.complaint_number,
            'product_id': self.product_id,
            'reason_id': self.reason_id,
            'customer_name': self.customer_name,
            'complaint_date': self.complaint_date.strftime('%Y-%m-%d %H:%M'),
            'description': self.description,
            'status': self.status
        }

# Функции для работы с данными


def init_db():
    """Инициализация базы данных с тестовыми данными"""
    # Очищаем таблицы
    db.drop_all()
    db.create_all()

    # Добавляем тестовые продукты
    products = [
        Product(sku='SKU-1001', name='Смартфон X',
                category='Электроника', price=29999),
        Product(sku='SKU-1002', name='Ноутбук Pro',
                category='Электроника', price=89999),
        Product(sku='SKU-1003', name='Наушники Air',
                category='Аксессуары', price=7999),
        Product(sku='SKU-1004', name='Чехол для телефона',
                category='Аксессуары', price=1499),
        Product(sku='SKU-1005', name='Монитор 27"',
                category='Электроника', price=24999),
    ]

    # Добавляем причины возврата
    reasons = [
        ReturnReason(code='DAMAGED', name='Повреждение при доставке',
                     severity=3, category='Доставка'),
        ReturnReason(code='DEFECTIVE', name='Бракованный товар',
                     severity=4, category='Производство'),
        ReturnReason(code='WRONG_ITEM', name='Не тот товар',
                     severity=2, category='Склад'),
        ReturnReason(code='LATE_DELIVERY', name='Задержка доставки',
                     severity=1, category='Доставка'),
        ReturnReason(code='CHANGED_MIND', name='Передумал',
                     severity=1, category='Клиент'),
        ReturnReason(code='MISMATCH', name='Не соответствует описанию',
                     severity=2, category='Маркетинг'),
    ]

    for product in products:
        db.session.add(product)

    for reason in reasons:
        db.session.add(reason)

    db.session.commit()
    print("База данных инициализирована с тестовыми данными")


def get_all_complaints(limit=100):
    """Получить все рекламации"""
    return Complaint.query.order_by(Complaint.complaint_date.desc()).limit(limit).all()


def add_new_complaint(complaint_number, product_id, reason_id, customer_name, description):
    """Добавить новую рекламацию"""
    try:
        complaint = Complaint(
            complaint_number=complaint_number,
            product_id=product_id,
            reason_id=reason_id,
            customer_name=customer_name,
            description=description,
            status='new'
        )

        db.session.add(complaint)
        db.session.commit()
        return True
    except Exception as e:
        print(f"Ошибка при добавлении рекламации: {e}")
        db.session.rollback()
        return False


def get_products():
    """Получить список продуктов"""
    return Product.query.order_by(Product.name).all()


def get_reasons():
    """Получить список причин возврата"""
    return ReturnReason.query.order_by(ReturnReason.name).all()


def get_dashboard_stats():
    """Получить статистику для дашборда"""
    stats = {}

    # Общее количество рекламаций
    stats['total_complaints'] = Complaint.query.count()

    # Рекламации по статусам
    stats['new_complaints'] = Complaint.query.filter_by(status='new').count()
    stats['resolved_complaints'] = Complaint.query.filter_by(
        status='resolved').count()

    # Рекламации за сегодня
    from datetime import date
    today = date.today()
    stats['today_complaints'] = Complaint.query.filter(
        func.date(Complaint.complaint_date) == today
    ).count()

    # Самая частая причина
    result = db.session.execute(text("""
        SELECT r.name, COUNT(c.id) as count
        FROM complaints c
        JOIN return_reasons r ON c.reason_id = r.id
        GROUP BY r.name
        ORDER BY count DESC
        LIMIT 1
    """))

    top_reason = result.fetchone()
    if top_reason:
        stats['top_reason'] = top_reason[0]
        stats['top_reason_count'] = top_reason[1]
    else:
        stats['top_reason'] = 'Нет данных'
        stats['top_reason_count'] = 0

    return stats


def get_complaints_by_reason(limit=10):
    """Получить количество рекламаций по причинам"""
    result = db.session.execute(text("""
        SELECT r.name, COUNT(c.id) as count
        FROM complaints c
        JOIN return_reasons r ON c.reason_id = r.id
        GROUP BY r.name
        ORDER BY count DESC
        LIMIT :limit
    """), {'limit': limit})

    return result.fetchall()


def get_complaints_by_month():
    """Получить рекламации по месяцам"""
    result = db.session.execute(text("""
        SELECT 
            TO_CHAR(complaint_date, 'YYYY-MM') as month,
            COUNT(*) as count
        FROM complaints
        GROUP BY TO_CHAR(complaint_date, 'YYYY-MM')
        ORDER BY month
    """))

    return result.fetchall()
