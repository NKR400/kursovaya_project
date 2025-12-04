import pandas as pd
from datetime import datetime, timedelta
import random
from database import db, Product, ReturnReason, Complaint
import os
import uuid


def extract_from_csv(filepath):
    """Извлечение данных из CSV файла"""
    if not os.path.exists(filepath):
        print(f"Файл {filepath} не найден")
        return pd.DataFrame()

    try:
        df = pd.read_csv(filepath)
        print(f"Извлечено {len(df)} записей из {filepath}")
        return df
    except Exception as e:
        print(f"Ошибка при чтении CSV: {e}")
        return pd.DataFrame()


def transform_complaints(df):
    """Преобразование данных о рекламациях"""
    if df.empty:
        return df

    # Копируем данные
    df_clean = df.copy()

    # Заполняем пропуски
    df_clean['customer_name'] = df_clean.get('customer_name', 'Неизвестно')
    df_clean['description'] = df_clean.get('description', '')
    df_clean['customer_region'] = df_clean.get('customer_region', '')

    # Стандартизируем причины
    if 'return_reason' in df_clean.columns:
        reason_mapping = {
            'поврежден': 'DAMAGED',
            'брак': 'DEFECTIVE',
            'не тот товар': 'WRONG_ITEM',
            'опоздание': 'LATE_DELIVERY',
            'передумал': 'CHANGED_MIND'
        }
        df_clean['return_reason'] = df_clean['return_reason'].replace(
            reason_mapping)

    # Добавляем дату
    if 'complaint_date' not in df_clean.columns:
        df_clean['complaint_date'] = datetime.now()

    print(f"Преобразовано {len(df_clean)} записей")
    return df_clean


def generate_unique_complaint_number():
    """Генерация уникального номера рекламации"""
    timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
    unique_id = str(uuid.uuid4())[:8]  # Берем первые 8 символов UUID
    return f"CMP-{timestamp}-{unique_id}"


def load_to_database(df):
    """Загрузка данных в базу данных"""
    if df.empty:
        return 0

    count = 0
    errors = 0

    try:
        for _, row in df.iterrows():
            try:
                # Генерируем УНИКАЛЬНЫЙ номер рекламации
                complaint_number = generate_unique_complaint_number()

                # Проверяем, нет ли уже такого номера в БД
                existing = Complaint.query.filter_by(
                    complaint_number=complaint_number
                ).first()

                if existing:
                    # Если уже есть, генерируем новый
                    complaint_number = generate_unique_complaint_number()

                # Находим продукт по SKU
                product_sku = row.get('product_sku', '').strip()
                product = Product.query.filter_by(sku=product_sku).first()

                if not product:
                    print(
                        f"Продукт с SKU '{product_sku}' не найден, пропускаем")
                    errors += 1
                    continue

                # Находим причину по коду
                reason_code = row.get('return_reason', '').strip()
                reason = ReturnReason.query.filter_by(code=reason_code).first()

                if not reason:
                    print(
                        f"Причина с кодом '{reason_code}' не найдена, пропускаем")
                    errors += 1
                    continue

                # Создаем рекламацию
                complaint = Complaint(
                    complaint_number=complaint_number,
                    product_id=product.id,
                    reason_id=reason.id,
                    customer_name=row.get('customer_name', 'Импорт'),
                    customer_region=row.get('customer_region', ''),
                    description=row.get('description', ''),
                    status='new',
                    complaint_date=row.get('complaint_date', datetime.now())
                )

                db.session.add(complaint)
                count += 1

                # Коммитим каждые 10 записей
                if count % 10 == 0:
                    db.session.commit()

            except Exception as e:
                print(f"Ошибка при загрузке записи: {e}")
                errors += 1
                db.session.rollback()  # Важно: откатываем транзакцию при ошибке
                continue

        # Финальный коммит
        db.session.commit()

    except Exception as e:
        print(f"Критическая ошибка при загрузке: {e}")
        db.session.rollback()

    print(f"Загружено {count} записей, ошибок: {errors}")
    return count


def generate_sample_data(num_records=50):
    """Генерация тестовых данных"""
    products = Product.query.all()
    reasons = ReturnReason.query.all()

    if not products or not reasons:
        print("Нет данных о продуктах или причинах")
        return pd.DataFrame()

    data = []
    for i in range(num_records):
        product = random.choice(products)
        reason = random.choice(reasons)

        # Генерируем случайную дату за последние 90 дней
        days_ago = random.randint(0, 90)
        complaint_date = datetime.now() - timedelta(days=days_ago)

        data.append({
            'product_sku': product.sku,
            'return_reason': reason.code,
            'customer_name': f'Клиент {i+1}',
            'customer_region': random.choice(['Москва', 'СПб', 'Новосибирск', 'Екатеринбург', 'Казань', 'Ростов-на-Дону']),
            'description': random.choice([
                'Товар прибыл поврежденным',
                'Не работает',
                'Не соответствует описанию',
                'Доставка с задержкой',
                'Неисправность аккумулятора',
                'Отсутствует деталь',
                'Некачественная сборка'
            ]),
            'complaint_date': complaint_date
        })

    return pd.DataFrame(data)


def run_etl():
    """Запуск полного ETL процесса"""
    print("Запуск ETL процесса...")

    # Получаем текущее количество рекламаций
    initial_count = Complaint.query.count()
    print(f"Начальное количество рекламаций: {initial_count}")

    try:
        # 1. Генерируем тестовые данные
        print("Шаг 1: Генерация тестовых данных...")
        df_raw = generate_sample_data(50)  # Уменьшим до 50 для теста

        if df_raw.empty:
            print("Не удалось сгенерировать данные")
            return 0

        # 2. Преобразуем данные
        print("Шаг 2: Преобразование данных...")
        df_clean = transform_complaints(df_raw)

        # 3. Загружаем в базу
        print("Шаг 3: Загрузка в базу данных...")
        loaded_count = load_to_database(df_clean)

        # 4. Получаем итоговое количество
        final_count = Complaint.query.count()
        added_count = final_count - initial_count

        print(f"ETL завершен. Добавлено {added_count} новых записей")
        return added_count

    except Exception as e:
        print(f"Ошибка в ETL процессе: {e}")
        db.session.rollback()
        return 0


def import_from_csv(filepath):
    """Импорт данных из CSV файла"""
    try:
        df = extract_from_csv(filepath)
        if df.empty:
            return 0

        initial_count = Complaint.query.count()
        df_clean = transform_complaints(df)
        count = load_to_database(df_clean)
        final_count = Complaint.query.count()

        added_count = final_count - initial_count
        print(f"Импортировано {added_count} новых записей из {filepath}")
        return added_count

    except Exception as e:
        print(f"Ошибка при импорте CSV: {e}")
        return 0
