import os
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv
import subprocess

load_dotenv()

def get_db_connection():
    return psycopg2.connect(
        host="localhost",
        port=os.getenv("DB_PORT", "5432"),
        database="dwh",
        user=os.getenv("DB_USER", "postgres"),
        password=os.getenv("DB_PASSWORD", "postgres")
    )

def analyze_data_gaps():
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        # Проверяем диапазон дат в витрине
        cursor.execute("""
            SELECT MIN(effective_from_date), MAX(effective_from_date), 
                   COUNT(DISTINCT effective_from_date) as unique_dates
            FROM dm.loan_holiday_info
        """)
        result = cursor.fetchone()
        print(f"Диапазон дат в витрине: {result[0]} - {result[1]}")
        print(f"Количество уникальных дат: {result[2]}")
        
        # Находим пропущенные даты
        cursor.execute("""
            WITH date_range AS (
                SELECT generate_series('2023-01-01'::date, '2023-12-31'::date, '1 day'::interval)::date as date
            ) 
            SELECT COUNT(*) as missing_dates
            FROM date_range 
            WHERE date NOT IN (SELECT DISTINCT effective_from_date FROM dm.loan_holiday_info)
        """)
        missing_count = cursor.fetchone()[0]
        print(f"Количество пропущенных дат: {missing_count}")
        
        # Проверяем данные в источниках
        cursor.execute("""
            SELECT 'deal_info' as table_name, 
                   MIN(effective_from_date) as min_date, 
                   MAX(effective_from_date) as max_date,
                   COUNT(DISTINCT effective_from_date) as unique_dates
            FROM rd.deal_info
            UNION ALL
            SELECT 'loan_holiday' as table_name, 
                   MIN(effective_from_date) as min_date, 
                   MAX(effective_from_date) as max_date,
                   COUNT(DISTINCT effective_from_date) as unique_dates
            FROM rd.loan_holiday
            UNION ALL
            SELECT 'product' as table_name, 
                   MIN(effective_from_date) as min_date, 
                   MAX(effective_from_date) as max_date,
                   COUNT(DISTINCT effective_from_date) as unique_dates
            FROM rd.product
        """)
        
        print("\nДанные в таблицах-источниках:")
        for row in cursor.fetchall():
            print(f"  {row[0]}: {row[1]} - {row[2]} ({row[3]} уникальных дат)")
            
    finally:
        cursor.close()
        conn.close()

def load_csv_to_table(csv_file, table_name, schema="rd"):
    print(f"\nЗагружаю данные из {csv_file} в {schema}.{table_name}")
    
    # Читаем CSV файл с правильной кодировкой
    df = pd.read_csv(csv_file, encoding='cp1251')
    print(f"Прочитано {len(df)} строк из {csv_file}")
    
    # Подключаемся к базе данных
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        # Очищаем таблицу перед загрузкой
        cursor.execute(f"DELETE FROM {schema}.{table_name}")
        print(f"Очищена таблица {schema}.{table_name}")
        
        # Подготавливаем данные для вставки
        columns = df.columns.tolist()
        values = [tuple(row) for row in df.values]
        
        # Создаем SQL запрос для вставки
        placeholders = ','.join(['%s'] * len(columns))
        insert_query = f"INSERT INTO {schema}.{table_name} ({','.join(columns)}) VALUES ({placeholders})"
        
        # Вставляем данные
        cursor.executemany(insert_query, values)
        
        # Подтверждаем изменения
        conn.commit()
        print(f"Успешно загружено {len(df)} строк в {schema}.{table_name}")
        
    except Exception as e:
        conn.rollback()
        print(f"Ошибка при загрузке данных: {e}")
        raise
    finally:
        cursor.close()
        conn.close()

def execute_sql_file(sql_file):
    print(f"\nВыполняю SQL файл: {sql_file}")
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        with open(sql_file, 'r') as f:
            sql_content = f.read()
        
        cursor.execute(sql_content)
        conn.commit()
        print(f"SQL файл {sql_file} выполнен успешно")
        
    except Exception as e:
        conn.rollback()
        print(f"Ошибка при выполнении SQL файла: {e}")
        raise
    finally:
        cursor.close()
        conn.close()

def refresh_mart():
    print("\n=== ПЕРЕСЧЕТ ВИТРИНЫ ===")
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        cursor.execute("CALL refresh_loan_holiday_info()")
        conn.commit()
        print("Витрина loan_holiday_info успешно пересчитана")
        
        # Проверяем результат
        cursor.execute("SELECT COUNT(*) FROM dm.loan_holiday_info")
        count = cursor.fetchone()[0]
        print(f"Количество записей в витрине после пересчета: {count}")
        
    except Exception as e:
        conn.rollback()
        print(f"Ошибка при пересчете витрины: {e}")
        raise
    finally:
        cursor.close()
        conn.close()

def main():
    print("Анализ витрины loan_holiday_info и загрузка недостающих данных")
    
    # Шаг 1: Анализируем текущее состояние данных
    analyze_data_gaps()
    
    # Шаг 2: Загружаем новые данные из CSV файлов
    print("\n=== ЗАГРУЗКА НОВЫХ ДАННЫХ ===")
    
    # Загружаем данные о сделках
    load_csv_to_table(
        "data/loan_holiday_info/deal_info.csv", 
        "deal_info"
    )
    
    # Загружаем данные о продуктах
    load_csv_to_table(
        "data/loan_holiday_info/product_info.csv", 
        "product"
    )
    
    # Шаг 3: Создаем процедуру для пересчета витрины
    print("\n=== СОЗДАНИЕ ПРОЦЕДУРЫ ПЕРЕСЧЕТА ===")
    execute_sql_file("refresh_loan_holiday_info.sql")
    
    # Шаг 4: Пересчитываем витрину
    refresh_mart()
    
    # Шаг 5: Финальный анализ
    print("\n=== ФИНАЛЬНЫЙ АНАЛИЗ ===")
    analyze_data_gaps()

if __name__ == "__main__":
    main() 