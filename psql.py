import psycopg2
from functools import wraps
from config import config
table = 'car_data'


# postgres декоратор для обработки ошибок, открытия и закрытия коннекта
def postgres_decorator(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        # подключение к БД
        conn = psycopg2.connect(host=config.host, dbname=config.dbname, user=config.user, password=config.password)
        conn.autocommit = True
        # исполнение sql запроса
        try:
            with conn.cursor() as cursor:
                result = func(cursor, *args, **kwargs)
                return result
        except Exception as e:
            print("[INFO] Postgres Error:", e)
        # закрытие
        finally:
            conn.close() if conn else None
    return wrapper


@postgres_decorator
def create_table(cursor):
    create_table_query = '''
    CREATE TABLE IF NOT EXISTS car_data (
        id SERIAL PRIMARY KEY,
        model VARCHAR(255),
        image_link TEXT,
        upload_date TIMESTAMP,
        details TEXT,
        plate_text VARCHAR(255),
        plate_png TEXT,
        tag VARCHAR(255),
        country VARCHAR(255),
        user_name VARCHAR(255),
        uuid INTEGER
    );'''
    cursor.execute(create_table_query)
    print(f"[INFO] {table} Table created")


@postgres_decorator
def insert_data(cursor, data_list):
    print('inserting data')
    insert_query = '''
     INSERT INTO car_data (model, image_link, upload_date, details, plate_text, plate_png, tag, country, user_name, uuid)
     VALUES (%(model)s, %(image)s, %(date)s, %(details)s, %(txt)s, %(png)s, %(tag)s, %(loc)s, %(user)s, %(uuid)s);
     '''
    cursor.executemany(insert_query, data_list)
    print('inserted rows:', len(data_list))

# посчитать уникальные
@postgres_decorator
def get_info(cursor):
    cursor.execute(f"SELECT image_link FROM {table}")

    row = cursor.fetchall()
    print(len(row))
    print(len(set(row)))
    # for i in row:
    #     print(i)

# убрать дубликаты
@postgres_decorator
def rm_dub(cursor):
    get_info()
    print('removing')
    delete_query = f"""
        DELETE FROM {table}
        WHERE id IN (
            SELECT id
            FROM (
                SELECT id,
                       ROW_NUMBER() OVER (PARTITION BY image_link ORDER BY id) AS rnum
                FROM {table}
            ) t
            WHERE t.rnum > 1
        );
    """
    cursor.execute(delete_query)
    print('removed')
    get_info()


if __name__ == '__main__':
    pass
