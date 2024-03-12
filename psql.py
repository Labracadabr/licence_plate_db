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
    query = '''
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
    cursor.execute(query)
    print(f"[INFO] {table} Table created")


@postgres_decorator
def insert_data(cursor, data_list):
    print('inserting data')
    query = '''
     INSERT INTO car_data (model, image_link, upload_date, details, plate_text, plate_png, tag, country, user_name, uuid)
     VALUES (%(model)s, %(image)s, %(date)s, %(details)s, %(txt)s, %(png)s, %(tag)s, %(loc)s, %(user)s, %(uuid)s);
     '''
    cursor.executemany(query, data_list)
    print('inserted rows:', len(data_list))

# посчитать уникальные
@postgres_decorator
def count_dub(cursor):
    cursor.execute(f"SELECT image_link FROM {table}")

    row = cursor.fetchall()
    print(len(row))
    print(len(set(row)))
    # for i in row:
    #     print(i)

# убрать дубликаты
@postgres_decorator
def rm_dub(cursor):
    print('removing')
    query = f"""
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
    cursor.execute(query)
    print('removed')


@postgres_decorator
def count_all(cursor) -> int:
    query = f"""
           SELECT COUNT(*)
           FROM {table};
       """
    cursor.execute(query)
    return cursor.fetchall()[0][0]


@postgres_decorator
def top_countries(cursor) -> int:
    query = f'''SELECT country, COUNT(*) AS count
            FROM {table}
            GROUP BY country
            ORDER BY count DESC
            LIMIT 300;'''

    cursor.execute(query)
    top = cursor.fetchall()
    return top


if __name__ == '__main__':
    print(psycopg2.__version__)

