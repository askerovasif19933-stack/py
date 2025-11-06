import psycopg2
from config import port, host, password, user
import functools

# для отлавливания исключений

def decor_error(func):
    @functools.wraps(func)
    def wrapper(self, *args, **kwargs):
        try:
            return func(self, *args, **kwargs)
        except Exception as e:
            print(f"Ошибка в {e}")
            return False
    return wrapper


class DBconnect:

    def __init__(self, base_name):
        try:
            self.connect_db = psycopg2.connect(
                port = port,
                host = host,
                password = password,
                user = user,
                database = base_name
            )
            self.connect_db.autocommit = True
        except Exception as e:
            print(f'ошибка подключения {e}')


    # select для запросов возвращающих результата
    @decor_error
    def select(self, sql, parms=None, ftch_all = False):
        with self.connect_db.cursor() as cur:
            cur.execute(sql, parms)
            if ftch_all:
                x = cur.fetchall()
            else:
                x = cur.fetchone()
            return x

    # для запросов не возвращающих результат
    @decor_error
    def execute(self, sql, parms=None, ex_many=False):
        with self.connect_db.cursor() as cur:
            if ex_many:
                # True для массовой вставки
                cur.executemany(sql, parms)
            else:
                cur.execute(sql, parms)

    # закрытие соединения
    def close(self):
        self.connect_db.close()

    # вход в with
    def __enter__(self):
        return self

    # выход из with
    def __exit__(self, exc_type, exc_val, exc_tb):
        return self.close()



