
import functools
from dotenv import load_dotenv
import os
import psycopg2

load_dotenv()
# класс для соединения, выборки, втавки и измения данных, декоратор для отлавливания ошибок 

def decorator_catching_errors(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            print(f'Ошибка {e}')
            return False
    return wrapper
    


class ObjectDataBaseConnect:

    def __init__(self, base_name):

        try:
            self.connect_db = psycopg2.connect(
                user = os.getenv('DB_USER'),
                host = os.getenv('DB_HOST'),
                port = os.getenv('DB_PORT'),
                password = os.getenv('DB_PASSWORD'),
                database = base_name
            )

            self.connect_db.autocommit = True
        except Exception as e:
            print(f'Ошибка подключения - {e}')

    
    def select(self, sql, parms=None, fetch_all=False):
        with self.connect_db.cursor() as cur:

            cur.execute(sql, parms)
            if fetch_all:
                fetch = cur.fetchall()
            else:
                fetch = cur.fetchone()
            return fetch
    
    def execute(self, sql, parms=None, ext_many = False):
        with self.connect_db.cursor() as cur:
            if ext_many:
                cur.executemany(sql, parms)
            else:
                cur.execute(sql, parms)
    
    def close(self):
        self.connect_db.close()

    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        return self.close()

        
        

    
            

        
        
