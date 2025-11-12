from config import host, port, password, user
import psycopg2

# класс для соединения, выборки, втавки и измения данных

class ObjectDataBaseConnect:

    def __init__(self, base_name):

        try:
            self.connect_db = psycopg2.connect(
                user = user,
                host = host,
                port = port,
                password = password,
                database = base_name
            )

            self.connect_db.autocommit = True
        except Exception as e:
            print(f'Ошибка подключения - {e}')

    
    def select(self, sql, parms=None, fetch_all=False):
        with self.connect_db.cursor() as cur:

            cur.execute(sql, parms)
            if fetch_all:
                return cur.fetchall()
            return cur.fetchone()
            
    
    def execute(self, sql, parms=None, ext_many = False):
        """Изменение и вставка данных"""

        with self.connect_db.cursor() as cur:
            if ext_many:
                cur.executemany(sql, parms)
            else:
                cur.execute(sql, parms)
    
    def close(self):
        """Закрытие соединения"""
        self.connect_db.close()

    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        return self.close()

        
        

    
            

        
        
