
from object_database_connect import ObjectDataBaseConnect



def creat_base(old_base: str, new_base: str):
    """Создаем базу данных под дествое задание"""
    try:
        db =  ObjectDataBaseConnect(old_base)
        conn = db.connect_db
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(f"CREATE DATABASE {new_base}")
            print(f'База {new_base} успешно создана')
    except Exception as e:
        print(f'Ошибка {e}')



old_base = 'postgres'
new_base = 'test'

if __name__ == '__main__':  
    creat_base(old_base, new_base)
    