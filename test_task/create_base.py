
from object_database_connect import ObjectDataBaseConnect
from decorator_catching_error import decorator_catching_errors

@decorator_catching_errors
def creat_base(old_base: str, new_base: str):
    """Создаем базу данных под дествое задание"""

    with ObjectDataBaseConnect(old_base) as db:
        db.execute(f"CREATE DATABASE {new_base}")
        print(f'База {new_base} успешно создана')



old_base = 'postgres'
new_base = 'test'

if __name__ == '__main__':  
    creat_base(old_base, new_base)
    