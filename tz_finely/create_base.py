
from object_database import ObjectDataBaseConnect, decorator_catching_errors


@decorator_catching_errors
def creat_base(old_base: str, new_base: str):

    with ObjectDataBaseConnect(old_base) as db:
        db.execute(f"CREATE DATABASE {new_base}")
        print(f'База {new_base} успешно создана')



old_base = 'postgres'
new_base = 'test'
if __name__ == '__main__':  
    creat_base(old_base, new_base)
    