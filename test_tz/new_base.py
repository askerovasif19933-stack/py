from config import db_name
from class_conect_DB import DBconnect


def create_base(old_base: str, new_base: str):
    with DBconnect(old_base) as db:
        exist = db.select("""SELECT 1 FROM pg_database WHERE datname = %s""", (new_base,))

        if not exist:
            db.execute(f"""CREATE DATABASE {new_base}""")
    print(f'база {new_base} создана')

base = 'new_base'
if __name__ == '__main__':
    create_base(db_name, base)














