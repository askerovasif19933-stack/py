from config import host, password, user
from sqlalchemy import text, create_engine


engine = create_engine(f"postgresql+psycopg2://{user}:{password}@{host}/postgres")


new_base = 'tz_base'

def get_new_base():
    try:
        with engine.connect() as conn:
            conn.execute(text(f"CREATE DATABASE {new_base}"))
            conn.commit()
        print('база создана')
    except Exception as e:
        print(e)