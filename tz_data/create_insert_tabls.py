

from create_base import new_base
from object_database import ObjectDataBaseConnect, decorator_catching_errors
from data_filler import make_data, make_documents

# создаем таблицы и заполняем сгенерированными значениями


# данные для базы:
data = make_data()
data_tbl = list(data.values())
documents_tbl = make_documents(data)

@decorator_catching_errors
def create_table(base: str):
    with ObjectDataBaseConnect(base) as db:
        db.execute("""
            CREATE TABLE IF NOT EXISTS public.data
            (
                object character varying(50) COLLATE pg_catalog."default" NOT NULL,
                status integer,
                level integer,
                parent character varying COLLATE pg_catalog."default",
                owner character varying(14) COLLATE pg_catalog."default",
                CONSTRAINT data_pkey PRIMARY KEY (object)
            )
        """)

        db.execute("""
            CREATE TABLE IF NOT EXISTS public.documents
            (
                doc_id character varying COLLATE pg_catalog."default" NOT NULL,
                recieved_at timestamp without time zone,
                document_type character varying COLLATE pg_catalog."default",
                document_data jsonb,
                processed_at timestamp without time zone,
                CONSTRAINT documents_pkey PRIMARY KEY (doc_id)
            )
        """)
        print('Таблицы созданы')

@decorator_catching_errors    
def insert(base: str, data: list[dict], document: list[dict]):

    insert_data = [(i['object'], i['status'], i['level'], i['parent'], i['owner'])  for i in data]
    insert_doc = [(i['doc_id'], i['recieved_at'], i['document_type'], i['document_data'])  for i in document]

    with ObjectDataBaseConnect(base) as db:

        db.execute("""
            INSERT INTO data(object, status, level, parent, owner)
            VALUES (%s, %s, %s, %s, %s) ON CONFLICT (object) DO NOTHING
            """, insert_data, ext_many=True)
        
        
        db.execute("""
            INSERT INTO documents(doc_id, recieved_at, document_type, document_data)
            VALUES (%s, %s, %s, %s) ON CONFLICT (doc_id) DO NOTHING
            """, insert_doc, ext_many=True)
        
        print('Данные вставлены')
        

if __name__ == '__main__':
    create_table(new_base)
    insert(new_base, data_tbl, documents_tbl)
    


