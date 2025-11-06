from new_base import base
from class_conect_DB import DBconnect
from data_filler import make_data, make_documents

data = make_data()
# данные для базы:
data_tbl = list(data.values())
documents_tbl = make_documents(data)

def create_table(base: str):

    with DBconnect(base) as db:

        db.execute("""
        CREATE TABLE IF NOT EXISTS public.data(
                 object character varying(50) COLLATE pg_catalog."default" NOT NULL,
                 status integer,
                 level integer,
                 parent character varying COLLATE pg_catalog."default",
                 owner character varying(14) COLLATE pg_catalog."default",
                 CONSTRAINT data_pkey PRIMARY KEY (object))
        """)

        db.execute("""
        CREATE TABLE IF NOT EXISTS public.documents(
               doc_id character varying COLLATE pg_catalog."default" NOT NULL,
               recieved_at timestamp without time zone,
               document_type character varying COLLATE pg_catalog."default",
               document_data jsonb,
               processed_at timestamp without time zone,
               CONSTRAINT documents_pkey PRIMARY KEY (doc_id))
        """)

        print('таблицы созданы')



def insert(data: list[dict], documents: list[dict], base: str):

    with DBconnect(base) as db:

        data_val = [(i['object'], i['status'], i['owner'], i['level'], i['parent']) for i in data]
        doc_val = [(i['doc_id'], i['recieved_at'], i['document_type'], i['document_data']) for i in documents]


        db.execute("""
        INSERT INTO data(object, status, owner, level, parent)
        VALUES (%s,%s,%s,%s,%s) ON CONFLICT (object) DO NOTHING
        """, data_val, ex_many=True)

        db.execute("""
        INSERT INTO documents(doc_id, recieved_at, document_type, document_data)
        VALUES (%s,%s,%s,%s) ON CONFLICT (doc_id) DO NOTHING
        """, doc_val, ex_many=True)

        print('таблицы заполнены')



if __name__ == '__main__':
    create_table(base)
    insert(data_tbl, documents_tbl, base)



