from object_database_connect import ObjectDataBaseConnect
from decorator_catching_error import decorator_catching_errors


 
@decorator_catching_errors
def select_one_doc(db: 'ObjectDataBaseConnect'):
    """SQL запрсо берет один не обработаный документ """
    sql = """
            SELECT doc_id, document_data FROM documents
            WHERE processed_at is NULL AND document_type = 'transfer_document'
            ORDER BY recieved_at ASC
            LIMIT 1      
            """
    row = db.select(sql)

    return row

@decorator_catching_errors
def parsing_data(db: 'ObjectDataBaseConnect', row: list):
    """Разбираем картеж на doc_id, json, разбирам json на objects, operation_details"""
    doc_id, jsonb = row
    obj = jsonb['objects']
    operation_details = jsonb['operation_details']

    return doc_id, obj, operation_details

@decorator_catching_errors
def search_all_child(db: 'ObjectDataBaseConnect', object_in_json: list):
    """Поиск дочерних обьектов"""
    sql = """SELECT object FROM data WHERE parent = ANY(%s)"""

    child = set(i[0] for i in db.select(sql, (object_in_json,), fetch_all=True))
    parand_child = list(child) + object_in_json

    return parand_child

@decorator_catching_errors
def correct_data(db: 'ObjectDataBaseConnect', all_parand_child: list, operation_details: dict[str: dict]):
    """Изменения старых значений на новые"""

    if operation_details:

        for operation, details in operation_details.items():
            new = details['new']
            old = details['old']

            db.execute(f""" 
                UPDATE data
                SET {operation} = %s
                WHERE {operation} = %s
                AND object = ANY(%s)
                """, (new, old, (all_parand_child,)))

@decorator_catching_errors
def set_processing_time(db: 'ObjectDataBaseConnect', doc_id: str):
    """Установка времнеи обработки документа"""
    sql = """
            UPDATE documents
            SET processed_at = NOW()
            WHERE doc_id = %s
            """

    db.execute(sql, (doc_id,))

    




