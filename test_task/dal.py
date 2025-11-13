
from object_database_connect import ObjectDataBaseConnect


def indexing(db: 'ObjectDataBaseConnect'):
    """Индексация полей для быстрого доступа без блокировки таблиц (для больших таблиц)"""
    # Список индексов для создания 
    # CREATE INDEX CONCURRENTLY - если таблица очень большая не блокирует запись 
    sql = [
        # Таблица documents
        'CREATE INDEX IF NOT EXISTS idx_documents_3_fields ON documents(processed_at, document_type, recieved_at)',

        # Таблица data
        'CREATE INDEX IF NOT EXISTS idx_data_parent ON data USING HASH (parent)',
        'CREATE INDEX IF NOT EXISTS idx_data_status_owner ON data(status, owner)'
        ]
    
    # Выполняем каждый индекс отдельно
    for query in sql:
        db.execute(query)


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


def parsing_data(row: list):
    """Разбираем картеж на doc_id, json, разбирам json на objects, operation_details"""
    doc_id, jsonb = row
    obj = jsonb['objects']
    operation_details = jsonb['operation_details']

    return doc_id, obj, operation_details


def search_all_child(db: 'ObjectDataBaseConnect', parent: list):
    """Поиск дочерних объектов"""

    placeholders = ', '.join('%s' for i in range(len(parent)))

    sql = f"SELECT object FROM data WHERE parent IN ({placeholders})"

    child = set(row[0] for row in db.select(sql, tuple(parent), fetch_all=True))

    parent_child = list(child)
    parent_child.extend(parent)
    return parent_child


def correct_data(db: 'ObjectDataBaseConnect', all_parand_child: list, operation_details: dict[str, dict]):
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


def set_processing_time(db: 'ObjectDataBaseConnect', doc_id: str):
    """Установка времнеи обработки документа"""
    sql = """
            UPDATE documents
            SET processed_at = NOW()
            WHERE doc_id = %s
            """

    db.execute(sql, (doc_id,))


def process_single_document(db:'ObjectDataBaseConnect'):
            
    row = select_one_doc(db)
    if not row:
        return None

    doc_id, object, operation_details = parsing_data(row)
    all_parand_child = search_all_child(db, object)

    correct_data(db, all_parand_child, operation_details)
    set_processing_time(db, doc_id)

    return row
