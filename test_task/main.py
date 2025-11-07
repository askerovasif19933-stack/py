from object_database import ObjectDataBaseConnect, decorator_catching_errors
from create_base import new_base


# берем один не обработаный документ документ
@decorator_catching_errors
def changing_the_data(base: str):
    with ObjectDataBaseConnect(base) as db:

        old_val = db.select("""SELECT status, owner from data""", fetch_all=True)

        while True:

            row = db.select("""
            SELECT doc_id, document_data FROM documents
            WHERE processed_at is NULL AND document_type = 'transfer_document'
            ORDER BY recieved_at ASC
            LIMIT 1
            """)

            if not row:
                break

            doc_id, jsonb = row
            obj = jsonb['objects']
            operation_details = jsonb['operation_details']

            all_relatives = find_children_parents(db)
            change_data(db, operation_details, obj, all_relatives)

            db.execute("""
                    UPDATE documents
                    SET processed_at = NOW()
                    WHERE doc_id = %s
                """, (doc_id,))
  
    return True


# собираем все дочерние обьекты в словарь
@decorator_catching_errors
def find_children_parents(db):
    row = db.select(""" 
    SELECT object, parent FROM data
    """, fetch_all=True)

    relatives = {}

    for child,parent in row:
        relatives.setdefault(parent, []).append(child)

    return relatives

# вносим изменения
@decorator_catching_errors
def change_data(db, operation_details, obj, all_relatives):
    if not operation_details:
        return
    all_children_parents = []

    for i in obj:
        if i in all_relatives:
            all_children_parents.extend(all_relatives[i] + [i])

    
    for operation, details in operation_details.items():
        new = details['new']
        old = details['old']

        db.execute(f""" 
            UPDATE data
            SET {operation} = %s
            WHERE {operation} = %s
            AND object = ANY(%s)
            """, (new, old, all_children_parents))


if __name__ == '__main__':
    print(changing_the_data(new_base))