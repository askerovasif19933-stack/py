from itertools import chain
from operator import le
from object_database import ObjectDataBaseConnect, decorator_catching_errors
from create_base import new_base



@decorator_catching_errors
def changing_the_data(base: str):

    with ObjectDataBaseConnect(base) as db:
        # для сраснения изменений
        # old_val = db.select("""SELECT status, owner from data""", fetch_all=True)

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

            
            all_relatives = db.select(""" 
                SELECT object, parent FROM data
                """, fetch_all=True)
            

            dict_relatives = {}

            for object,parent in all_relatives:
                dict_relatives.setdefault(parent, []).append(object)
 

            if operation_details:
                all_children_parents = []

                for i in obj:
                    if i in dict_relatives:
                        all_children_parents.extend(dict_relatives[i] + [i])

                
                for operation, details in operation_details.items():
                    new = details['new']
                    old = details['old']

                    db.execute(f""" 
                        UPDATE data
                        SET {operation} = %s
                        WHERE {operation} = %s
                        AND object = ANY(%s)

                        """, (new, old, all_children_parents))
            
            db.execute("""
                    UPDATE documents
                    SET processed_at = NOW()
                    WHERE doc_id = %s
                """, (doc_id,))
            # для проверки изменения результата до и после
            # new_val = db.select("""SELECT status, owner from data""", fetch_all=True)
            # for old, new in zip(old_val, new_val):
            #    print(old, old == new, new)
            
    return True


if __name__ == '__main__':
    print(changing_the_data(new_base))