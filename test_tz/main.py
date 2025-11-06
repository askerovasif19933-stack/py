
from new_base import base
from class_conect_DB import DBconnect


def main(base: str):

    with DBconnect(base) as db:
        # jjj = db.select("""SELECT status, owner from data""", ftch_all=True)
        while True:

            row = db.select(f"""
            SELECT doc_id, document_data FROM documents
            WHERE processed_at IS NULL
            AND document_type = 'transfer_document'
            ORDER BY recieved_at ASC
            LIMIT 1
            """)

            if not row:
                print('документы закончились')
                break

            doc_id, jsonb = row

            obj = jsonb['objects']
            operation_details = jsonb['operation_details']

            all_child = set(obj)
            turn = list(obj)

            while turn:

                obj_turn = turn.pop(0)

                search_child = db.select("""SELECT object FROM data WHERE parent = (%s)""", (obj_turn,), ftch_all=True)

                child_list = [i[0] for i in search_child]

                for i in child_list:

                    if i not in all_child:

                        all_child.add(i)
                        turn.append(i)


                if operation_details:

                    for operation,details in operation_details.items():

                        new = details['new']
                        old = details['old']

                        db.execute(f"""
                        UPDATE data
                        SET {operation} = %s
                        WHERE {operation} = %s AND object = ANY(%s)
                        """, (new, old, list(all_child)))

            db.execute("""UPDATE documents SET processed_at = NOW() WHERE doc_id = %s""", (doc_id,))
            # kkk = db.select("""SELECT status, owner from data""", ftch_all=True)
            # для проверки изменения результата до и после
            # for i in zip(jjj, kkk):
            #     print(i[0] == i[-1])

    return True

print(main(base))














