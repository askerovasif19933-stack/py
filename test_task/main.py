from object_database_connect import ObjectDataBaseConnect
from data_correction import select_one_doc, parsing_data, search_all_child, correct_data, set_processing_time
from create_base import new_base
from indexing import indexing



indexing(new_base)

def main(base:str):

    with ObjectDataBaseConnect(base) as db:
        old_val = db.select("""SELECT status, owner from data""", fetch_all=True)

        row = select_one_doc(db)

        doc_id, obj, operation_details = parsing_data(db, row)

        all_parand_child = search_all_child(db, obj)

        correct_data(db, all_parand_child, operation_details)

        set_processing_time(db, doc_id)
        new_val = db.select("""SELECT status, owner from data""", fetch_all=True)
        for k,v in zip(old_val, new_val):
            print(k, k==v, v)
        return True

print(main(new_base))