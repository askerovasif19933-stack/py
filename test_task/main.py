
from object_database_connect import ObjectDataBaseConnect
from dal import select_one_doc, parsing_data, search_all_child, correct_data, set_processing_time, indexing, process_single_document 
from create_base import new_base



        
def main(base: str):
    try:
        with ObjectDataBaseConnect(base) as db:
            old_val = db.select("""SELECT status, owner from data""", fetch_all=True)
            indexing(db)
            while True:
                
                row = process_single_document(db)
                if not row:
                    print(f'Все документы обработаны')
                    break
            new_val = db.select("""SELECT status, owner from data""", fetch_all=True)
            for k,v in zip(old_val, new_val):
                print(k, k==v, v)
    except Exception as e:
        return False


    return True

print(main(new_base))