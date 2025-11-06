
from database import Base, engine, get_session
from data_filler import make_data, make_documents
from models import Data, Documents
from sqlalchemy import select, and_, update, func

# сначала удаляем старое значение потом переприсваиваем, что бы не добавлялись значения подряд
def create_table():
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)



data = make_data()
data_tbl = list(data.values())
documents_tbl = make_documents(data)


def insert():
    with get_session() as session:

        insert_data = [Data(**i) for i in data_tbl]
        session.add_all(insert_data)

        insert_doc = [Documents(**i) for i in documents_tbl]
        session.add_all(insert_doc)
        session.commit()

        print(f"Вставлено {len(insert_data)} данных и {len(insert_doc)} документов")


def main():
    with get_session() as session:
        old = select(Data.status, Data.owner)
        old_ex = session.execute(old).all()
        while True:
            # Находим необработанный документ
            stmt = select(Documents).where(
                and_(
                    Documents.processed_at == None,
                    Documents.document_type == 'transfer_document'
                )
            ).order_by(Documents.recieved_at.asc()).limit(1)

            document = session.scalar(stmt)

            if not document:
                print('документы закончились')
                break

            # Извлекаем данные из JSONB
            data = document.document_data
            objects = data['objects']
            operations = data.get('operation_details', {})

            # собираем все дочерние обьекты в список по ключу parent
            relative = select(Data.object, Data.parent)
            all_child_parent = session.execute(relative).all()
            parent_child = {}

            for child, parent in all_child_parent:
                parent_child.setdefault(parent, []).append(child)

            # Применяем операции если есть
            if operations:
                list_all_relative = []

                for i in objects:
                    if i in parent_child:
                        list_all_relative.extend(parent_child[i]+[i])

                for operation, details in operations.items():
                    session.execute(
                        update(Data).where(
                        and_(
                            getattr(Data, operation) == details['old'],
                            Data.object.in_(list_all_relative)
                        )
                    ).values(**{operation: details['new']})
                )

            # Помечаем документ обработанным
            document.processed_at = func.now()
            session.commit()

        # new = select(Data.status, Data.owner)
        # new_ex = session.execute(new).all()


        # for k,v in zip(old_ex, new_ex):
        #     print(k, k==v, v)

    return True


