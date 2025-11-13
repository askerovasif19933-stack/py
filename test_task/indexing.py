from decorator_catching_error import decorator_catching_errors
from object_database_connect import ObjectDataBaseConnect
from create_base import new_base

@decorator_catching_errors
def indexing(base: str):
    """Индексация полей для быстрого доступа без блокировки таблиц (для больших таблиц)"""
    with ObjectDataBaseConnect(base) as db:

        # Список индексов для создания
        sql = [
            # Таблица documents
            'CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_documents_processed_at ON documents (processed_at)',
            'CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_documents_document_type ON documents (document_type)',
            'CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_documents_recieved_at ON documents (recieved_at)',
            'CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_documents_3_fields ON documents(processed_at, document_type, recieved_at)',

            # Таблица data
            'CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_data_status ON data(status)',
            'CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_data_parent ON data(parent)',
            'CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_data_owner ON data(owner)',
            'CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_data_object ON data(object)',
            'CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_data_status_owner ON data(status, owner)'
        ]

        # Выполняем каждый индекс отдельно
        for query in sql:
            try:
                db.execute(query)
            except Exception as e:
                print(f"Ошибка при создании индекса: {e}")