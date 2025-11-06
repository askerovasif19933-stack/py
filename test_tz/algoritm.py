from test_tz.class_conect_DB import DBconnect


with DBconnect('tz_base') as conn:
    x = conn.select("SELECT object, parent FROM data", ftch_all=True)
    row = conn.select(f"""
               SELECT doc_id, document_data FROM documents
               ORDER BY recieved_at ASC
               LIMIT 1
               """)

    doc_id, jsonb = row
    object_json = jsonb['objects']
    dict_parent = {}

    print(dict_parent)
    for i in x:
        dict_parent[i[-1]] = dict_parent.get(i[-1], []) + [i[0]]

    for j in object_json:
        if j in dict_parent:
            v = dict_parent[j]+[j]
            print(v)

    print(x)
    print(object_json)