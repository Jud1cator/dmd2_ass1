import psycopg2
import datetime
from pymongo import MongoClient
from decimal import Decimal
from time import sleep


tables = [
    'actor',
    'store',
    'address',
    'category',
    'city',
    'country',
    'customer',
    'film_actor',
    'film_category',
    'inventory',
    'language',
    'rental',
    'staff',
    'payment',
    'film',
]


def create_dbs():
    s = "dbname='dvdrental' user='postgres'" + \
        "host='postgres' password='postgres'"
    conn = None

    while True:
        try:
            conn = psycopg2.connect(s)
            break
        except Exception as e:
            print(f"Error: {e}", flush=True)
            sleep(1)

    client = MongoClient("mongodb://mongo")
    client.drop_database('dvdrental')

    return client['dvdrental'], conn


def get_content(conn, name):
    cursor = conn.cursor()
    cursor.execute(f'SELECT * FROM {name}')
    col_names = [desc[0] for desc in cursor.description]
    rows = cursor.fetchall()
    return col_names, rows


def transfer(db, conn):
    collections = []
    for table_name in tables:
        collections.append(db[table_name])
        columns, table_data = get_content(conn, table_name)
        for row in table_data:
            doc = {}
            for i in range(len(columns)):
                field = row[i]
                if type(field) == datetime.date:
                    field = datetime.datetime.combine(row[i], datetime.time())
                if table_name == 'staff' and columns[i] == 'picture':
                    if row[i] is None:
                        field = None
                    else:
                        field = row[i].tobytes()
                if type(field) == Decimal:
                    field = float(row[i])
                doc.update({columns[i]: field})
            collections[-1].insert_one(doc)
        print('Table "%s": %d records inserted' %
              (table_name, collections[-1].count_documents({})))
