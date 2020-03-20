import psycopg2
import datetime
from pymongo import MongoClient
from decimal import Decimal
from time import sleep
from os import system


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

# Creating Database clients
client = MongoClient("mongodb://mongo")
client.drop_database('dvdrental')
db = client['dvdrental']
conn = None

while not conn:
    try:
        conn = psycopg2.connect(user='postgres', dbname='dvdrental', 
                                password='postgres', host='postgres')
    except Exception as _:
        print("Sleeping...", flush=True)
        sleep(0.4)



# Creating collections and inserting data
def get_content(name):
    cursor = conn.cursor()
    cursor.execute(f'SELECT * FROM {name}')
    col_names = [desc[0] for desc in cursor.description]
    rows = cursor.fetchall()
    conn.close()
    return col_names, rows


def prepare():
    print("preparing...", flush=True)
    
    #conn.cursor().execute("CREATE DATABASE dvdrental;");
    #conn.cursor().execute(open("./postgree/restore.sql").read())
    system("PGPASSWORD=postgres psql -U postgres -h postgres -d dvdrental -f ./postgree/restore.sql")


def transfer():
    prepare()
    print("Here")

    collections = []
    for table_name in tables:
        collections.append(db[table_name])
        columns, table_data = get_content(table_name)
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