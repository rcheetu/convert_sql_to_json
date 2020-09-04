import ibm_db_dbi as db
import pymssql
import json
from json import JSONEncoder
import datetime
import os
import glob
from timeit import default_timer as timer

global type_data_base
db2_query = [f for f in glob.glob("../sqls/db2/*.sql")]
mssql_query = [f for f in glob.glob("../sqls/mssql/*.sql")]

class DateTimeEncoder(JSONEncoder):
        def default(self, obj):
            if isinstance(obj, (datetime.date, datetime.datetime)):
                return obj.isoformat()

def get_current_date_time():
    return datetime.datetime.now().strftime('%a_%d.%b.%Y_%H.%M.%S')

def ibm_db2_connection():
    try:
        return db.connect("DATABASE=DATABASE;HOSTNAME=IP.COM;PORT=3700;PROTOCOL=TCPIP;UID=USERNAME;PWD=PASSWORD;", "", "")
    except Exception:
        print("\nERROR: Unable to connect to the server.")
        exit(-1)

def mssql_connection():
    try:
        return pymssql.connect(server="IP.COM", user="USERNAME", password="PASSWORD", database="DATABASE")
    except Exception:
        print("\nERROR: Unable to connect to the server.")
        exit(-1)

def query_db(query):
    global type_data_base
    if type_data_base: 
        cur = ibm_db2_connection().cursor()
    else: 
        cur = mssql_connection().cursor()
    cur.execute(query)
    r = [dict((cur.description[i][0], value) for i, value in enumerate(row)) for row in cur.fetchall()]
    cur.connection.close()
    return r

def write_json(query_path):
    with open(f"{query_path}", 'r') as f:
        sql = f.read().replace('\n', ' ')
    file_name = os.path.splitext(os.path.basename(f"{query_path}"))[0]
    with open(f"../output_json_nice_converter/{file_name}_{get_current_date_time()}.json", 'w', encoding='utf-8') as f:
        json.dump(query_db(sql), f, ensure_ascii=False, indent=4, cls=DateTimeEncoder)

def conversion_db():
    global type_data_base
    type_data_base = True
    for sql in db2_query:
        write_json(sql)

def conversion_mssql():
    global type_data_base
    type_data_base = False
    for sql in mssql_query:
        write_json(sql)

if __name__ == "__main__":
    start = timer()
    print('\033[33m', "Conversion started, please wait...", '\033[0m', sep='')
    conversion_db()
    conversion_mssql()
    end = timer()
    print('\033[32m', "Conversion completed successfully for: {}!".format(end - start), '\033[0m', sep='')
