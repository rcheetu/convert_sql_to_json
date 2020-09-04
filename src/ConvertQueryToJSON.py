import sys
import ibm_db
import pymssql
import codecs
import logging
from datetime import datetime
 
def get_current_date_time():
    return datetime.now().strftime('%a_%d.%b.%Y_%H.%M.%S')
 
logger = logging.getLogger('')
logger.setLevel(logging.DEBUG)
fh = logging.FileHandler(f'../logs/log_{get_current_date_time()}.log')
sh = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter('[%(asctime)s] %(levelname)s [%(filename)s.%(funcName)s:%(lineno)d] %(message)s', datefmt='%a, %d %b %Y %H:%M:%S')
fh.setFormatter(formatter)
sh.setFormatter(formatter)
logger.addHandler(fh)
logger.addHandler(sh)
 
characters = ["[", "{", "}", "\n]", "\n}"]
 
def yellow(text):
    print('\033[33m', text, '\033[0m', sep='')
 
def green(text):
    print('\033[32m', text, '\033[0m', sep='')
 
def ibm_db2_connection():
    try:
        conn = ibm_db.connect("DATABASE=DATABASE;HOSTNAME=IP.COM;PORT=3700;PROTOCOL=TCPIP;UID=USERNAME;PWD=PASSWORD;", "", "")
        logging.info("IBM DB2 Connected!")
        return conn
    except Exception:
        logging.error("\nERROR: Unable to connect to the server.")
        # print("error: ", ibm_db.conn_errormsg())
        exit(-1)
 
def mssql_connection():
    try:
        conn = pymssql.connect(server="IP.COM", user="USERNAME", password="PASSWORD", database="DATABASE")
        logging.info("MSSQL Connected!")
        return conn
    except Exception:
        logging.error("\nERROR: Unable to connect to the server.")
        exit(-1)
 
def first_example():
    logging.info("FIRST EXAMPLE conversion started...")
    conn = ibm_db2_connection()
    with open(f"../sqls/db2/test_sql.sql", 'r') as f:
        sql = f.read().replace('\n', ' ')
    try:
        stmt = ibm_db.exec_immediate(conn, sql)
    except:
        logging.error(f"Transaction couldn't be completed: {ibm_db.stmt_errormsg()}")
    else:
        with codecs.open(f"example_function_{get_current_date_time()}.json", "w", "utf-8-sig") as f:
            f.write(characters[0])
            while ibm_db.fetch_row(stmt) is not False:
                f.write(f'\n\t{characters[1]}\n\t"FIRST_ROW": "{ibm_db.result(stmt, 0)}",')
                f.write(f'\n\t"SECOND_ROW": "{ibm_db.result(stmt, 1)}",')
                f.write(f'\n\t"THIRD_ROW": "{ibm_db.result(stmt, 2)}"\n\t{characters[2]},')
            ibm_db.close(conn)
            logging.info("DATABASE Connection Close.")
            f.write(characters[3])
        logging.info("FIRST EXAMPLE conversion completed!")

def second_example():
    logging.info("SECOND EXAMPLE conversion started...")
    conn = mssql_connection()
    cursor = conn.cursor()
    with open(f"../sqls/mssql/test_sql_2.sql", 'r') as f:
        sql = f.read().replace('\n', ' ')
    cursor.execute(sql)
    row = cursor.fetchone()
    with codecs.open(f"second_example_{get_current_date_time()}.json", "w", "utf-8-sig") as f:
        f.write(characters[0])
        while row: 
            f.write(f'\n\t{characters[1]}\n\t"FIRST_ROW": "{row[0]}",')
            f.write(f'\n\t"SECOND_ROW": "{row[1]}",')
            f.write(f'\n\t"THIRD_ROW": "{row[2]}"\n\t{characters[2]},')
            row = cursor.fetchone()
        conn.close()
        f.write(characters[3])
    logging.info("MSSQL Connection Close.")
    logging.info("SECOND EXAMPLE conversion completed!")
 

if __name__ == "__main__":
    yellow("Conversion started, please wait...")
 
    first_example()
    second_example()
 
    green("Conversion completed successfully!")
 
