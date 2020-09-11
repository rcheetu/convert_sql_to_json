from abc import ABC, abstractmethod
import ibm_db_dbi as db
import pymssql
import configparser
import json
import codecs
import logging
from json import JSONEncoder
import datetime
import os
import sys
import glob
from timeit import default_timer as timer

config  = configparser.RawConfigParser() 
config.read('../config.ini')

logger = logging.getLogger('')
logger.setLevel(logging.DEBUG)
fh = logging.FileHandler(f'../logs/log_generator.log')
sh = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter('[%(asctime)s] %(levelname)s [%(filename)s.%(funcName)s:%(lineno)d] %(message)s', datefmt='%a, %d %b %Y %H:%M:%S')
fh.setFormatter(formatter)
sh.setFormatter(formatter)
logger.addHandler(fh)
logger.addHandler(sh)

def singleton(class_):
    instances = {}

    def get_instance(*args, **kwargs):
        if class_ not in instances:
            instances[class_] = class_(*args, **kwargs)
        return instances[class_]

    return get_instance

class Config(object):
    def load_db2_param():
        return "database={};hostname={};port={};protocol={};uid={};pwd={};".format(config.get('ibm', 'DATABASE'),
                                                                                   config.get('ibm', 'HOSTNAME'),
                                                                                   config.get('ibm', 'PORT'),
                                                                                   config.get('ibm', 'PROTOCOL'), 
                                                                                   config.get('ibm', 'UID'),
                                                                                   config.get('ibm', 'PWD'))
        
    def load_mssql_param():
        return "{},{},{},{}".format(config.get('mssql', 'SERVER'),
                                    config.get('mssql', 'USER'),
                                    config.get('mssql', 'PASSWORD'),
                                    config.get('mssql', 'DATABASE'))                                                                             

    CHOSE_DATABASE = {
          'db2': load_db2_param,
          'mssql': load_mssql_param,
          }


class Database(ABC):
   @abstractmethod
   def connection(self):
        pass

   @abstractmethod
   def get_query(self):
        pass

@singleton
class DB2DataBase(Database):
    def connection(self):
        try:
            conn = db.connect(Config.CHOSE_DATABASE['db2'](), "", "")
            logging.info("DB2 Connected!\n")
            return conn
        except Exception:
            logging.error("\nERROR: Unable to connect to the ibm server.")

    @staticmethod
    def get_query():
        return [f for f in glob.glob("../sqls/db2/*.sql")]

@singleton
class MsSqlDataBase(Database):
    def connection(self):
        try:
            conn = pymssql.connect(server   = config.get('mssql', 'SERVER'),
                                   user     = config.get('mssql', 'USER'),
                                   password = config.get('mssql', 'PASSWORD'),
                                   database = config.get('mssql', 'DATABASE'))
            logging.info("MSSQL Connected!\n")
            return conn
        except Exception:
            logging.error("\nERROR: Unable to connect to the mssql server.")

    
    @staticmethod
    def get_query():
        return [f for f in glob.glob("../sqls/mssql/*.sql")]


class DbFactory:
    def get_database_connection(self, database):
        return database.connection()
    def get_database_query(self, database):
        return database.get_query()


class DateTimeEncoder(JSONEncoder):
        def default(self, obj):
            if isinstance(obj, (datetime.date, datetime.datetime)):
                return obj.isoformat()


class Convertor:
    def _decorator(function):
        def inner_decorator(database):
            start = timer()
            function(database)
            end = timer()
            logger.info(f"SQLs from {database} database are successfully converted to JSON for {end - start}  !\n\n")
        return inner_decorator
    
    @staticmethod
    def get_current_date_time():
        return datetime.datetime.now().strftime('%a_%d.%b.%Y_%H.%M.%S')
    
    @staticmethod
    def query_db(sql, conn):
        cur = conn.cursor()
        cur.execute(sql)
        r = [dict((cur.description[i][0], value) for i, value in enumerate(row)) for row in cur.fetchall()]
        cur.connection.close()
        logger.info(f"Conncetion {conn} is  closed.")
        return r

    @staticmethod
    def write_json(sql_path, conn):
        with codecs.open(f"{sql_path}", 'r', "utf-8-sig") as f:
            sql = f.read().replace('\n', ' ')
        logger.info(f"Start converting {os.path.basename(sql_path)} into JSON...")
        file_name = os.path.splitext(os.path.basename(f"{sql_path}"))[0]
        with open(f"../output_json_generator/{file_name}_{Convertor.get_current_date_time()}.json", 'w', encoding='utf-8') as f:
            json.dump(Convertor.query_db(sql, conn), f, ensure_ascii=False, indent=4, cls=DateTimeEncoder)
        logger.info(f"The sql {os.path.basename(sql_path)} successfully converted to JSON !\n")
    
    @staticmethod
    @_decorator
    def start_converting(database):
        for sql in DbFactory().get_database_query(database):
            Convertor.write_json(sql, DbFactory().get_database_connection(database))


if __name__ == "__main__":
    Convertor.start_converting(DB2DataBase())
    Convertor.start_converting(MsSqlDataBase())
