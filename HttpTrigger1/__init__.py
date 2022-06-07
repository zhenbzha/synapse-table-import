import logging

import azure.functions as func

from sqlalchemy import create_engine, MetaData, Table, select, Column, Integer, String, Sequence
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import urllib
import json
import re

server = 'zhenzh-demoserver.database.windows.net'
database = 'demo'
username = 'zhenbo'
password = ''   
driver= '{ODBC Driver 17 for SQL Server}'
Base = declarative_base()

class TableMapping(Base):
    __tablename__ = 'tbl_mapping'
    id = Column(Integer, Sequence('id'), primary_key=True)
    src_tbl_schema = Column(String(50))
    src_tbl_name = Column(String(50))
    dest_tbl_schema = Column(String(50))
    dest_tbl_name = Column(String(50))
    dest_tbl_dist = Column(String(50))
    dest_tbl_idx = Column(String(50))

    def __repr__(self):
        return """<TableMapping(src_tbl_schema={}, src_tbl_name={}, 
                dest_tbl_schema={}, dest_tbl_name={}, dest_tbl_dist={}, 
                dest_tbl_idx={})>""".format(self.name, self.fullname, self.nickname)

def get_from_query_str_or_body(key, req):

    value = req.params.get(key)
    if not value:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            value = req_body.get(key)

    return value


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    src_tbl_schema = get_from_query_str_or_body('src_tbl_schema', req)
    src_tbl_name = get_from_query_str_or_body('src_tbl_name', req)
    dest_tbl_schema = get_from_query_str_or_body('dest_tbl_schema', req)
    dest_tbl_name = get_from_query_str_or_body('dest_tbl_name', req)
    dest_tbl_dist = get_from_query_str_or_body('dest_tbl_dist', req)
    dest_tbl_idx = get_from_query_str_or_body('dest_tbl_idx', req)

    params = urllib.parse.quote_plus(
        'Driver=%s;' % driver +
        'Server=tcp:%s,1433;' % server +
        'Database=%s;' % database +
        'Uid=%s;' % username +
        'Pwd={%s};' % password +
        'Encrypt=yes;' +
        'TrustServerCertificate=no;' +
        'Connection Timeout=30;')
    conn_str = 'mssql+pyodbc:///?odbc_connect=' + params
    engine = create_engine(conn_str)

    Session = sessionmaker(bind=engine)
    session = Session()
    session.query()

    metadata = MetaData()
    src_tbl = Table(src_tbl_name, metadata, autoload=True, autoload_with=engine, schema=src_tbl_schema)

    col_str = ""
    for col in src_tbl.columns:            
        #todo: remove ()
        src_col_name = col.name

        if src_col_name[0] == '(' or src_col_name[0] == ')':
            src_col_name = src_col_name[1:]
        if src_col_name[-1] == '(' or src_col_name[-1] == ')':
            src_col_name = src_col_name[:-1]
        
        src_col_name = re.sub("[()]", "_", src_col_name)
        
        dest_col_name = src_col_name
        
        # if src col is geometry use	varbinary in sql pool
        type = str(col.type).replace("\"", "")
        col_str += "{} {},\n".format(dest_col_name, type)
    
    col_str = col_str[:-2]

    table_creation_statement = """
    CREATE TABLE {}.{}
    (  
        {}
    )  
    WITH  
    (   
        DISTRIBUTION = {},
        {}
    ); 
    """.format(dest_tbl_schema, dest_tbl_name, col_str, dest_tbl_dist, dest_tbl_idx)


    return json.dumps({"query_str": table_creation_statement})
