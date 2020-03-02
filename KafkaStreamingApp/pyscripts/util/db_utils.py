"""
This is a utils file for database
"""
import sqlite3
from sqlite3 import Error
from pyscripts.util.logging_util import get_logger

# Method to get the db connection
from pyscripts.constants.app_producer_constants import DB_FILE_PATH


def get_db_connection(db_file_path, logger):
    """ create a database connection to a SQLite database """
    conn = None
    try:
        logger.info(db_file_path)
        conn = sqlite3.connect(db_file_path)
    except Error as e:
        logger.error(e)
    return conn


# Method to initialize the db by creating the table for processstatus log
def initialize_db(cursor, logger):
    try:
        cursor.execute("CREATE TABLE IF NOT EXISTS producer_message_log "
                       "(id integer primary key autoincrement,name text,records_processed text)")
    except Error as e:
        logger.error(e)


# Method to insert into table
def insert_processstatuslog(cursor, conn, table_name, producer_name, processed_count, logger):
    recordid = 0
    try:
        recordid = cursor.execute(
            "insert into " + table_name + " (name,records_processed) values('" + str(producer_name) + "','" + str(
                processed_count) + "')").lastrowid
        conn.commit()
    except Error as e:
        logger.error(e)
    return recordid


# Method to update the processstatus log
def update_processstatuslog(cursor, conn, recordid, table_name, processed_count, logger):
    try:
        cursor.execute("update " + table_name + " set records_processed='" + str(processed_count) + "' where id=:id",
                       {"id": recordid})

        conn.commit()
    except Error as e:
        logger.error(e)


# Method to fetch records from processstatus log
def get_processstatuslog_db(cursor, table_name, producer_name, logger):
    query = "SELECT * FROM " + table_name
    try:
        if producer_name:
            query = query + " where name='" + producer_name+"'"
        logger.info(query)
        cursor.execute(query)
        rows = cursor.fetchall()
        return rows
    except Error as e:
        logger.error(e)
    return None

'''if __name__=="__main__":
    logger = get_logger()
    conn=get_db_connection(DB_FILE_PATH, logger)
    cursor = conn.cursor()
    initialize_db(cursor, logger)
    #insert_processstatuslog(cursor, conn, "producer_message_log", "STREAM_PRODUCER", 1, logger)
    print(get_processstatuslog_db(cursor, "producer_message_log", "STREAM_PRODUCER", logger))'''