"""
This is a utils file for database
"""
import sqlite3
from sqlite3 import Error


# Method to get the db connection
def get_db_connection(db_file_path, logger):
    """ create a database connection to a SQLite database """
    conn = None
    try:
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
            query = query + " where name=" + producer_name
        logger.info(query)
        cursor.execute(query)
        rows = cursor.fetchall()
        return rows
    except Error as e:
        logger.error(e)
    return None
