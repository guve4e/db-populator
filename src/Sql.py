#!/usr/bin/python3


import pymysql


class SQL(object):
    """
        SQL connects to MySQL and gives back handle (db)
        Members:
            1. db =
    """
    db = None

    def __init__(self, server, username, password, schema) -> None:
        """
        Constructor
        :param server: database server name
        :param username:
        :param password:
        :param schema:
        """
        super().__init__()
        self.server = server
        self.username = username
        self.password = password
        self.schema = schema

    @staticmethod
    def create_schema(server, username, password, schema_name):
        sql = """CREATE DATABASE IF NOT EXISTS {};""".format(schema_name)

        # Open database connection
        db = pymysql.connect(server, username, password)

        # prepare a cursor object using cursor() method
        cursor = db.cursor()

        # execute
        cursor.execute(sql)

        # disconnect from server
        db.close()

    def create_table(self, sql_string: str):
        """
            Creates MySQL table
            :param sql_string is the sql string
        """
        if sql_string is None:
            raise Exception("Null parameters")

        # Open database connection
        self.db = pymysql.connect(self.server, self.username, self.password, self.schema)

        # prepare a cursor object using cursor() method
        cursor = self.db.cursor()

        # execute
        cursor.execute(sql_string)

        # disconnect from server
        self.db.close()

    def drop_table(self, table_name: str):
        """
            Drops MySQL table
            :param table_name is the name of the table
        """

        if table_name is None:
            raise Exception("Null parameters")

        # Open database connection
        self.db = pymysql.connect(self.server, self.username, self.password, self.schema)

        # prepare a cursor object using cursor() method
        cursor = self.db.cursor()

        # drop table if it already exist using execute() method
        cursor.execute("DROP TABLE IF EXISTS {0}".format(table_name))

        # disconnect from server
        self.db.close()

    def insert_into_table(self, table_name: str, cols: [], rows: []):
        """
            Prepare SQL query to INSERT a record into the database.
            :param table_name is the name of the table
            :param cols is list with the names of the columns (attributes)
            :param rows is list with the attribute values
        """
        sql = """INSERT INTO {0}({1})
           VALUES ({2})""".format(table_name, cols, rows)

        # Open database connection
        self.db = pymysql.connect(self.server, self.username, self.password, self.schema)

        # prepare a cursor object using cursor() method
        cursor = self.db.cursor()

        try:
            print("Inserting into {} table... ".format(table_name) + "Row: " + str(rows))
            # Execute the SQL command
            cursor.execute(sql)
            # Commit your changes in the database
            self.db.commit()
        except Exception as e:
            # Rollback in case there is any error
            self.db.rollback()
            print("Exception in insert_into_table " + str(e))

        # disconnect from server
        self.db.close()

    def retrieve_database_tables_names(self) -> list:
        sql = """SHOW TABLES"""

        # Open database connection
        self.db = pymysql.connect(self.server, self.username, self.password, self.schema)

        # prepare a cursor object using cursor() method
        cursor = self.db.cursor()
        cursor.execute(sql)

        return [item[0] for item in cursor.fetchall()]

    def retrieve_table_info(self, table_name) -> tuple:
        sql = """SELECT COLUMN_NAME, COLUMN_TYPE, EXTRA
                FROM INFORMATION_SCHEMA.COLUMNS 
                WHERE TABLE_NAME = '{}';""".format(table_name)

        # Open database connection
        self.db = pymysql.connect(self.server, self.username, self.password, self.schema)

        # prepare a cursor object using cursor() method
        cursor = self.db.cursor(pymysql.cursors.DictCursor)
        cursor.execute(sql)

        return cursor.fetchall()

    def retrieve_table_values(self, table_name) -> list:

        # Open database connection
        self.db = pymysql.connect(self.server, self.username, self.password, self.schema)

        sql = """SELECT * FROM {};""".format(table_name)

        # prepare a cursor object using cursor() method
        cursor = self.db.cursor(pymysql.cursors.DictCursor)
        cursor.execute(sql)

        return cursor.fetchall()



