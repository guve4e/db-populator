#!/usr/bin/env python3
import time
import collections
import json
from os import listdir
from os.path import isfile, join
from src.Sql import SQL
from src.parse_json import ParseJson

SCRIPTS_DICT = "dumps"


class Populate(object):

    def __init__(self, info_json) -> None:
        """
        Constructor
        """
        super().__init__()

        self.info = ParseJson(info_json)
        server = self.info.json_data['server']
        username = self.info.json_data['username']
        password = self.info.json_data['password']
        database = self.info.json_data['database']

        # create schema
        SQL.create_schema(server, username, password, database)

        self.sql = SQL(server, username, password, database)

    @staticmethod
    def double_quote_string(word: str) -> str:
        double_q = '"'
        return double_q + word + double_q

    @staticmethod
    def get_name_and_type_of_column(dict: {}):
        """
            Retrieves the name and the type of the column
            :param dict is dictionary describing an attribute
            It has type as (string, int) and value.
            :return col_type and col_value
        """
        if len(dict) > 2:
            raise Exception("Wrong JSON file")

        # initial values for type and value
        col_type = ""
        col_value = ""

        # iterate trough the dictionary
        # get the type and value of the table cell

        for key, value in dict.items():
            if key == 'type':
                col_type = value
            elif key == 'value':
                col_value = value
            else:
                raise Exception("Wrong JSON file")

        # return the type and the value of the cell
        return col_type, col_value

    @staticmethod
    def make_table_values(row_list: []) -> []:
        """
            Makes list of table values
            :param row_list is list of raw values.
            This function determines if the value will
            be string (with double quotes) or int
            :return list of table values as: "327 N.Elm" or 327 as int
        """
        # list to hold strings with double quotes
        row_list_salted = []

        # iterate trough the dict and
        for dict in row_list:
            type, value = Populate.get_name_and_type_of_column(dict)

            if type == 'string':
                value = Populate.double_quote_string(value)

            row_list_salted.append(value)

        # list with added double quotes
        return row_list_salted

    @staticmethod
    def make_join_string(list: []) -> []:
        """
             Makes list of table values
             :param list is list of values
             :return list with coma separation
         """
        # return the same values with coma separation
        return ', '.join(list)

    @staticmethod
    def extract_table_name(sql_str: str) -> dict:
        """
            Extracts the name of the table
            from sql string.
            :param sql_str is sql string
            :return dictionary with one element
            key is the name of the table value is
            the sql string
        """
        if sql_str is None:
            raise Exception("Null Parameter")

        # split sql string
        sql_string = sql_str.split()

        # get the third token
        if sql_string[2] is not None:
            table = sql_string[2]
        else:
            raise Exception("Sql file is in wrong format")

        # make dict and return it
        return {table: sql_str}

    @staticmethod
    def extract_rows_and_columns_names(row_dict: {}):
        """
            Given a dictionary representing a row in
            a table with:
            key : the name of the column and
            value : the value of the cell
            This function separates the names of the columns
            with the values of the cells in two separate lists
        """
        # make sure dict is ordered
        row_dict = collections.OrderedDict(row_dict)

        columns = []
        rows = []
        # iterate trough the dict and append
        # the columns and rows lists
        for key, value in row_dict.items():
            # append the lists
            columns.append(key)
            rows.append(value)

        # give back the rows and columns lists
        return columns, rows

    @staticmethod
    def get_scripts_from_file(filename: str) -> [str]:
        """
            Open and read the file as a single buffer
            :param filename is the name of the sql file
            :return sql_commands is sql string
        """
        fd = open(filename, 'r')
        sql_file = fd.read()
        fd.close()

        # all SQL commands (split on ';')
        sql_commands = sql_file.rstrip(';').split(';')
        return sql_commands

    def insert_into_table(self, table_name: str, table_json_file):
        """"
            Insert into table
            :param table_name is the name of the table
            :param table_json_file is json file with table attributes
        """

        path = SCRIPTS_DICT + "/" + table_json_file

        try:
            with open(path) as json_data:
                # load json to json_array
                # at this point json_array has to be a list
                # of dictionaries, each element of json_array
                # is dictionary representing one table row
                json_array = json.load(json_data)

                i = 0
                for table_row in json_array:
                    # get table's rows cells and column names
                    column_dict, row_dict = Populate.extract_rows_and_columns_names(table_row)

                    # send column_dict that contains the names of the columns to make_join_string
                    # so the names get added to a list of names, separated by comas
                    column_string = Populate.make_join_string(column_dict)

                    # retrieve attributes' values
                    attribute_values_string = Populate.make_table_values(row_dict)
                    # convert attributes' values to string
                    attribute_values_string = list(map(str, attribute_values_string))

                    # send attribute_values_string that contains the attributes' values
                    # so the the values get added to a list of values, separated by comas
                    attribute_values_string = Populate.make_join_string(attribute_values_string)

                    self.sql.insert_into_table(table_name, column_string, attribute_values_string)
                    i = i + 1

        except IOError as e:
            print("Exception opening file " + table_json_file + " " + e.strerror)

    def create_schema(self):
        self.sql.create_schema(self.database)

    def create_tables(self):
        """
        Loads a sql file with script to create tables.
        :return:
        """
        # load script from file
        sql_list = Populate.get_scripts_from_file("schema/tables.sql")

        # store it in ordered dictionary
        sql_dict = collections.OrderedDict()
        for sql in sql_list:
            if not sql == '\n':
                sql_dict.update(Populate.extract_table_name(sql))

        # reverse the dictionary
        rev = collections.OrderedDict(reversed(list(sql_dict.items())))

        # drop schema first
        for key, value in rev.items():
            self.sql.drop_table(key)

        # create schema
        for table_name, sql_string in sql_dict.items():
            self.sql.create_table(sql_string)

    @staticmethod
    def retrieve_table_name(file_name):
        table_name = file_name.split(".")
        return table_name[0]


if __name__ == "__main__":

    start_time = time.time()

    db = Populate("schema/schema_info.json")

    # create tables
    db.create_tables()

    files = [f for f in listdir(SCRIPTS_DICT) if isfile(join(SCRIPTS_DICT, f))]

    for file in files:
        table_name = Populate.retrieve_table_name(file)
        db.insert_into_table(table_name, file)


    end_time = time.time()
    elapsed_time = round((end_time - start_time), 2)
    print("=====================================")
    print("It took " + str(elapsed_time) + " seconds to run script!")
