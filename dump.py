#!/usr/bin/env python3
import time
import collections
import json
from src.Sql import SQL
from src.parse_json import ParseJson


class Dump(object):

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
    def get_auto_incremented_column_name(row_infos: []):
        """
        Searches for element with key 'EXTRA',
        if it is "auto_increment" it returns
        the column name.
        :param row_infos: list with information about
        a table row
        :return:
        """
        for element in row_infos:
            for key, value in element.items():
                if key == "EXTRA" and value == "auto_increment":
                    return element['COLUMN_NAME']

        return None

    @staticmethod
    def remove_auto_incremented_values(row_infos: [], row_values: []) -> []:
        """
        Removes an element with specific key, that is retrieved form
        function get_auto_incremented_column_name.
        TODO Not sure if ordered dictionary is needed
        :param row_infos:
        :param row_values:
        :return:
        """

        # try to get the key to be deleted, if any
        del_key = Dump.get_auto_incremented_column_name(row_infos)

        # if there are no auto incremented columns, return early
        if del_key is None:
            return

        ordered_values = []

        # convert list of dict to list of ordered dict
        for element in row_values:
            ord_dict = collections.OrderedDict(element)
            ordered_values.append(ord_dict)

        # delete the element
        for element in ordered_values:
            del element[del_key]

        return ordered_values

    @staticmethod
    def determine_sql_type(key: str, row_info: []) -> str:
        """
        Searches for column name in the list of
        information
        :param key: the key to search for
        :param row_info: list of information about the columns
        :return: the value for the key, the type of sql column
        """
        for element in row_info:
            if element["COLUMN_NAME"] == key:
                return element["COLUMN_TYPE"]

    @staticmethod
    def determine_type(sql_type: []) -> str:
        """
        Static map, mapping a sql type,
        to our specific type.
        :param sql_type:
        :return: our type
        """
        type_map = {
            "mediumint": "int",
            "decimal": "int",
            "int": "int",
            "varchar": "string",
            "text": "string",
            "date": "string",
            "timestamp": "string"
        }

        for key, value in type_map.items():
            if key in sql_type:
                return value

    @staticmethod
    def create_entry_dict(column_type: str, column_value: str) -> dict:
        """
        Creates an entry dictionary
        """
        return {"type": column_type, "value": column_value}

    @staticmethod
    def create_json_list(column_info_list: [], columns: []) -> []:
        """
            Creates individual fields using column name, column type
            and column value, then make a dict and adds it to a list.
            :param column_info_list:
            :param columns:
            :param table_name:
            :return:
            """

        columns = Dump.remove_auto_incremented_values(column_info_list, columns)
        # at this point the auto_increment field is removed

        # If table is empty, row_values will be empty
        # then we do not need to proceed, exit early
        if not columns:
            return

        rows_list = []

        for column in columns:
            field_dict = {}
            for key, value in column.items():
                sql_type = Dump.determine_sql_type(key, column_info_list)
                type = Dump.determine_type(sql_type)
                field = Dump.create_entry_dict(type, value)
                field_dict[key] = field

            rows_list.append(field_dict)

        return rows_list

    @staticmethod
    def dump_json(rows_list: [], table_name: str):
        """
        Dumps rows_list parameter to json file.
        :param rows_list:
        :param table_name:
        :return:
        """
        print("Dumping table {} .....".format(table_name))

        with open('dumps/{}.json'.format(table_name), 'w') as outfile:
            json.dump(rows_list, outfile, sort_keys=True, indent=4, default=str)

    def get_columns_info(self, table_name: str):
        """
        Looks into the database and
        retrieves all the table names.
        :param table_name:
        :return:
        """
        info = self.sql.retrieve_table_info(table_name)
        return info

    def get_values(self, table: str):
        """
        Fetches all row values
        :param table:
        :return:
        """
        values = self.sql.retrieve_table_values(table)
        return values

    def get_tables_names(self):
        return self.sql.retrieve_database_tables_names()


if __name__ == "__main__":

    start_time = time.time()

    db = Dump("schema/schema_info.json")

    tables = db.get_tables_names()

    for table in tables:
        info = db.get_columns_info(table)
        values = db.get_values(table)
        json_list = Dump.create_json_list(info, values)
        # if the list is not empty
        if json_list:
            Dump.dump_json(json_list, table)

    end_time = time.time()
    elapsed_time = round((end_time - start_time), 2)
    print("=====================================")
    print("It took " + str(elapsed_time) + " seconds to run script!")
