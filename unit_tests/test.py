#!/usr/bin/python3
import unittest
import populate
import collections


class DatabasePopulateTestCase(unittest.TestCase):
    # Arrange
    # Act
    # Assert


    def test_double_quote_string(self):
        # Arrange
        string = 'Some String'
        # Act
        string_quotes = populate.double_quote_string(string)
        # Assert
        self.assertEqual(string_quotes, '"Some String"')

    def test_get_name_and_type_of_column(self):
        # Arrange
        name_and_type_dict = {'value': 'soap.jpg', 'type': 'string'}
        name_and_type_dict_except = {'value': 'soap.jpg', 'type': 'string', 'other': 'something else'}
        # Act
        col_type, col_value = populate.get_name_and_type_of_column(name_and_type_dict)
        # Assert
        self.assertEqual(col_type, 'string')
        self.assertEqual(col_value, 'soap.jpg')
        self.assertRaises(Exception, lambda: populate.get_name_and_type_of_column(name_and_type_dict_except))

    def test_make_table_values(self):
        # Arrange
        row_list = [
            {'type': 'string', 'value': 'some value'},
            {'type': 'int', 'value': 1234},
            {'type': 'double', 'value': 12.34}
        ]

        row_values = ['"some value"', 1234, 12.34]

        # Act
        row_values_test = populate.make_table_values(row_list)
        # Assert
        self.assertEqual(row_values_test, row_values)

    def test_make_join_string(self):
        # Arrange
        column_values = ["T_ID", "T_NAME", "T_TQY", "T_PRICE"]
        column_values_str_test = "T_ID, T_NAME, T_TQY, T_PRICE"
        # Act
        column_values_str = populate.make_join_string(column_values)
        # Assert
        self.assertEqual(column_values_str_test, column_values_str)

    def test_extract_table_name(self):
        # Arrange
        sql_string = '"CREATE TABLE USER ( U_ID MEDIUMINT NOT NULL AUTO_INCREMENT ) ENGINE=InnoDB"'
        dict_test = {'USER': '"CREATE TABLE USER ( U_ID MEDIUMINT NOT NULL AUTO_INCREMENT ) ENGINE=InnoDB"'}

        # Act
        dict = populate.extract_table_name(sql_string)

        # Assert
        self.assertEqual(dict, dict_test)

    def test_extract_rows_and_columns_names(self):
        # Arrange
        row_dict = {
            'T_LDESC': {
                'value': 'Lorem ipsum dolor sit amet',
                'type': 'string'
            },
            'T_STATUS': {
                'value': 'In Stock',
                'type': 'string'
            },
            'T_SDESC': {
                'value': 'Short Description',
                'type': 'string'
            },
            'T_SKU': {
                'value': 1234,
                'type': 'int'
            },
            'T_PRICE': {
                'value': 12.34,
                'type': 'int'
            }
        }

        row_dict = collections.OrderedDict(row_dict)

        rows = [
            {
                'value': 'Lorem ipsum dolor sit amet',
                'type': 'string'
            },
            {
                'value': 'In Stock',
                'type': 'string'
            },
            {
                'value': 'Short Description',
                'type': 'string'
            },
            {
                'value': 1234,
                'type': 'int'
            },
            {
                'value': 12.34,
                'type': 'int'
            }
        ]

        columns = ['T_LDESC', 'T_STATUS', 'T_SDESC', 'T_SKU', 'T_PRICE']

        # Act
        columns_test, rows_test = populate.extract_rows_and_columns_names(row_dict)
        # since dictionaries are unordered, extract_rows_and_columns_names will
        # return unordered list, convert the lists to sets, so you can check items
        columns_test = set(columns_test)
        columns = set(columns)

        pairs_lists = zip(rows, rows_test)

        # Assert
        self.assertEqual(columns,columns_test)
        self.assertTrue(any(x != y for x, y in pairs_lists))

