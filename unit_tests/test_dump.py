#!/usr/bin/python3
import unittest
import dump
import collections


class DumpTestCase(unittest.TestCase):

    def setUp(self):

        self.info = [
            {
                'COLUMN_NAME': 'ET_ID',
                'COLUMN_TYPE': 'mediumint(9)',
                'EXTRA': 'auto_increment'
            },
            {
                'COLUMN_NAME': 'U_ID',
                'COLUMN_TYPE': 'mediumint(9)',
                'EXTRA': ''
            },
            {
                'COLUMN_NAME': 'ET_NAME',
                'COLUMN_TYPE': 'varchar(128)', 'EXTRA': ''},
            {
                'COLUMN_NAME': 'ET_DESCRIPTION',
                'COLUMN_TYPE': 'text',
                'EXTRA': ''
            },
            {
                'COLUMN_NAME': 'ET_TYPE',
                'COLUMN_TYPE': 'varchar(64)',
                'EXTRA': ''
            }
        ]

    def test_create_entry_dict(self):
        # Arrange
        expected_entry = {
            "type": "string",
            "value": "8S24TY"
        }

        # Act
        actual = dump.create_entry_dict("string", "8S24TY")

        # Assert
        self.assertEqual(expected_entry, actual)

    def test_remove_auto_incremented_values(self):

        # Arrange
        values = [
            {
                'ET_ID': 1,
                'U_ID': 1001,
                'ET_NAME': 'Rent',
                'ET_DESCRIPTION': 'Monthly Rent.',
                'ET_TYPE': 'CONSTANT'
            },
            {
                'ET_ID': 2,
                'U_ID': 1001,
                'ET_NAME': 'Electric',
                'ET_DESCRIPTION': 'Electric Bill.',
                'ET_TYPE': 'CONSTANT'
            }
        ]

        expected = [
            {
                'U_ID': 1001,
                'ET_NAME': 'Rent',
                'ET_DESCRIPTION': 'Monthly Rent.',
                'ET_TYPE': 'CONSTANT'
            },
            {
                'U_ID': 1001,
                'ET_NAME': 'Electric',
                'ET_DESCRIPTION': 'Electric Bill.',
                'ET_TYPE': 'CONSTANT'
            }
        ]

        # Act
        actual = dump.remove_auto_incremented_values(self.info, values)

        # Assert
        self.assertEqual(expected, actual)

    def test_determine_type(self):
        # Arrange

        # Act
        mediumint = dump.determine_type("mediumint")
        decimal = dump.determine_type("decimal(4,2")
        integer = dump.determine_type("int")
        varchar = dump.determine_type("varchar(128)")
        text = dump.determine_type("text")
        date = dump.determine_type("date")
        timestamp = dump.determine_type("timestamp")

        # Assert
        self.assertEqual("int", mediumint)
        self.assertEqual("int", decimal)
        self.assertEqual("int", integer)
        self.assertEqual("string", varchar)
        self.assertEqual("string", text)
        self.assertEqual("string", date)
        self.assertEqual("string", timestamp)


    def test_create_json_list(self):
        # Arrange
        values = [
            {
                'ET_ID': 1,
                'U_ID': 1001,
                'ET_NAME': 'Rent',
                'ET_DESCRIPTION': 'Monthly Rent.',
                'ET_TYPE': 'CONSTANT'
            },
            {
                'ET_ID': 2,
                'U_ID': 1001,
                'ET_NAME': 'Electric',
                'ET_DESCRIPTION': 'Electric Bill.',
                'ET_TYPE': 'CONSTANT'
            }
        ]

        expected = [
            {
                'U_ID': {
                        'type': 'int',
                        'value': 1001
                    },
                'ET_NAME': {
                    'type': 'string',
                    'value': 'Rent'
                },
                'ET_DESCRIPTION': {
                    'type': 'string',
                    'value': 'Monthly Rent.'
                },
                'ET_TYPE': {
                    'type': 'string',
                    'value': 'CONSTANT'
                }
            },
            {
                'U_ID': {
                        'type': 'int',
                        'value': 1001
                    },
                'ET_NAME': {
                        'type': 'string',
                        'value': 'Electric'
                    },
                'ET_DESCRIPTION': {
                    'type': 'string',
                    'value': 'Electric Bill.'
                },
                'ET_TYPE': {
                    'type': 'string',
                    'value': 'CONSTANT'
                }
            }
        ]

        # Act
        actual_list = dump.create_json_list(self.info, values)

        # Assert
        self.assertEqual(expected, actual_list)