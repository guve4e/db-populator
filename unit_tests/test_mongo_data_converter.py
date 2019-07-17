#!/usr/bin/python3
import unittest
from src.mongo_data_converter import MongoDataConverter


class MongoDataConverterTest(unittest.TestCase):

    __sql_json = [
        {
            "U_FNAME": {
                "type": "string",
                "value": "John"
            },
            "U_LNAME": {
                "type": "string",
                "value": "Doe"
            },
            "U_HASH": {
                "type": "string",
                "value": "1@#4frg##fwf4"
            },
            "U_USERNAME": {
                "type": "string",
                "value": "john@gmail.com"
            },
            "U_PASSWORD": {
                "type": "string",
                "value": "john"
            },
            "U_TIMESTAMP": {
                "type": "string",
                "value": "2018-01-11 03:14:07"
            }
        },
        {
            "U_FNAME": {
                "type": "string",
                "value": "Daniel"
            },
            "U_LNAME": {
                "type": "string",
                "value": "Johnson"
            },
            "U_HASH": {
                "type": "string",
                "value": "1@#4frn7&^^^53"
            },
            "U_USERNAME": {
                "type": "string",
                "value": "daniel@gmail.com"
            },
            "U_PASSWORD": {
                "type": "string",
                "value": ")(%#^bbjid)"
            },
            "U_TIMESTAMP": {
                "type": "string",
                "value": "2018-02-11 01:12:17"
            }
        }
    ]

    __mongo_json = {
        "users": [
            {
                "fname": "john",
                "lname": "Doe",
                "hash": "1@#4frg##fwf4",
                "username": "john@gmail.com",
                "password": "john",
                "timestamp": "2018-01-11 03:14:07"
            },
            {
                "fname": "Daniel",
                "lname": "Johnson",
                "hash": "1@#4frn7&^^^53",
                "username": "daniel@gmail.com",
                "password": ")(%#^bbjid)",
                "timestamp": "2018-02-11 01:12:17"
            }
        ]
    }

    def setUp(self):
        pass

    def runTest(self):
        pass

    def tearDown(self):
        pass

    def test_sql_json_to_mongo_json_conversion(self):

        converter = MongoDataConverter()
        converter.sql_json_to_mongo_json("users", self.__sql_json)
        actual = converter.get_converted_data()

        self.assertEqual(actual, self.__mongo_json)

    def test_mongo_json_to_slq_json_conversion(self):

        converter = MongoDataConverter()
        converter.mongo_json_to_sql_json(self.__mongo_json)
        actual = converter.get_converted_data()

        self.assertEqual(actual, self.__sql_json)

