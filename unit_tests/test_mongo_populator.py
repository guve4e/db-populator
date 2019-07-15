#!/usr/bin/python3
import unittest
from src.mongo_populator import MongoPopulator
from src.mongo import Mongo

class MongoPopulatorTest(unittest.TestCase):

    def setUp(self):
        pass

    def runTest(self):
        pass

    def tearDown(self):
        pass

    def test_mongo_populator_with_object(self):

        data = {
                   "testCollection1": {
                       "key": "Value"
                   },
                    "testCollection2": [
                        {
                            "key": "Value"
                        },
                        {
                            "key1": "Value1"
                        },
                        {
                            "key2": "Value2"
                        }
                    ],
                    "testCollection3": {
                        "key": {
                            "key": {
                                "key": "value"
                            }
                        }
                    },
                    "testCollection4": {
                        "key": {
                            "key": [
                                {
                                    "key": "value"
                                },
                                {
                                    "key": "value"
                                },
                                {
                                    "key": "value"
                                }
                            ],
                        }
                    }
            }

        productionMongo = Mongo("mongodb://localhost:27017/")

        a = MongoPopulator("SABAAA", data, productionMongo)
        a.create_schema()
        a.populate()

        productionMongo.drop_database("SABAAA")

    def test_mongo_populator_with_list(self):

        data = { "users": [
                {
                    "U_FNAME":  {
                        "type": "string",
                        "value": "John"
                    },
                    "U_LNAME":  {
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
                    "U_FNAME":  {
                        "type": "string",
                        "value": "Daniel"
                    },
                    "U_LNAME":  {
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
        }

        productionMongo = Mongo("mongodb://localhost:27017/")

        a = MongoPopulator("SABAAA", data, productionMongo)
        a.create_schema()
        a.populate()

