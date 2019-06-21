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

    def test_foo(self):

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