#!/usr/bin/python3
import unittest
from unittest import TestCase, mock
from src.redis_transformator import RedisTransformator


class RedisTransformatorTest(unittest.TestCase):
    def setUp(self):
        pass

    def runTest(self):
        pass

    def tearDown(self):
        pass

    def test_append(self):

        complex_object = [
            {
                "date": {
                    "date": "2019.05.12",
                    "time": {
                        "hours": 12,
                        "minutes": 45
                    }
                },
                "temperature": 12,
                "humidity": 84
            },
            {
                "date": {
                    "date": "2019.05.13",
                    "time": {
                        "hours": 17,
                        "minutes": 23
                    }
                },
                "temperature": 19.5,
                "humidity": 74.3
            }
        ]

        expected_data_structure = [
            [
                {
                    "time:1": {
                        "hours": 12,
                        "minutes": 45
                    }
                },
                {
                    "time:2": {
                        "hours": 17,
                        "minutes": 23
                    }
                }
            ],
            [
                {
                    "date:1": {
                        "date": "2019.05.12",
                        "time": "time:1"
                    }
                },
                {
                    "date:2": {
                        "date": "2019.05.13",
                        "time": "time:2"
                    }
                }
            ],
            [
                {
                    "object:1": {
                        "date": "object:1:date1",
                        "temperature": 12,
                        "humidity": 84
                    }
                },
                {
                    "object:2": {
                        "date": "object:1:date2",
                        "temperature": 19.5,
                        "humidity": 74.3
                    }
                }
            ]
        ]

        store = RedisTransformator(complex_object)
        store.set_name('object')
        
        actual_data_structure = store.get_redis_data_structures()

        self.assertEqual(expected_data_structure, actual_data_structure)
