import pymongo

class Mongo(super):
    def __init__(self, connection_str: str) -> None:
        super().__init__()

        self.__connection = pymongo.MongoClient(connection_str)

    def drop_database(self, db: str):
        self.__connection.drop_database(db)

    def get_database(self, db: str):
        return self.__connection[db]

    def list_databases(self)-> []:
        return self.__connection.list_database_names()

    def insert_one(self, obj):
        self.__connection.insert_one(obj)

    def insert_many(self, obj: {}):
        self.__connection.insert_many(obj)