from src.populator import Populator
from src.mongo import Mongo

class MongoPopulator(Populator):
    def __init__(self, db: str, data: [], connection) -> None:
        super().__init__()
        self.__db = db
        self.__data = data
        self.__client = self.__retrieve_connection(connection, db)
        pass

    def __retrieve_connection(self, connection: Mongo, db: str):
        # We want to create new db
        if db in connection.list_databases():
            self.__client = connection.drop_database(db)

        return connection.get_database(db)

    def create_schema(self):
        pass

    def populate(self):

        global col, o

        if isinstance(self.__data, list):
            for collection in self.__data:
                try:
                    col = self.__client[collection]
                    col.insert_many(collection)
                except Exception as e:
                    print(e)
        else:
            for collection in self.__data:
                try:
                    col = self.__client[collection]
                    o = self.__data[collection]

                    col.insert_one(o)

                except TypeError as e:
                    col.insert_many(o)
