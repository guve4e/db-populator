

class MongoDataConverter(super):
    def __init__(self) -> None:
        super().__init__()

    @classmethod
    def __remove_first_two_characters(cls, string: str):
        return string[:2]

    @classmethod
    def __extract_key(cls, sql_key):
        return MongoDataConverter.__remove_first_two_characters(sql_key).lower()

    @classmethod
    def __extract_value(cls, sql_value: {}):
        return sql_value['value']

    @classmethod
    def __convert_row(cls, row: {}):

        out = {}

        for key, val in row:
            v = MongoDataConverter.__extract_value(val)
            k = MongoDataConverter.__extract_key(key)
            out[v] = k

        return out

    def get_converted_data(self):
        pass

    def sql_json_to_mongo_json(self, table_name: str, sql_json: []):

        table = []

        for row in sql_json:
            r = MongoDataConverter.__convert_row(row)


    def mongo_json_to_sql_json(self, mongo_json: {}):
        pass