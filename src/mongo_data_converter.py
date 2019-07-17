

class MongoDataConverter(super):
    def __init__(self) -> None:
        super().__init__()
        self.__table = []

    @classmethod
    def __append_sql_row_name(cls, name):
        name = name.upper()
        name = 'U_' + name
        return name

    @classmethod
    def __remove_first_two_characters(cls, string: str):
        return string[2:]

    @classmethod
    def __extract_key(cls, sql_key):
        return MongoDataConverter.__remove_first_two_characters(sql_key).lower()

    @classmethod
    def __extract_value(cls, sql_value: {}):
        return sql_value['value']

    @classmethod
    def __add_metadata(cls, val):
        #TODO more intelligents here
        #TODO extract class that does this
        try:
            val = float(val)
            return {
                "type": "decimal",
                "value": val
            }
        except ValueError:
            return {
                "type": "string",
                "value": val
            }

    def __construct_rows(self, rows: []):
        for row in rows:
            dictionary = {}
            for key, val in row.items():
                key = MongoDataConverter.__append_sql_row_name(key)
                val_dict = MongoDataConverter.__add_metadata(val)
                dictionary[key] = val_dict

            self.__table.append(dictionary)

    @classmethod
    def __convert_row(cls, row: {}):

        out = {}

        for key, val in row.items():
            sanitized_value = MongoDataConverter.__extract_value(val)
            sanitized_key = MongoDataConverter.__extract_key(key)
            out[sanitized_key] = sanitized_value

        return out

    def sql_json_to_mongo_json(self, table_name: str, sql_json: []):

        for row in sql_json:
            self.__table.append(MongoDataConverter.__convert_row(row))

        return {
            table_name: self.__table
        }

    def mongo_json_to_sql_json(self, mongo_json: {}):

        for key, val in mongo_json.items():
            self.__construct_rows(val)

        return self.__table
