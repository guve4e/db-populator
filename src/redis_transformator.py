

class RedisTransformator(object):

    def __init__(self, value)-> None:
        super().__init__()

        self.__value = value


    def set_name(self, name: str):
        return self

    def set_key(self, key: str)-> None:
        pass

    def set_value(self, value)-> None:
        pass

    def get_redis_data_structures(self):
        pass

