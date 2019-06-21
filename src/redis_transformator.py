

class RedisTransformator(object):

    def __init__(self, value)-> None:
        super().__init__()
        self.__name = None
        self.__value = value
        self.__hashes_list = []

    def set_name(self, name: str):
        self.__name = name

        if self.__name is None:
            raise ValueError("RadisTransformator needs valid name!")

        return self

    def set_key(self, key: str)-> None:
        pass

    def set_value(self, value)-> None:
        pass

    def get_redis_data_structures(self):
        self.__flatten_object()
        return self.__hashes_list

    def __flatten_object(self):
        i = 0
        last_dict = {}
        l = []

        def flatten(x, name=''):
            nonlocal i, last_dict

            if type(x) is dict:
                last_dict = {}
                for dict_element in x:
                    flatten(x[dict_element], dict_element)
                self.__make_flat_object(last_dict, name, i)

                last_dict = {}
                pass

            elif type(x) is list:
                # walk to the end
                for list_element in x:
                    flatten(list_element)
                    i += 1
            else:
                last_dict[name] = x
                return

        flatten(self.__value)
        pass

    def __make_flat_object(self, dictionary, name: str, i: int):
        if not name:
            name = 'object'

        self.__hashes_list.append({
            "{}:{}".format(name, i): dictionary
        })