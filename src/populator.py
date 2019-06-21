from abc import abstractmethod
from abc import ABC

class Populator(ABC):

    @abstractmethod
    def create_schema(self):
        pass

    @abstractmethod
    def populate(self):
        pass


