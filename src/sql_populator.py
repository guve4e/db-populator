from src.populator import Populator


class SqlPopulator(Populator):
    def __init__(self, db: str, data: [], connection) -> None:
        super().__init__()

    def create_schema(self):
        pass

    def populate(self):
        pass
