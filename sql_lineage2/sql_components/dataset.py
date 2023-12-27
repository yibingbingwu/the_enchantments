from typing import List


class Dataset(object):
    def __init__(self, db: str = None, schema: str = None, tab_name: str = None):
        self.namespace = db
        self.dataset = schema
        self.table_name = tab_name
        self.alias = None
        self.select_columns = []
        self.join_columns = []
        self.where_columns = []

    def get_all_columns(self, alias: str) -> List[str]:
        return []

    def add_join_columns(self, cols: list) -> None:
        pass

    def absorb_another(self, another_ds) -> None:
        pass

    def resolve_name(self, col_name: str) -> list:
        return []
