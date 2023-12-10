class Dataset(object):
    def __init__(self, db: str = None, schema: str = None, tab_name: str = None):
        self.namespace = db
        self.dataset = schema
        self.table_name = tab_name
        self.alias = None
        self.select_columns = []
        self.join_columns = []
        self.where_columns = []

    def reads_from_db(self) -> None:
        self._query_catalog(self.namespace, self.dataset, self.table_name)

    def add_join_columns(self, cols: list) -> None:
        pass

    def absorb_another(self, another_ds) -> None:
        pass

    def _query_catalog(self, database, schema, table_name):
        pass
