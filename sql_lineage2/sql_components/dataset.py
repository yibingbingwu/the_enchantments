class Dataset(object):
    def __init__(self, db: str = None, schema: str = None, tab_name: str = None):
        self.database = db
        self.schema = schema
        self.table_name = tab_name
        self.alias = None
        self.columns = []

    def reads_from_db(self) -> None:
        self._query_catalog(self.database, self.schema, self.table_name)

    def _query_catalog(self, database, schema, table_name):
        pass
