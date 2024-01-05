from typing import List, Optional, Dict

from sql_lineage2.sql_components.column import Column, TableColumn


class Dataset(object):
    def __init__(self, db: str = None, schema: str = None, tab_name: str = None):
        # self.namespace = db
        # self.dataset = schema
        # self.table_name = tab_name
        # self.alias = None
        # self.select_columns = []
        # self.join_columns = []
        # self.where_columns = []
        self.column_pools = dict()
        self.alias_map = dict()

    def get_all_columns(self, alias: Optional[str]) -> List[Column]:
        prefx = (alias + '.') if alias else ''
        return self.column_pools[prefx]

    def add_join_columns(self, cols: list) -> None:
        pass

    def absorb_another(self, another_ds) -> None:
        pass

    def resolve_name(self, col_name: str) -> Optional[Column]:
        col_parts = col_name.split('.')
        if len(col_parts) == 3:
            alias_idx = col_parts[0] + '.' + col_parts[1]
            col_name = col_parts[2]
        elif len(col_parts) == 2:
            alias_idx = col_parts[0]
            col_name = col_parts[1]
        elif len(col_parts) == 1:
            alias_idx = None
            col_name = col_parts[0]
        else:
            raise NotImplementedError('Unknown column expression. Expecting only fewer than three names')

        return self.column_pools[alias_idx].get(col_name, None)

    def add_columns_from_table(self,
                               namespace: Optional[str],
                               schema: Optional[str],
                               tab_name: str,
                               alias: Optional[str],
                               cols: List[TableColumn]):
        self.column_pools.pop(alias, None)
        col_objs = [Column.from_table(namespace, schema, tab_name, tc) for tc in cols]
        self.column_pools[alias] = col_objs
        self.column_pools[tab_name] = col_objs
        if schema:
            self.column_pools[schema + '.' + tab_name] = col_objs
