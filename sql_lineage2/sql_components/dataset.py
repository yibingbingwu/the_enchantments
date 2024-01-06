from typing import List, Optional, Dict

from sql_lineage2.sql_components.column import Column, TableColumn


class Dataset(object):
    def __init__(self):
        self.column_pools: Dict[str, List[Column]] = dict()

    def get_all_columns(self, alias: Optional[str]) -> List[Column]:
        if alias:
            return self.column_pools[alias]

        retval = []
        for k, v in self.column_pools.items():
            retval.extend(v)
        return retval

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

        if alias_idx:
            t_cols = self.column_pools[alias_idx]
            for c in t_cols:
                if c.known_as == col_name:
                    return c

        for k, v in self.column_pools.items():
            if alias_idx and k == alias_idx:
                continue

            for c in v:
                if c.known_as == col_name:
                    return c

        return None

    def set_columns_from_table(self,
                               namespace: Optional[str],
                               schema: Optional[str],
                               tab_name: str,
                               alias: Optional[str],
                               cols: List[TableColumn]):
        col_objs = [Column.from_table(namespace, schema, tab_name, tc) for tc in cols]
        self.column_pools[alias] = col_objs
        self.column_pools[tab_name] = col_objs
        if schema:
            self.column_pools[schema + '.' + tab_name] = col_objs

    def add_columns_from_select(self,
                                alias: Optional[str],
                                cols: List[Column]):
        self.column_pools[alias] = cols if alias not in self.column_pools else self.column_pools[alias].extend(cols)
