import copy
import json
import os.path
from typing import Optional, List, Union

from jsonschema import validate

from sql_lineage2 import KW_NID
from sql_lineage2.sql_components.column import TableColumn
from sql_lineage2.util.maplist import MapList


def _replace_col_obj(table_def: dict):
    tab_cols = table_def.pop('columns')
    new_objs = [TableColumn(name=x['column'], type=x['type']) for x in tab_cols]
    table_def['columns'] = new_objs


class DbCatalog(object):
    def __init__(self, levels: int, default_namespace: Optional[str] = None):
        self.levels = levels if levels else 3
        assert self.levels in (2, 3), "Only supports two kinds of Levels: MySQL-style and Snowflake-style"

        self.namespace = default_namespace
        self.dataset: Optional[str] = None

        curr_dir = os.path.dirname(__file__)
        schema_fn = f"{curr_dir}{os.path.sep}catalog_schema.json"
        with open(schema_fn, 'r') as fin:
            self.ctg_schema_dict = json.load(fin)
        self.root = None

    def setup_store(self, ctg: list):
        validate(ctg, schema=self.ctg_schema_dict)
        self.root = ctg
        for ns in self.root:
            for ds in ns.get('datasets', []):
                for tb in ds.get('tables', []):
                    _replace_col_obj(tb)

    def add_table(self, new_ds: str, new_tab: str, new_cols: List[TableColumn], new_ns: str = None):
        ns_val = new_ns or self.namespace
        ns_struct = None
        for x in self.root:
            if x['namespace'] == ns_val:
                ns_struct = x
                break

        if not ns_struct:
            ns_struct = {
                'namespace': ns_val,
                'datasets': []
            }
            self.root.append(ns_struct)

        ds_struct = None
        for y in ns_struct['datasets']:
            if y.get('dataset', '') == new_ds:
                ds_struct = y
                break

        if not ds_struct:
            ds_struct = {
                'dataset': new_ds,
                'tables': []
            }
            ns_struct['datasets'].append(ds_struct)

        for i, z in enumerate(ds_struct['tables']):
            if z.get('table', '') == new_tab:
                del ds_struct['tables'][i]
                break

        tab_struct = {'table': new_tab,
                      'columns': new_cols
                      }
        ds_struct['tables'].append(tab_struct)

    def use_namespace(self, ns: str):
        assert ns, "Namespace/Project name cannot be NULL"
        self.namespace = ns

    def use_dataset(self, ds: str):
        assert ds, "Dataset/Schema name cannot be NULL"
        self.dataset = ds

    def find_table(self, fq_key: Optional[str] = None, fq_arr: Optional[list] = None) -> Optional[dict]:
        names = fq_key.split('.') if fq_arr is None else fq_arr
        fq_names = self.qualify_db_obj_name(input_v=names, target_levels=3)
        curr_ns: Optional[list] = None
        curr_ds: Optional[list] = None
        for i, n in enumerate(fq_names):
            if i == 0:
                for x in self.root:
                    _ns = x.get('namespace', None)
                    _an = n or self.namespace
                    if (_ns is None and _an is None) or (_ns and _an and _ns == _an):
                        curr_ns = x.get('datasets', [])
                        continue
                if not curr_ns:
                    return None
                continue

            if i == 1:
                for x in curr_ns:
                    _ds = x.get('dataset', None)
                    if n == _ds:
                        curr_ds = x.get('tables', [])
                        continue
                if not curr_ds:
                    return None
                continue

            if i == 2:
                for x in curr_ds:
                    _tn = x.get('table', None)
                    if n == _tn:
                        retval = {'namespace': fq_names[0], 'dataset': fq_names[1]}
                        retval.update(x)
                        return retval
                return None

        return None

    def find_column(self, fq_key: str) -> Optional[dict]:
        fq_names = self.qualify_db_obj_name(input_v=fq_key, target_levels=4)
        tab_obj = self.find_table(fq_arr=fq_names[:-1])
        if tab_obj:
            cn = fq_key[-1]
            for x in tab_obj.get('columns', []):
                _tn = x.get('column', None)
                if cn == _tn:
                    return x

        return None

    def qualify_db_obj_name(self, input_v: Union[str, List[dict]] = None,
                            target_levels: int = -1):
        ret_arr = copy.deepcopy(input_v) if type(input_v) == list else input_v.split('.')
        if target_levels > 0:
            if target_levels - len(ret_arr) == 2:
                ret_arr.insert(0, self.dataset)
            if target_levels - len(ret_arr) == 1:
                ret_arr.insert(0, self.namespace)
        return ret_arr

# Test case in "test_cases"
