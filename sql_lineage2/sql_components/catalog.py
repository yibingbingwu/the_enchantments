import json
import os.path
from typing import Optional

from jsonschema import validate


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

    def use_dataset(self, ds: str):
        assert ds, "Dataset/Schema name cannot be NULL"
        self.dataset = ds

    def find_table(self, fq_key: Optional[str] = None, fq_arr: Optional[list] = None) -> Optional[dict]:
        names = fq_key.split('.') if fq_arr is None else fq_arr
        fq_names = self.pad_fq_name(names, 3)
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
                        return x
                return None

        return None

    def find_column(self, fq_key: str) -> Optional[dict]:
        names = fq_key.split('.')
        fq_names = self.pad_fq_name(names, 4)
        tab_obj = self.find_table(fq_arr=fq_names[:-1])
        if tab_obj:
            cn = fq_key[-1]
            for x in tab_obj.get('columns', []):
                _tn = x.get('column', None)
                if cn == _tn:
                    return x

        return None

    def pad_fq_name(self, in_arr: list, expected_level: int) -> list:
        if len(in_arr) == expected_level:
            return in_arr

        cl = len(in_arr)
        if cl == 1:
            assert self.dataset, "Cannot resolve a table name without a default Dataset/Schema name"
            in_arr.insert(0, self.dataset)

        in_arr.insert(0, self.ctg_schema_dict)
        return in_arr

# Test case in "test_cases"
