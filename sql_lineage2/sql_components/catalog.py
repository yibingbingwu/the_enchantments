namespace_schema = {
    "type": "object",
    "properties": {
        "namespace": {"type": "string"},
        "datasets": {
            "type": "array",
            "items": {"type": "object"},
        },
    }
}

dataset_schema = {
    "type": "object",
    "properties": {
        "dataset": {"type": "string"},
        "tables": {
            "type": "array",
            "items": {"type": "object"},
        },
    }
}

table_schema = {
    "type": "object",
    "properties": {
        "name": {"type": "string"},
        "columns": {
            "type": "array",
            "items": {"type": "object"},
        },
    }
}

column_schema = {
    "type": "object",
    "properties": {
        "name": {"type": "string"},
        "type": {"type": "string"},
        "misc": {"type": "string"},
    }
}

from jsonschema import validate


class DataCatalog(object):
    def __init__(self, levels: int = 3, default_namespace: str = None):
        assert levels in (2, 3), "Only supports two kinds of Levels: MySQL-style and Snowflake-style"
        self.levels = levels
        self.namespace = default_namespace
        self.root = dict()

    def setup_store(self, ctg: list):
        assert type(ctg) == list, "Unexpected Catalog datatype: must be a List"
        ds_list = []
        if self.levels == 3:
            for ns in ctg:
                validate(ns, schema=namespace_schema)
                ds_list.extend(ns.get('datasets'))

        else:
            ds_list = ctg

        for ds in ds_list:
            validate(ds, schema=dataset_schema)

            for tab in ds.get('tables'):
                validate(tab, schema=table_schema)

                for col in tab.get('columns'):
                    validate(col, schema=column_schema)
