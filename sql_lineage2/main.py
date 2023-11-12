import sys
from typing import Optional

import sqlfluff

from sql_lineage2.sql_components.dataset import Dataset

sys.setrecursionlimit(1024 * 1024)


class SqlLineage(object):
    IMPL_PREFIX = '_proc_'
    KNOWN_TYPES = ['statement',
                   'create_table_statement',
                   'select_statement',
                   'from_expression',
                   'from_expression_element',
                   ]

    def _check_impl(self):
        missing_impl = []
        for t in self.KNOWN_TYPES:
            func_name = f"{self.IMPL_PREFIX}{t}"
            proc_func = getattr(self, func_name, None)
            if not proc_func:
                missing_impl.append(func_name)

        assert len(missing_impl) == 0, f"Missing SQL processing functions: {str(missing_impl)}"

    def __init__(self, sql_txt: str = None):
        self.context = {}
        self.sql_stmts = sql_txt or """create table test_schema.z as
        SeLEct  a.*, 1, b.blah as fOO
        from mySchema.myTable a
        left join mySchema.yTable b on a.x=b.y
        where blah!='abc';
                        
        select * from test1.test2;"""

        self._check_impl()

    def evaluate_statement(self, stmt_block: Optional[dict]):
        assert len(stmt_block) == 1, "Unexpected Sqlfluff object structure"
        type, block = list(stmt_block.items())[0]
        if type in self.KNOWN_TYPES:
            func_name = f"{self.IMPL_PREFIX}{type}"
            proc_func = getattr(self, func_name, None)
            proc_func(block)

    def evaluate_joins(self, stmt_block: Optional[dict]) -> Optional[Dataset]:
        return None

    def _proc_statement(self, stmt_block: Optional[dict]) -> Optional[Dataset]:
        return self.evaluate_statement(stmt_block)

    def _proc_create_table_statement(self, stmt_parts: Optional[list]) -> Optional[Dataset]:
        for p in stmt_parts:
            if 'select_statement' in p:
                return self.evaluate_statement(p)

        assert True, 'Unknown create table statement type'
        return None

    def _proc_select_statement(self, stmt_parts: Optional[dict]) -> Optional[Dataset]:
        if 'select_clause' in stmt_parts and 'from_clause' in stmt_parts:
            return self._resolve_basic_select(stmt_parts)
        return None

    def _proc_from_expression(self, stmt_parts: Optional[dict]) -> Optional[Dataset]:
        ret_ds = self._resolve_from_element(stmt_parts['from_expression_element'])
        if 'join_clause' in stmt_parts:
            join_ds = self.evaluate_joins(stmt_parts['join_clause'])
            ret_ds = ret_ds.merge_datasets(join_ds)

        return ret_ds

    def _proc_from_expression_element(self, stmt_parts: Optional[dict]) -> Optional[Dataset]:
        return None

    def _resolve_basic_select(self, stmt_parts: Optional[dict]) -> Optional[Dataset]:
        for f in stmt_parts['from_clause']:
            if 'from_expression' in f:
                return self.evaluate_statement(f)
        return None

    def _resolve_from_element(self, stmt_parts: Optional[dict]) -> Optional[Dataset]:
        ret_ds = None
        if 'table_expression' in stmt_parts:
            tab_parts = stmt_parts['table_expression']['table_reference']
            _names = [x for x in tab_parts if 'naked_identifier' in x]
            _len = len(_names)
            for x in range(_len, 3):
                _names.insert(0, None)
            ret_ds = Dataset(db=_names[0], schema=_names[1], tab_name=_names[2])
            ret_ds.reads_from_db()

        if 'alias_expression' in stmt_parts:
            ret_ds.alias = stmt_parts['alias_expression']['naked_identifier']

        return ret_ds

    def parse_sql(self):
        parsed_raw = sqlfluff.parse(self.sql_stmts, dialect="snowflake")
        assert ('file' in parsed_raw and
                type(parsed_raw['file']) == list), "Unknown Sqlfluff parsing output"

        for stmt in parsed_raw['file']:
            self.evaluate_statement(stmt)

        print('OK')


if __name__ == '__main__':
    # with open(sys.argv[1], 'r') as fin:
    #     in_query = fin.read()
    psr = SqlLineage()
    psr.parse_sql()
