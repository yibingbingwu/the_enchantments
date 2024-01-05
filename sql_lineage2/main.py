import sys
from typing import Optional, Union, List

import sqlfluff

from sql_lineage2.sql_components.catalog import DbCatalog
from sql_lineage2.sql_components.column import Column
from sql_lineage2.sql_components.dataset import Dataset
from sql_lineage2.util.maplist import MapList

sys.setrecursionlimit(1024 * 1024)

"""
Reminder - method naming convention:
- _eval_    auto-matched to known statements
- proc_     need further/nested processing
- resolve_  always returns a ready dataset 
"""


class SqlLineage(object):
    IMPL_PREFIX = '_eval_'
    KNOWN_TYPES = ['statement',
                   'create_table_statement',
                   'select_statement',
                   'from_expression',
                   'join_on_condition',
                   ]
    KW_NID = 'naked_identifier'

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
        self.sql_stmts = sql_txt
        self.db_catalog: Optional[DbCatalog] = None
        self.lineage_graph: Optional[dict] = None
        self._check_impl()

    def setup_catalog(self, ctg: list, levels: Optional[int] = None, default_namespace: Optional[str] = None):
        self.db_catalog = DbCatalog(levels=levels, default_namespace=default_namespace)
        self.db_catalog.setup_store(ctg)

    def proc_statement(self, stmt_block: dict):
        assert len(stmt_block) == 1, "Unexpected Sqlfluff object structure"
        type, block = list(stmt_block.items())[0]
        if type in self.KNOWN_TYPES:
            func_name = f"{self.IMPL_PREFIX}{type}"
            proc_func = getattr(self, func_name, None)
            return proc_func(block)
        return None

    def proc_join_into_ds(self, stmt_parts: list, existing_ds: Dataset) -> Dataset:
        # Read past the JOIN keyword:
        fnd_join = False
        fnd_ds = False
        for i in stmt_parts:
            kw = list(i.keys())[0].lower()
            if kw in ['whitespace', 'newline']:
                continue

            if kw == 'keyword' and (i[kw]).lower() == 'join':
                fnd_join = True
                continue

            if fnd_join and kw == 'from_expression_element':
                fnd_ds = True
                ret_ds = self.resolve_from_element(i[kw])
                existing_ds.absorb_another(ret_ds)
                continue

            if fnd_ds and kw == 'join_on_condition':
                cond_cols = self.proc_statement(kw)
                existing_ds.add_join_columns(cond_cols.join_columns)
                continue

            if not fnd_join:
                continue

            assert False, "Unexpected JOIN pattern. Expecting JOIN from_expression_element join_on_condition"

        return existing_ds

    def _eval_statement(self, stmt_block: dict) -> Optional[Dataset]:
        return self.proc_statement(stmt_block)

    def _eval_create_table_statement(self, stmt_parts: Optional[list]) -> Optional[Dataset]:
        has_as = False
        for p in stmt_parts:
            if 'keyword' in p and p['keyword'].lower() == 'as':
                has_as = True
                continue

            if has_as and 'select_statement' in p:
                rs = self.proc_statement(p)
                return rs

        assert True, 'Unknown create table statement type'
        return None

    def _eval_join_on_condition(self, stmt_parts: dict) -> List[str]:
        pass

    def _eval_select_statement(self, stmt_parts: dict) -> Optional[Dataset]:
        if 'select_clause' in stmt_parts and 'from_clause' in stmt_parts:
            return self.proc_basic_select(stmt_parts)
        return None

    def _eval_from_expression(self, stmt_parts: Union[dict, List[dict]]) -> Dataset:
        stmts = MapList(stmt_parts)
        ret_ds = self.resolve_from_element(stmts.get('from_expression_element'))
        join_parts = stmts.get('join_clause')
        if join_parts:
            for j in join_parts:
                ret_ds = self.proc_join_into_ds(j, ret_ds)
        return ret_ds

    def proc_basic_select(self, stmt_parts: dict) -> Optional[Dataset]:
        from_parts = MapList.to_list(stmt_parts['from_clause'])

        source_rs = None
        for f in from_parts:
            if 'from_expression' in f:
                source_rs = self.proc_statement(f)
                break

        if source_rs:
            ret_ds = Dataset()
            column_list = []
            sel_cols = MapList.to_list(stmt_parts['select_clause'])
            exp_list = [x['select_clause_element'] for x in sel_cols if 'select_clause_element' in x]
            for cn in exp_list:
                col_objs = self.resolve_column_exp(exp=cn, src_ds=source_rs)
                column_list.extend(col_objs)

            # for cn in column_list:
            #     col_obj = source_rs.resolve_name(cn)
            #     for pc in col_obj:
            #         ret_ds.select_columns.append(Column.build_from(pc))

            return ret_ds

        return None

    def resolve_from_element(self, stmt_parts: Union[dict, List[dict]]) -> Optional[Dataset]:
        ret_ds = None
        stmts = MapList(stmt_parts)

        tab_alias = None
        if 'alias_expression' in stmt_parts:
            tab_alias = stmt_parts['alias_expression'][self.KW_NID]

        if 'table_expression' in stmts:
            tab_refs = MapList(stmts.get('table_expression'))
            tab_parts = tab_refs.get('table_reference')[0]
            _names = [x[self.KW_NID] for x in tab_parts if self.KW_NID in x]
            _len = len(_names)
            for x in range(_len, 3):
                _names.insert(0, None)

            ret_ds = Dataset()
            tab_by_name = self.db_catalog.find_table(fq_arr=_names)
            tab_columns = tab_by_name.get('columns', [])
            ret_ds.add_columns_from_table(db=_names[0],
                                          schema=_names[1],
                                          tab_name=_names[2],
                                          alias=tab_alias,
                                          cols=tab_columns)

        return ret_ds

    def parse_sql(self, sql_txt: str = None):
        if sql_txt:
            self.sql_stmts = sql_txt
        parsed_raw = sqlfluff.parse(self.sql_stmts, dialect="snowflake")
        mapped_rs = MapList(parsed_raw)
        assert 'file' in mapped_rs, "Unknown Sqlfluff parsing output"

        retval = None
        for stmt in mapped_rs.get('file'):
            retval = self.proc_statement(stmt)

        print('Done Parsing')
        return retval

    def expand_wildcard_cols(self, col_exp: dict, src_ds: Dataset) -> list:
        return []

    def unwind_col_ref(self, c: list):
        l = [x.popitem()[1] for x in c]
        return ''.join(l)

    def resolve_column_exp(self, exp: dict, src_ds: Dataset) -> List[Column]:
        retval = []
        if 'wildcard_expression' in exp:
            wc_id = exp['wildcard_expression']['wildcard_identifier']
            tab_alias = wc_id.get('naked_identifier', None)
            return src_ds.get_all_col_names(alias=tab_alias)

        elif 'function' in exp:
            ret_list = []
            func_params = MapList.to_list(exp['function']['bracketed'])
            for e in func_params:
                if 'expression' in e:
                    sub_exp = e['expression']
                    nested_rs = self.resolve_column_exp(sub_exp, src_ds)
                    ret_list.extend(nested_rs)
            return ret_list

        elif 'column_reference' in exp:
            retval = self.unwind_col_ref(exp['column_reference'])
            return [retval]

        elif 'star' in exp:
            # This is the count(*) case -- ignore for now
            return []

        else:
            raise NotImplementedError(f"Unknown expression {exp}")

        if 'alias_expression' in exp:
            alias = exp['alias_expression']


if __name__ == '__main__':
    # with open(sys.argv[1], 'r') as fin:
    #     in_query = fin.read()
    test_sql = """create table test_schema.z as
        SeLEct  a.*, 1, b.blah as fOO
        from mySchema.myTable a
        left JOIN   
        mySchema.yTable b on 
        a.x=b.y
        full outer join (
        select x from mySchema.zTable) z on a.x=z.x
        where blah!='abc';
                        
        select * from test1.test2;"""
    psr = SqlLineage(test_sql)
    psr.parse_sql()
