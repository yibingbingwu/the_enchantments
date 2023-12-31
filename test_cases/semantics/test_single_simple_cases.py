from sql_lineage2.main import SqlLineage
from sql_lineage2.sql_components.column import Column
from sql_lineage2.sql_components.dataset import Dataset

CTG = [
    {
        'namespace': 'ns_a',
        'datasets': [
            {
                'dataset': 'ds_a',
                'tables': [
                    {
                        'table': 'tab_a',
                        'columns': [
                            {
                                'column': 'col_a1',
                                'type': 'int',
                            },
                            {
                                'column': 'col_a2',
                                'type': 'str',
                            },
                            {
                                'column': 'col_a3',
                                'type': 'str',
                            },
                        ]
                    },
                    {
                        'table': 'tab_b',
                        'columns': [
                            {
                                'column': 'col_b1',
                                'type': 'long',
                            },
                            {
                                'column': 'col_b2',
                                'type': 'str',
                            },
                            {
                                'column': 'col_b3',
                                'type': 'str',
                            },
                        ]
                    },
                ]
            }
        ]
    }
]

psr = SqlLineage()
psr.setup_catalog(CTG, default_namespace='ns_a')


def test_basic_select_001():
    sql = """select * from ds_a.tab_a"""
    rs = psr.parse_sql(sql)
    assert (type(rs) == Dataset and
            len(rs.column_pools.keys()) == 1 and
            list(rs.column_pools.keys())[0] is None and
            len(rs.column_pools[None]) == 3 and
            type(rs.column_pools[None][0]) == Column and
            rs.column_pools[None][0].known_as == 'col_a1')


def test_basic_select_002():
    sql = """select *, a.*, e.f f0, foo(tee(*), z.y, k.f) from ds_a.tab_a a"""
    rs = psr.parse_sql(sql)
    assert True, "Use this one to test parsing results"


def test_basic_lineage_001():
    sql = """create table ds_a.tab_new_01 as select tab_a.* from ds_a.tab_a"""
    rs = psr.parse_sql(sql)
    assert type(rs) == Dataset and rs.dataset == 'ds_a' and rs.table_name == 'tab_a' and \
           len(rs.select_columns) == 3, "Test failed"
