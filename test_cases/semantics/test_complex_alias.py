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


def test_complex_alias_001():
    sql = """select tab_a.col_a1 c1, col_a2 c2, t0.* from ds_a.tab_a t0"""
    rs = psr.parse_sql(sql)
    assert (type(rs) == Dataset and
            len(rs.column_pools.keys()) == 1 and
            list(rs.column_pools.keys())[0] is None and
            len(rs.column_pools[None]) == 5 and
            type(rs.column_pools[None][0]) == Column), "Basic type check failed"

    out_map = dict()
    for c in rs.column_pools.get(None):
        assert c.known_as in ['c1', 'c2', 'col_a1', 'col_a2', 'col_a3']
        for u in c.dependencies.get_all():
            if u not in out_map:
                out_map[u] = 1
            else:
                out_map[u] += 1

    assert ('ns_a.ds_a.tab_a.col_a1' in out_map and out_map['ns_a.ds_a.tab_a.col_a1'] == 2)
    assert ('ns_a.ds_a.tab_a.col_a2' in out_map and out_map['ns_a.ds_a.tab_a.col_a2'] == 2)
    assert ('ns_a.ds_a.tab_a.col_a3' in out_map and out_map['ns_a.ds_a.tab_a.col_a3'] == 1)


def test_complex_alias_002():
    psr.use_dataset('ds_a')
    sql = """select * from tab_a t0 join tab_b on col_a1=col_b1"""
    rs = psr.parse_sql(sql)
    print('OK')


def test_basic_select_002():
    sql = """select *, a.*, e.f f0, foo(tee(*), z.y, k.f) from ds_a.tab_a a"""
    rs = psr.parse_sql(sql)
    assert True, "Use this one to test parsing results"
