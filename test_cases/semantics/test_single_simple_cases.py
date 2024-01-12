from sql_lineage2.main import SqlLineage
from sql_lineage2.db_components.column import Column
from sql_lineage2.db_components.dataset import Dataset

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
            rs.column_pools[None][0].known_as in ['col_a1', 'col_a2', 'col_a3'])


def test_basic_lineage_001():
    sql = """create table ds_a.tab_new_01 as select tab_a.* from ds_a.tab_a"""
    rs = psr.parse_sql(sql)
    # print('OK')
    assert (len(psr.lineage_graph) == 3 and
            'SELECT' in psr.lineage_graph['ns_a.ds_a.tab_new_01.col_a1'] and
            list(psr.lineage_graph['ns_a.ds_a.tab_new_01.col_a1']['SELECT']) == ['ns_a.ds_a.tab_a.col_a1'] and
            len(psr.lineage_graph['ns_a.ds_a.tab_new_01.col_a1']['WHERE']) == 0 and
            len(psr.lineage_graph['ns_a.ds_a.tab_new_01.col_a1']['JOIN']) == 0), 'Basic lineage results error'
