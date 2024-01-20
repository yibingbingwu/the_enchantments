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
                    {
                        'table': 'tab_c',
                        'columns': [
                            {
                                'column': 'col_a1',
                                'type': 'int',
                            },
                            {
                                'column': 'col_c2',
                                'type': 'str',
                            },
                            {
                                'column': 'col_c3',
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


def test_join_simple_001():
    # TODO
    psr.use_dataset('ds_a')
    sql = """select * from tab_a t0 join tab_b on col_a1=col_b1"""
    rs = psr.parse_sql(sql)
    print('OK')


def test_join_cross_002():
    # TODO
    psr.use_dataset('ds_a')
    sql = """select * from tab_a t0 join tab_b, tab_b"""
    rs = psr.parse_sql(sql)
    print('OK')


def test_join_natural_003():
    # TODO
    psr.use_dataset('ds_a')
    sql = """select * from tab_a t0 full outer join tab_c using (col_a3)"""
    rs = psr.parse_sql(sql)
    print('OK')


def test_join_nested_004():
    # TODO
    psr.use_dataset('ds_a')
    sql = """select * from tab_a t0 right join (select col_b1 c0 from tab_b) on t0.col_a1=c0"""
    rs = psr.parse_sql(sql)
    print('OK')


def test_join_mixed_003():
    # TODO
    psr.use_dataset('ds_a')
    sql = """
select * from tab_a t0 
    join tab_b on col_a1=col_b1
    left join tab_c using (col_a2)
"""
    rs = psr.parse_sql(sql)
    print('OK')
