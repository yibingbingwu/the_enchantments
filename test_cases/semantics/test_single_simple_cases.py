from sql_lineage2.main import SqlLineage

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
psr.setup_catalog(CTG)


def test_basic_select_001():
    sql = """select * from ds_a.tab_a"""
    psr.parse_sql(sql)
    assert True, "So long there is no exception"
