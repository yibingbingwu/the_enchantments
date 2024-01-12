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


def test_function_001():
    pass


def test_constants_001():
    pass
