from sql_lineage2.sql_components.catalog import DbCatalog
from sql_lineage2.sql_components.column import TableColumn


def test_db_catalog_basic():
    dc = DbCatalog(levels=3)
    test_data = [
        {
            'namespace': 'db-01',
            'datasets': [
                {
                    'dataset': 'schema-01',
                    'tables': [
                        {'table': 'tab-1',
                         'columns': [
                             {
                                 'column': 'col1',
                                 'type': 'int',
                             }
                         ]
                         }
                    ]
                },
                {
                    'dataset': 'schema-02',
                    'tables': [
                        {'table': 'tab-01',
                         'columns': [
                             {
                                 'column': 'col1',
                                 'type': 'int',
                             },
                             {
                                 'column': 'col2',
                                 'type': 'string',
                             }
                         ]
                         },
                        {'table': 'tab-02',
                         'columns': [
                             {
                                 'column': 'col01',
                                 'type': 'blob',
                             },
                             {
                                 'column': 'col02',
                                 'type': 'string',
                             }
                         ]
                         }
                    ]
                }
            ]
        }
    ]
    dc.setup_store(test_data)
    tab_obj = dc.find_table('db-01.schema-02.tab-02')
    assert 'table' in tab_obj and 'columns' in tab_obj, 'Missing keys'
    assert len(tab_obj['columns']) == 2 and type(tab_obj['columns'][0]) == TableColumn, 'Content not matching'

    col_list = [
        TableColumn(name='new_col1', type='int'),
        TableColumn(name='new_col2', type='string'),
    ]
    dc.add_table(new_ns='db-02', new_ds='schema-02', new_tab='tab-03', new_cols=col_list)
    assert len(dc.root) == 2, 'Namespace counts do not match'
    tab_obj = dc.find_table('db-02.schema-02.tab-03')
    assert tab_obj and tab_obj['columns'] and len(tab_obj['columns']) == 2, 'Details from the new table do not match'
