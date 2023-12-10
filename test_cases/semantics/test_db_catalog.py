from sql_lineage2.sql_components.catalog import DbCatalog


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
    assert len(tab_obj) == 2, 'Failed test'
