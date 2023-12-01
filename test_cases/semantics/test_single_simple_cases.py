from sql_lineage2.main import SqlLineage


def test_basic_select_001():
    sql = """select * from a.b"""
    psr = SqlLineage(sql)
    psr.setup_catalog([
        {'schema': 'a',
         'tables': [
             {'name': 'b'},
             {'columns': [
                 ('col1', 'int'),
                 ('col2', 'str'),
                 ('col3', 'int'),
             ]}
         ]}
    ])
    psr.parse_sql()
    assert True, "So long there is no exception"
