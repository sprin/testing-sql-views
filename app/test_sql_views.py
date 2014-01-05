import os

import sqlalchemy
import sqlalchemy.sql.expression
import app
app.app.config['TESTING'] = True

import engines
import tables

def setup_module():
    """Create the database and all tables."""
    try:
        engines.standard_engine.execute('CREATE DATABASE sql_views_testing');
    # If an error was made that caused connections not to be closed the last
    # time tests were run, the DB would not be dropped by the teardown.
    # In that case, we drop it before re-creating.
    except sqlalchemy.exc.ProgrammingError:
        engines.standard_engine.execute('DROP DATABASE sql_views_testing');
        engines.standard_engine.execute('CREATE DATABASE sql_views_testing');

    # Create tables
    tables.metadata.create_all(engines.testing_engine)

    # Load an SQL module - would normally be done by a more sophisticated loader.
    sql_module = os.path.join(
        os.path.dirname(__file__),
        '../sql/latest_project.sql')
    with open(sql_module) as f:
        engines.testing_engine.execute(f.read())

def teardown_module():
    """Shutdown the pool and drop the database."""
    engines.testing_engine.dispose()
    engines.standard_engine.execute('DROP DATABASE sql_views_testing');

def get_test_fixture():
    return {
      'account': [
        {
          'account_name': 'antonioni',
          'project': [
            {'project_name': 'l''avventura', 'time_start': '1960-10-19 23:59:33+00'},
            {'project_name': 'la notte', 'time_start': '1961-10-19 23:59:33+00'},
            {'project_name': 'l''eclisse', 'time_start': '1962-10-19 23:59:33+00'},
          ]
        }
      ]
    }

def partition_own_properties_and_children(dict_):
    own_properties = {}
    children = {}
    for k, v in dict_.items():
        if isinstance(v, list):
            children[k] = v
        else:
            own_properties[k] = v
    return own_properties, children

def construct_inserts_for_fixture(fixture):
    for tablename, objs in fixture.items():
        return construct_insert_with_children(
            tablename,
            objs[0],
        )

def cast_from_pg_type(column, value, cur):
    return tables.typecasters[str(column.type)](value, cur)


def construct_insert_with_children(tablename, obj):
    # Annoyingly, mogrify is only available from a cursor instance.
    cur = engines.testing_engine.raw_connection().cursor()

    table = tables.metadata.tables[tablename]
    obj, children = partition_own_properties_and_children(obj)

    for k, v in obj.items():
        column = table.columns[k]
        obj[k] = cast_from_pg_type(column, v, cur)

    parent_pkey = [x.name for x in table.primary_key]
    fmt = (
"""
INSERT INTO {tablename}({cols}) VALUES %s
RETURNING {parent_pkey}
"""
    .format(
        tablename = tablename,
        cols = ','.join(obj.keys()),
        parent_pkey = ','.join(parent_pkey),
    ))
    parent_insert = cur.mogrify(fmt, [tuple(obj.values())])

    child_inserts = []
    for tablename, objs in children.items():
        table = tables.metadata.tables[tablename]

        ## Construct the VALUES CTE
        cte_alias = "new_{0}s".format(tablename)
        cte_cols = objs[0].keys()
        fmt = ", {cte_alias}({cols}) AS (VALUES {markers})".format(
            cte_alias = cte_alias,
            cols = ','.join(cte_cols),
            markers = '\n,'.join(['%s'] * len(objs))
        )

        # Cast types
        for obj in objs:
            for k, v in obj.items():
                column = table.columns[k]
                obj[k] = cast_from_pg_type(column, v, cur)

        values_cte = cur.mogrify(fmt, [tuple(x.values()) for x in objs])

        # Construct the INSERT INTO from sub-select
        col_list = tuple(parent_pkey + cte_cols)
        child_insert_str = (
"""
{values_cte}
INSERT INTO {tablename}({cols})
SELECT {cols}
FROM parent, {cte_alias}
"""
            .format(
                values_cte = values_cte,
                tablename = tablename,
                cols = ','.join(col_list),
                cte_alias = cte_alias,
        ))

        child_inserts.append(child_insert_str)
    insert_with_children = (
        'WITH parent AS ({parent_insert}){child_inserts}'
        .format(
            parent_insert = parent_insert,
            child_inserts = '\n'.join(child_inserts)
        ))
    return insert_with_children


def insert_account_with_projects(conn):
        conn.execute(
            construct_inserts_for_fixture(get_test_fixture()))

def test_insert():
    with engines.testing_engine.connect() as conn:
        trans = conn.begin()
        insert_account_with_projects(conn)
        result = conn.execute(
'''
SELECT account_name, latest_project_time::text FROM account_latest_project_time
'''
    )
        trans.rollback()
    # We expect the DB to have timezone = 'UTC' in postgresql.conf,
    # therefore the conversion to string will have this +00 offset.
    assert list(result) == [
        ("antonioni", '1962-10-19 23:59:33+00')]

#Create lots of dupe tests for nose to run
for x in xrange(998):
    vars()['test{0}'.format(x)] = test_insert

def test_transaction_rollback():
    result = engines.testing_engine.execute('SELECT count(*) FROM account');
    assert result.fetchall()[0][0] == 0

