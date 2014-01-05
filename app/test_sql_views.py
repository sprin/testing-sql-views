import os

import mako.template
import sqlalchemy
import sqlalchemy.sql.expression
import app
app.app.config['TESTING'] = True

import engines
import tables

def setup_module():
    """Create the database and all tables."""
    try:
        # Check if DB is already created (saves about 500ms if so),
        # and drop and recreate the public schema if so (start from scratch).
        engines.testing_engine.execute('DROP SCHEMA IF EXISTS public CASCADE')
        engines.testing_engine.execute('CREATE SCHEMA public')
    except sqlalchemy.exc.OperationalError:
        engines.standard_engine.execute('CREATE DATABASE sql_views_testing');

    # Create tables
    tables.metadata.create_all(engines.testing_engine)

    # Load an SQL module - would normally be done by a more sophisticated loader.
    sql_module = os.path.join(
        os.path.dirname(__file__),
        '../sql/latest_project.sql')
    with open(sql_module) as f:
        engines.testing_engine.execute(f.read())

def get_test_fixture1():
    return {
      'account': [
        {
          'name': 'antonioni',
          'project': [
            {'name': 'l''avventura', 'time_start': '1960-10-19 23:59:33+00'},
            {'name': 'la notte', 'time_start': '1961-10-19 23:59:33+00'},
            {'name': 'l''eclisse', 'time_start': '1962-10-19 23:59:33+00'},
          ],
          'member': [
              {'name': 'vitti'},
              {'name': 'mastroianni'},
          ],
        },
        {
          'name': 'fellini',
          'project': [
            {'name': 'la dulce vita', 'time_start': '1960-10-19 23:59:33+00'},
            {'name': 'otto e mezzo', 'time_start': '1963-10-19 23:59:33+00'},
          ],
          'member': [
              {'name': 'masina'},
          ],
        }
      ]
    }

def get_test_fixture2():
    return {
      'person': [
        {'name': 'antonioni'},
        {'name': 'vitti'},
      ],
      'relation': [
        {
          'person1': 'vitti',
          'person2': 'antonioni',
          'nature_relation': 'love',
        },
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
            objs,
        )

def cast_from_pg_type(column, value, cur):
    return tables.typecasters[str(column.type)](value, cur)

INSERT_WITH_CHILDREN_TMPL = mako.template.Template(
    open('templates/insert_with_children.sql').read(),
    strict_undefined=True,
)

def construct_insert_with_children(tablename, objs):
    # Annoyingly, mogrify is only available from a cursor instance.
    cur = engines.testing_engine.raw_connection().cursor()

    parent_table = tables.metadata.tables[tablename]

    parents = []
    for obj in objs:
        obj, children = partition_own_properties_and_children(obj)
        parent_cols = obj.keys()

        for k, v in obj.items():
            column = parent_table.columns[k]
            obj[k] = cast_from_pg_type(column, v, cur)

        parent_pk = [x.name for x in parent_table.primary_key]
        parent_values = [tuple(obj.values())]

        all_children = []
        for tablename, objs in children.items():
            child_table = tablename
            table = tables.metadata.tables[tablename]

            ## Construct the VALUES CTE
            cte_cols = objs[0].keys()

            # Cast types
            for obj in objs:
                for k, v in obj.items():
                    column = table.columns[k]
                    obj[k] = cast_from_pg_type(column, v, cur)
            cte_values = [tuple(x.values()) for x in objs]

            # Construct the INSERT INTO from sub-select
            child_cols = tuple(parent_pk + cte_cols)
            child_ctx = {
                'child_table': child_table,
                'child_cols': child_cols,
                'cte_cols': cte_cols,
                'cte_values': cte_values,
            }
            all_children.append(child_ctx)

        def mogrify(args):
            markers = '\n,'.join(['%s'] * len(args))
            return cur.mogrify(markers, args)

        parents.append({
            'parent_table': parent_table,
            'parent_cols': parent_cols,
            'parent_values': parent_values,
            'parent_pk': parent_pk,
            'all_children': all_children,
        })

    ctx = {
        'parents': parents,
        'comma_join': lambda x: ','.join(x),
        'mogrify': mogrify,
    }
    insert_with_children = INSERT_WITH_CHILDREN_TMPL.render(**ctx)

    return insert_with_children

def test_m2m_insert():
    fix = get_test_fixture2()
    intersections = fix['relation']
    table = tables.metadata.tables['relation']
    for intersection in intersections:
        for k, v in intersection.items():
            target = table.info['natural_fks'].get(k)
            if target:
                fk = [x for x in table.c.person1_id.foreign_keys][0]
                target_column = fk.column.name
                parent_table = fk.column.table
                parent_real_key = parent_table.info['natural_key']
                print(
                    "{0}={1} is natural fk to {2} on {3}, "
                    "{4} is surrogate fk to {5} on {6}"
                    .format(
                        k,
                        v,
                        parent_real_key,
                        parent_table,
                        target,
                        target_column,
                        parent_table,
                    ))
            else:
                target = table.columns[k].name
                print("{0}={1}".format(target, v))

def insert_account_with_projects(conn):
        conn.execute(
            construct_inserts_for_fixture(get_test_fixture1()))

def test_insert():
    with engines.testing_engine.connect() as conn:
        trans = conn.begin()
        insert_account_with_projects(conn)
        result = conn.execute(
'''
SELECT name, latest_project_time::text
FROM account_latest_project_time
'''
    )
        trans.rollback()
    # We expect the DB to have timezone = 'UTC' in postgresql.conf,
    # therefore the conversion to string will have this +00 offset.
    assert list(result) == [
        ("fellini", '1963-10-19 23:59:33+00'),
        ("antonioni", '1962-10-19 23:59:33+00'),
    ]

#Create lots of dupe tests for nose to run
for x in xrange(998):
    vars()['test{0}'.format(x)] = test_insert

def test_transaction_rollback():
    result = engines.testing_engine.execute('SELECT count(*) FROM account');
    assert result.fetchall()[0][0] == 0

