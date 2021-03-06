import collections
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

test_fixture1 = {
  'account': [
    {
      'name': 'antonioni',
      'project': [
        {'name': 'l''avventura', 'time_start': '1960-10-19 23:59:33+00'},
        {'name': 'la notte', 'time_start': '1961-10-19 23:59:33+00'},
        {'name': 'l''eclisse', 'time_start': '1962-10-19 23:59:33+00'},
      ],
      'member': [
        {'name': 'mastroianni'},
        {'name': 'vitti',
          'cat': [
             {'name': 'statua'}
           ],
        },
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
  ],
  'locale': [
    {'name': 'la spiaggia'},
    {'name': 'le rovine'},
    {'name': 'giardino'},
  ],

  'person': [
    {'name': 'antonioni'},
    {'name': 'vitti'},
  ],
  'relation': [
    {
      'person1_name': 'vitti',
      'person2_name': 'antonioni',
      'nature_relation': 'love',
    },
    {
      'person1_name': 'vitti',
      'person2_name': 'antonioni',
      'nature_relation': 'actor-director',
    },
  ]
}



def partition_own_attrs_and_children(dict_):
    own_attrs = {}
    children = {}
    for k, v in dict_.items():
        if isinstance(v, list):
            children[k] = v
        else:
            own_attrs[k] = v
    return own_attrs, children


def cast_from_pg_type(column, value, cur):
    return tables.typecasters[str(column.type)](value, cur)

def get_pg_type_values_tup(obj, table_obj, cur):
    pg_type_values = []
    for k, v in obj.items():
        column = table_obj.columns.get(k)
        if column is None:
            #Fall back to attempting lookup on natural fk.
            natural_join = table_obj.info['natural_joins'][k]
            parent_table = tables.metadata.tables[natural_join['parent_table']]
            column = parent_table.columns[natural_join['parent_natural_key']]
        pg_type_values.append(cast_from_pg_type(column, v, cur))
    return tuple(pg_type_values)

def get_insert_values(obj, table_obj, cur, natural_fk=None):
    """
    Get the INSERT context for a single object.
    """
    val_tup = get_pg_type_values_tup(obj, table_obj, cur)
    if natural_fk:
        val_tup = (natural_fk,) + val_tup
    return val_tup

def mogrify(args, cur):
    markers = '\n\t,'.join(['%s'] * len(args))
    return cur.mogrify(markers, args)

INSERT_WITH_CHILDREN_TMPL = mako.template.Template(
    open('templates/insert_with_children.sql').read(),
    strict_undefined=True,
)

def get_insert_order(deps, insert_order=None):
    """
    We're in for some trouble if there are circular deps.
    """
    insert_order = []
    dep_list = [(t, p) for t, p in deps.items()]
    for tablename, parents in dep_list:
        # If all parents are already ordered, add this to the ordered list.
        if not [p for p in parents if p not in insert_order]:
            insert_order.append(tablename)
        else:
            # Otherwise, add tuple to end of list, so we check it again.
            dep_list.append((tablename, parents))
    return insert_order

def get_ordered_record_list(record_cache, insert_order):
    ordered = []
    for tablename in insert_order:
        records = record_cache[tablename]
        # Put tablename inside of records object, since we are now
        # using a list, instead of a dict.
        records['tablename'] = tablename
        ordered.append(records)
    return ordered

def annotate_with_join_info(records_list):
    for records in records_list:
        tablename = records['tablename']
        natural_joins = tables.metadata.tables[tablename].info.get('natural_joins')
        if not natural_joins:
            continue

        records['natural_joins'] = []
        for col in records['value_cols']:
            natural_join = natural_joins.get(col)
            if natural_join:
                records['natural_joins'].append(natural_join)

def comma_join(seq, prefix=''):
    if prefix:
        return ', '.join([prefix+'.'+x for x in seq])
    else:
        return ', '.join(seq)



def construct_insert_str_with_children(fixture):
    # We need a cursor in order to type-cast and mogrify.
    cur = engines.testing_engine.raw_connection().cursor()
    record_cache = get_record_cache(fixture, cur)

    deps = dict([(k, v['parent_tablenames']) for k, v in record_cache.items()])
    insert_order = get_insert_order(deps)
    ordered_records = get_ordered_record_list(record_cache, insert_order)
    annotate_with_join_info(ordered_records)
    ctx = {
        'records': ordered_records,
        'comma_join': comma_join,
        'mogrify': lambda x: mogrify(x, cur)
    }
    return INSERT_WITH_CHILDREN_TMPL.render(**ctx)

def get_record_cache(fixture, cur):

    record_cache = collections.defaultdict(lambda: {
        'value_cols': None,
        'values': [],
        'parent_tablenames': set([]),
    })

    tables_meta = tables.metadata.tables

    def get_records_cache_inner(fixture, parent_tname=None,
                                parent_natural_fk_val=None):

        if parent_tname:
            parent_natural_key = tables_meta[parent_tname].info['natural_key']

        for tname, objs in fixture.items():
            table = tables_meta[tname]

            obj, children = partition_own_attrs_and_children(objs[0])
            cols = obj.keys()

            value_cols = []
            if parent_tname:
                natural_fk = table.info['fk_reversed'][parent_natural_key]
                value_cols = (natural_fk,) + tuple(cols)
                insert_cols = tuple(cols)
            else:
                # See if key refers to a parent by natural key
                natural_fks = []
                for col in cols:
                    natural_joins = table.info.get('natural_joins')
                    if natural_joins:
                        natural_fk = natural_joins.get(col)
                        if natural_fk:
                            natural_fks.append(col)
                # Filter out natural FKs from insert columns.
                insert_cols = tuple([x for x in cols if x not in natural_fks])
                value_cols = tuple(cols)

            for obj in objs:

                # We split the obj dict into the objects own attributes,
                # and it's lists of child objects.
                obj, children = partition_own_attrs_and_children(obj)

                ## Construct context for VALUES CTE
                if parent_tname:
                    record_cache[tname]['parent_tablenames'].add(parent_tname)
                record_cache[tname]['value_cols'] = value_cols
                record_cache[tname]['insert_cols'] = insert_cols

                record_cache[tname]['values'].append(
                    get_insert_values(obj, table, cur,
                                      parent_natural_fk_val))

                # Prepare for recursive call
                if children:
                    natural_fk_val = obj[table.info['natural_key']]
                    get_records_cache_inner(children, tname, natural_fk_val)

    get_records_cache_inner(fixture)

    return record_cache

def insert_account_with_projects(conn):
    insert_stmt = construct_insert_str_with_children(test_fixture1)
    conn.execute(insert_stmt)

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

