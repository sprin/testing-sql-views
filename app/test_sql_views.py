import os

import sqlalchemy
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

def insert_account_with_projects(conn):
        conn.execute(
'''
WITH new_account AS (
    INSERT INTO account (account_name) VALUES ('antonioni')
    RETURNING account_id
)
, new_projects(project_name, time_start) AS (VALUES
    ('l''avventura', '1960-10-19 23:59:33Z')
    , ('la notte', '1961-10-19 23:59:33Z')
    , ('l''eclisse', '1962-10-19 23:59:33Z')
)
INSERT INTO project (account_id, project_name, time_start)
    SELECT
        new_account.account_id
        , new_projects.project_name
        , time_start::timestamptz
    FROM new_account, new_projects
'''
        )

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
