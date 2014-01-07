import psycopg2.extensions
from sqlalchemy import (
    MetaData,
    Table,
    Column,
    Integer,
    String,
    DateTime,
    ForeignKey,
)

import engines

typecasters = {
    'INTEGER': psycopg2.extensions.INTEGER,
    'VARCHAR': lambda x, y: x,
    'DATETIME': psycopg2.extensions.PYDATETIME,
}

metadata = MetaData(bind=engines.engine)

account = Table('account', metadata,
    Column('account_id', Integer, primary_key = True),
    Column('name', String),
    info = {'natural_key': 'name'}
)

project = Table('project', metadata,
    Column('project_id', Integer, primary_key = True),
    Column('account_id', ForeignKey("account.account_id"), nullable=False),
    Column('name', String),
    Column('time_start', DateTime(timezone=True)),
    info = {
        'natural_key': 'name',
        'natural_fks': {'account_name': 'account_id'}
    }
)

member = Table('member', metadata,
    Column('member_id', Integer, primary_key = True),
    Column('account_id', ForeignKey("account.account_id"), nullable=False),
    Column('name', String),
    info = {
        'natural_key': 'name',
        'natural_fks': {'account_name': 'account_id'}
    }
)

cat = Table('cat', metadata,
    Column('cat_id', Integer, primary_key = True),
    Column('member_id', ForeignKey("member.member_id"), nullable=False),
    Column('name', String),
    info = {
        'natural_key': 'name',
        'natural_fks': {'member_name': 'member_id'}
    }
)

locale = Table('locale', metadata,
    Column('locale_id', Integer, primary_key = True),
    Column('name', String),
    info = {'natural_key': 'name'}
)

person = Table('person', metadata,
    Column('person_id', Integer, primary_key = True),
    Column('name', String),
    info = {'natural_key': 'name'}
)

relation = Table('relation', metadata,
    Column('person1_id', ForeignKey("person.person_id"), nullable=False),
    Column('person2_id', ForeignKey("person.person_id"), nullable=False),
    Column('nature_relation', String),
    info = {
        'natural_fks': {
            'person1_name': 'person1_id',
            'person2_name': 'person2_id',
        }
    }
)

def extend_metadata():
    """
    Follow foreign key relations to get the values needed to
    construct a "natural join", or a join on a natural FK and a
    natural key.
    Caches these values in table.info['natural_join'].
    """
    for table in metadata.tables.values():
        natural_fks = table.info.get('natural_fks')
        if natural_fks:
            table.info['natural_joins'] = {}
            table.info['fk_reversed'] = {}
            for natural_fk, surrogate_fk in natural_fks.items():
                fk = [x for x in table.columns[surrogate_fk].foreign_keys][0]
                parent_table = fk.column.table
                parent_pk = [x.name for x in parent_table.primary_key][0]
                parent_natural_key = parent_table.info['natural_key']
                table.info['natural_joins'][natural_fk] = {
                    'natural_fk': natural_fk,
                    'surrogate_fk': surrogate_fk,
                    'parent_table': parent_table.name,
                    'parent_natural_key': parent_natural_key,
                    'parent_pk': parent_pk,
                }
                # Build a map of reversed natural keys, for the
                # case where there is only one possible
                # relation to the parent
                table.info['fk_reversed'][parent_natural_key] = natural_fk
extend_metadata()

# An example natural join
'''
WITH person_cte AS (
	INSERT INTO person(name) VALUES
		('antonioni')
		, ('vitti')
	RETURNING person_id, name
)
, values1_1(person1_name, person2_name, nature_relation) AS (VALUES
    ('vitti', 'antonioni', 'love')
    , ('vitti', 'antonioni', 'actor-director')
)
INSERT INTO relation(person1_id, person2_id, nature_relation)
SELECT p1.person_id, p2.person_id, nature_relation
FROM values1_1 v
JOIN person_cte p1
ON v.person1_name = p1.name
JOIN person_cte p2
ON v.person2_name = p2.name
'''

