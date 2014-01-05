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
    info = {'natural_key': 'name'}
)

member = Table('member', metadata,
    Column('member_id', Integer, primary_key = True),
    Column('account_id', ForeignKey("account.account_id"), nullable=False),
    Column('name', String),
    info = {'natural_key': 'name'}
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
            'person1': 'person1_id',
            'person2': 'person2_id',
        }
    }
)


import psycopg2.extensions
typecasters = {
    'INTEGER': psycopg2.extensions.INTEGER,
    'VARCHAR': lambda x, y: x,
    'DATETIME': psycopg2.extensions.PYDATETIME,
}
