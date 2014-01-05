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
)

project = Table('project', metadata,
    Column('project_id', Integer, primary_key = True),
    Column('account_id', ForeignKey("account.account_id"), nullable=False),
    Column('name', String),
    Column('time_start', DateTime(timezone=True)),
)

member = Table('member', metadata,
    Column('member_id', Integer, primary_key = True),
    Column('account_id', ForeignKey("account.account_id"), nullable=False),
    Column('name', String),
)

import psycopg2.extensions
typecasters = {
    'INTEGER': psycopg2.extensions.INTEGER,
    'VARCHAR': lambda x, y: x,
    'DATETIME': psycopg2.extensions.PYDATETIME,
}
