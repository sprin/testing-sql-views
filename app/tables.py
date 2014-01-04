from sqlalchemy import (
    MetaData,
    Table,
    Column,
    Integer,
    String,
    DateTime,
    ForeignKey,
)

metadata = MetaData()

account = Table('account', metadata,
    Column('account_id', Integer, primary_key = True),
    Column('account_name', String),
)

project = Table('project', metadata,
    Column('project_id', Integer, primary_key = True),
    Column('account_id', ForeignKey("account.account_id"), nullable=False),
    Column('project_name', String),
    Column('time_start', DateTime(timezone=True)),
)

