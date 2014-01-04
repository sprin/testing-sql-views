from sqlalchemy import create_engine
from app import app

standard_engine = create_engine(
    app.config['DATABASE_URL'],
    isolation_level = 'AUTOCOMMIT',
)
testing_engine = create_engine(
    app.config['DATABASE_URL_TESTING'],
    isolation_level = 'READ_COMMITTED',
)

if app.config.get('TESTING'):
    engine = testing_engine
else:
    engine = standard_engine

