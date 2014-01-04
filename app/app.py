class Application(object):
    """Mock application object with just a config dictionary."""
    def __init__(self):
        self.config = {}

app = Application()

app.config['DATABASE_URL'] = 'postgresql://alchemy@localhost:5432/sql_views'
app.config['DATABASE_URL_TESTING'] = (
    'postgresql://alchemy@localhost:5432/sql_views_testing')

