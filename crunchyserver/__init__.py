from pyramid.config import Configurator
from sqlalchemy import engine_from_config

from .models import Base, init_model
from .repositories import StatementRepository

def main(settings):
    """Create and return a WSGI application."""
    engine = engine_from_config(settings)
    Base.metadata.create_all(engine)
    config = Configurator()
    config.include('pyramid_services')

    dbmaker = init_model(settings)

    def dbsession_factory(context, request):
        """Initialize an SQLAlchemy database session."""
        dbsession = dbmaker()
        return dbsession

    config.register_service_factory(dbsession_factory, name='db')

    def statement_repository_factory(context, request):
        """Initialize a StatementRepository."""
        statement_repository = StatementRepository(request)
        return statement_repository

    config.register_service_factory(statement_repository_factory, name='statement_repository')

    config.add_route('find_statements', '/statements', request_method='GET')
    config.add_route('get_statement', '/statements/{reference}', request_method='GET')
    config.add_route('put_statement', '/statements/{reference}', request_method='PUT')

    config.scan('.controllers')

    app = config.make_wsgi_app()
    return app
