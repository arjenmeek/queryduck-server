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
    config.add_route('delete_statement', '/statements/{reference}', request_method='DELETE')

    config.add_route('create_volume', '/volumes/{reference}', request_method='PUT')
    config.add_route('delete_volume', '/volumes/{reference}', request_method='DELETE')
    config.add_route('get_volume', '/volumes/{reference}')
    config.add_route('list_volumes', '/volumes')
    config.add_route('create_blob', '/blobs/new', request_method='POST')
    config.add_route('get_blob', '/blobs/{reference}')
    config.add_route('list_blobs', '/blobs')
    config.add_route('list_volume_files', '/volumes/{volume_reference}/files', request_method='GET')
    config.add_route('mutate_volume_files', '/volumes/{volume_reference}/files', request_method='POST')
    config.add_route('get_volume_file', '/volumes/{volume_reference}/files/{file_path}', request_method='GET')

    config.scan('.controllers')

    app = config.make_wsgi_app()
    return app
