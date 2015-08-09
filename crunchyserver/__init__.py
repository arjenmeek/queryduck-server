from pyramid.config import Configurator
from sqlalchemy import engine_from_config

from .models import Base, init_model

def main(settings):
    engine = engine_from_config(settings)
    Base.metadata.create_all(engine)
    config = Configurator()
    config.include('pyramid_services')

    dbmaker = init_model(settings)

    def dbsession_factory(context, request):
        dbsession = dbmaker()
        return dbsession

    config.register_service_factory(dbsession_factory, name='db')

    config.add_route('get_statement', '/statements/{reference}', request_method='GET')

    config.scan('.controllers')

    app = config.make_wsgi_app()
    return app
