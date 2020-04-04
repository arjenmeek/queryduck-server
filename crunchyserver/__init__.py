from pyramid.config import Configurator
from pyramid.authentication import BasicAuthAuthenticationPolicy
from pyramid.authorization import ACLAuthorizationPolicy
from pyramid.security import ALL_PERMISSIONS, Allow, Authenticated, forget
from pyramid.view import forbidden_view_config, view_config
from pyramid.httpexceptions import HTTPUnauthorized

from .models import init_db

def forbidden_view(request):
    """Trigger client to send basic HTTP auth info"""
    if request.authenticated_userid is None:
        response = HTTPUnauthorized()
        response.headers.update(forget(request))
        return response
    return HTTPForbidden()

def check_credentials(username, password, request):
    """Always allows everything"""
    return []

class Root:
    """Very basic root context"""
    __acl__ = (
        (Allow, Authenticated, ALL_PERMISSIONS),
    )

def main(settings):
    """Create and return a WSGI application."""

    config = Configurator()
    config.include('pyramid_services')

    engine = init_db(settings)

    def dbsession_factory(context, request):
        """Initialize an SQLAlchemy database connection."""
        conn = engine.connect()
        return conn

    config.register_service_factory(dbsession_factory, name='db')

    # Configure authentication / authorization
    authn_policy = BasicAuthAuthenticationPolicy(check_credentials)
    config.set_authentication_policy(authn_policy)
    config.set_authorization_policy(ACLAuthorizationPolicy())
    config.set_root_factory(lambda request: Root())
    config.add_forbidden_view(forbidden_view)

    config.add_static_view(name='static', path='../../webclient/static')

    config.add_route('find_statements', '/statements', request_method='GET')
    config.add_route('create_statements', '/statements', request_method='POST')
    config.add_route('get_schema', '/schemas/{reference}', request_method='GET')
    config.add_route('establish_schema', '/schemas/{reference}', request_method='POST')
    config.add_route('schema_transaction', '/schemas/{reference}/statements', request_method='POST')

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
    config.scan('.storage.controllers')

    app = config.make_wsgi_app()
    return app
