from pyramid.view import view_config


class BaseController(object):

    def __init__(self, request):
        self.request = request
        self.db = self.request.find_service(name='db')


class StatementController(BaseController):

    @view_config(route_name='get_statement', renderer='json')
    def get_statement(self):
        return {'hello': 'world'}
