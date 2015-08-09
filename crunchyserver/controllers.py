from pyramid.view import view_config

from .models import Statement
from .utility import deserialize_value


class BaseController(object):

    def __init__(self, request):
        self.request = request
        self.db = self.request.find_service(name='db')


class StatementController(BaseController):

    @view_config(route_name='get_statement', renderer='json')
    def get_statement(self):
        uuid_ = deserialize_value(self.request.matchdict['reference'])

        statement = self.db.query(Statement).filter_by(uuid=uuid_).one()

        return statement

    @view_config(route_name='put_statement', renderer='json')
    def put_statement(self):
        print('put_statement:', self.request.json_body)
        uuid_ = deserialize_value(self.request.matchdict['reference'])

        statement = Statement(uuid_)
        statement.subject = deserialize_value(self.request.json_body[1], context=statement, db=self.db)
        statement.predicate = deserialize_value(self.request.json_body[2], context=statement, db=self.db)
        statement.object = deserialize_value(self.request.json_body[3], context=statement, db=self.db)

        self.db.add(statement)
        self.db.commit()
        return statement
