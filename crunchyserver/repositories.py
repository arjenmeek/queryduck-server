from .models import Statement
from .utility import StatementReference


class StatementRepository(object):

    def __init__(self, request):
        self.request = request
        self.db = self.request.find_service(name='db')

    def resolve_reference(self, orig_value, context=None):
        if isinstance(orig_value, StatementReference):
            value = orig_value.resolve(context, self)
        else:
            value = orig_value
        return value

    def get_by_uuid(self, uuid_):
        statement = self.db.query(Statement).filter_by(uuid=uuid_).one()
        return statement

    def find(self):
        statements = self.db.query(Statement).all()
        return statements

    def new(self, uuid_, subject_r, predicate_r, object_r):
        statement = Statement(uuid_)
        statement.subject = self.resolve_reference(subject_r, statement)
        statement.predicate = self.resolve_reference(predicate_r, statement)
        statement.object = self.resolve_reference(object_r, statement)
        return statement

    def save(self, statement):
        self.db.add(statement)

    def commit(self):
        self.db.commit()
