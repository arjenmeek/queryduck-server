from .models import Statement
from .utility import StatementReference


class StatementRepository(object):
    """Query and modify the set of stored Statements."""

    def __init__(self, request):
        """Make relevant services available."""
        self.request = request
        self.db = self.request.find_service(name='db')

    def resolve_reference(self, orig_value, context=None):
        """Resolve StatementReference instances into an actual Statement."""
        if isinstance(orig_value, StatementReference):
            value = orig_value.resolve(context, self)
        else:
            value = orig_value
        return value

    def get_by_uuid(self, uuid_):
        """Get a Statement by its UUID identifier."""
        statement = self.db.query(Statement).filter_by(uuid=uuid_).one()
        return statement

    def find(self):
        """Find and return multiple Statements."""
        statements = self.db.query(Statement).all()
        return statements

    def new(self, uuid_, subject_r, predicate_r, object_r):
        """Instantiate a new Statement based on the element values provided."""
        statement = Statement(uuid_)
        statement.subject = self.resolve_reference(subject_r, statement)
        statement.predicate = self.resolve_reference(predicate_r, statement)
        statement.object = self.resolve_reference(object_r, statement)
        return statement

    def save(self, statement):
        """Save a Statement into the underlying database transaction."""
        self.db.add(statement)

    def commit(self):
        """Commit the currently open database transaction."""
        self.db.commit()
