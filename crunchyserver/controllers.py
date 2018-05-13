from pyramid.view import view_config
from uuid import UUID

from crunchylib.exceptions import GeneralError
from crunchylib.utility import deserialize_value, get_value_type

from .models import Statement


class StatementQuery(object):

    def __init__(self, db):
        self.db = db
        self.aliases = {'main': Statement}
        self.q = self.db.query(self.aliases['main'])

    def _parse_reference(self, reference, object_type=None):
        if ':' in reference:
            value = deserialize_value(reference)
        elif '.' in reference:
            alias_name, attribute_name = reference.split('.')
            if attribute_name == 'object' and object_type is not None:
                attribute_name = 'object_{}'.format(object_type)
            if not alias_name in self.aliases:
                raise GeneralError("Unknown alias name: {}".format(alias_name))
            value = getattr(self.aliases[alias_name], attribute_name)
        else:
            raise GeneralError("Invalid reference: {}".format(reference))
        return value

    def apply_filter(self, lhs_str, op_str, rhs_str=None):
        rhs_type = None
        if rhs_str is None:
            rhs = None
        else:
            rhs = self._parse_reference(rhs_str)
            if ':' in rhs_str:
                rhs_type = get_value_type(rhs)
        lhs = self._parse_reference(lhs_str, object_type=rhs_type)

        if op_str == 'eq':
            self.q = self.q.filter(lhs==rhs)
            print('filter: {} == {}'.format(lhs, rhs))
        else:
            raise GeneralError("Unknown filter operation: {}".format(op_str))

    def all(self):
        statements = self.q.all()
        return statements


class BaseController(object):
    """Provide a basic Controller class to extend."""

    def __init__(self, request):
        """Make relevant services available."""
        self.request = request
        self.db = self.request.find_service(name='db')


class StatementController(BaseController):
    """Handle requests primarily concerned with Statements."""

    def __init__(self, request):
        """Make relevant services available."""
        self.request = request
        self.db = self.request.find_service(name='db')
        self.statements = self.request.find_service(name='statement_repository')

    def _parse_filter_string(self, filter_string):
        parts = filter_string.split(',')
        if len(parts) == 2:
            lhs, op = parts
            rhs = None
        elif len(parts) == 3:
            lhs, op, rhs = parts
        else:
            raise GeneralError("Invalid filter string: {}".format(filter_string))

        return lhs, op, rhs


    def parse_uuid_reference(self, reference):
        """Deserialize the reference if it's a UUID, raise an exception otherwise."""
        uuid_ = deserialize_value(reference)
        if type(uuid_) != UUID:
            raise GeneralError("Invalid reference type")

        return uuid_

    @view_config(route_name='find_statements', renderer='json')
    def find_statements(self):
        """Return multiple Statements."""
        qc = StatementQuery(self.db)

        filter_strings = self.request.GET.getall('filter')
        for fs in filter_strings:
            lhs, op, rhs = self._parse_filter_string(fs)
            qc.apply_filter(lhs, op, rhs)

        statements = qc.all()
        return statements

    @view_config(route_name='get_statement', renderer='json')
    def get_statement(self):
        """Get one Statement by its UUID."""
        uuid_ = self.parse_uuid_reference(self.request.matchdict['reference'])
        statement = self.statements.get_by_uuid(uuid_)
        return statement

    @view_config(route_name='put_statement', renderer='json')
    def put_statement(self):
        """Insert one Statement by its UUID."""
        print('put_statement:', self.request.json_body)
        raw_st = self.request.json_body
        uuid_ = self.parse_uuid_reference(self.request.matchdict['reference'])

        subject_r, predicate_r, object_r = [deserialize_value(v) for v in self.request.json_body[1:]]

        statement = self.statements.new(uuid_, subject_r, predicate_r, object_r)

        self.db.add(statement)
        self.db.commit()
        return statement

    @view_config(route_name='delete_statement', renderer='json')
    def delete_statement(self):
        """Delete a Statement by its UUID."""
        uuid_ = self.parse_uuid_reference(self.request.matchdict['reference'])
        statement = self.statements.get_by_uuid(uuid_)
        self.db.delete(statement)
        self.db.commit()
        return {}
