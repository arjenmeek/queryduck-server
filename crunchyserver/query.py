from crunchylib.exceptions import GeneralError
from crunchylib.utility import deserialize_value, get_value_type

from .models import Statement


class StatementQuery(object):

    def __init__(self, db, statements):
        self.db = db
        self.statements = statements
        self.aliases = {'main': Statement}
        self.q = self.db.query(self.aliases['main'])

    def _parse_reference(self, reference, object_type=None):
        if ':' in reference:
            value = deserialize_value(reference)
            value = self.statements.resolve_reference(value)
        elif '.' in reference:
            alias_name, attribute_name = reference.split('.')
            if attribute_name == 'object' and object_type is not None:
                attribute_name = 'object_{}'.format(object_type)
            if object_type == 'statement':
                attribute_name += '_id'
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
            print('filter: {} == {}'.format(lhs, rhs))
            if isinstance(rhs, Statement):
                self.q = self.q.filter(lhs==rhs.id)
            else:
                self.q = self.q.filter(lhs==rhs)
        else:
            raise GeneralError("Unknown filter operation: {}".format(op_str))

    def all(self):
        statements = self.q.all()
        return statements
