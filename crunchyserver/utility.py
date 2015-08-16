import datetime

from uuid import UUID

from . import models
from .exceptions import GeneralError


class StatementReference(object):

    def __init__(self, uuid_):
        self.uuid = uuid_
        self.self_reference = False

    def resolve(self, context, statement_repository):
        if self.self_reference or (context is not None and self.uuid == context.uuid):
            return context
        elif self.uuid == context.uuid:
            statement = statement_repository.get_by_uuid(self.uuid)
            return statement


class SelfReference(StatementReference):

    def __init__(self):
        self.uuid = None
        self.self_reference = True


def serialize_value(value):
    if value is None:
        return None
    elif type(value) == UUID:
        return 'uuid:{}'.format(str(value))
    elif type(value) == int:
        return 'int:{}'.format(value)
    elif type(value) == str:
        return 'str:{}'.format(value)
    elif type(value) == datetime.datetime:
        return 'datetime:{}'.format(datetime.datetime.strftime(value, '%Y-%m-%dT%H:%M:%S.%f'))
    elif type(value) == models.Statement:
        return 'st:{}'.format(value.uuid)


def deserialize_value(value):
    type_str, value_str = value.split(':', 1)
    if type_str == 'uuid':
        return UUID(value_str)
    elif type_str == 'st':
        uuid_ = UUID(value_str)
        return StatementReference(uuid_=uuid_)
    elif type_str == 'int':
        return int(value_str)
    elif type_str == 'str':
        return str(value_str)
    elif type_str == 'datetime':
        return datetime.datetime.strptime(value_str, '%Y-%m-%dT%H:%M:%S.%f')
    elif type_str == 'special' and value_str == 'self':
        return SelfReference()
