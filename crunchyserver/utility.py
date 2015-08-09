import datetime

from uuid import UUID

from . import models
from .exceptions import GeneralError

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

def deserialize_value(value, db=None, context=None):
    type_str, value_str = value.split(':', 1)
    if type_str == 'uuid':
        return UUID(value_str)
    elif type_str == 'st':
        uuid_ = UUID(value_str)
        if context is not None and uuid_ == context.uuid:
            statement = context
        elif db is not None:
            statement = db.query(models.Statement).filter_by(uuid=uuid_).one()
        else:
            raise GeneralError("Unable to fetch statement, no db object available")
        return statement
    elif type_str == 'int':
        return int(value_str)
    elif type_str == 'str':
        return str(value_str)
    elif type_str == 'datetime':
        return datetime.datetime.strptime(value_str, '%Y-%m-%dT%H:%M:%S.%f')
    elif type_str == 'special' and value_str == 'self':
        return context
