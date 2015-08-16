import datetime

from sqlalchemy import (
    engine_from_config,
    Boolean,
    Column,
    DateTime,
    ForeignKey,
    Integer,
    String,
)

from sqlalchemy.dialects.postgresql import (
    UUID,
)

from sqlalchemy.ext.declarative import declarative_base

from sqlalchemy.orm import (
    backref,
    relationship,
    sessionmaker,
    scoped_session,
)

from sqlalchemy.orm.session import object_session

from .utility import serialize_value

Base = declarative_base()


def init_model(settings):
    engine = engine_from_config(settings)
    dbmaker = sessionmaker()
    dbmaker.configure(bind=engine)
    return scoped_session(dbmaker)


class Statement(Base):
    __tablename__ = 'statement'

    id = Column(Integer, primary_key=True)
    uuid = Column(UUID(as_uuid=True), index=True, unique=True, nullable=False)

    subject_id = Column(Integer, ForeignKey('statement.id'), index=True)
    predicate_id = Column(Integer, ForeignKey('statement.id'), index=True)
    object_statement_id = Column(Integer, ForeignKey('statement.id'), index=True)
    object_integer = Column(Integer, index=True)
    object_string = Column(String, index=True)
    object_boolean = Column(Boolean, index=True)
    object_datetime = Column(DateTime, index=True)

    subject = relationship('Statement', backref="subject_statements", remote_side=[id],
        primaryjoin='Statement.subject_id==Statement.id', post_update=True)
    predicate = relationship('Statement', backref="predicate_statements", remote_side=[id],
        primaryjoin='Statement.predicate_id==Statement.id', post_update=True)
    object_statement = relationship('Statement', backref="object_statements", remote_side=[id],
        primaryjoin='Statement.object_statement_id==Statement.id', post_update=True)


    def __init__(self, uuid_, subject=None, predicate=None, object_=None):
        self.uuid = uuid_

        if subject:
            self.subject = subject
        if predicate:
            self.predicate = predicate
        if object_:
            self.object = object_

    def __json__(self, request):
        values = [
            serialize_value(self.uuid),
            serialize_value(self.subject),
            serialize_value(self.predicate),
            serialize_value(self.object),
        ]
        return values

    @property
    def object(self):
        if self.object_statement is not None:
            return self.object_statement
        elif self.object_integer is not None:
            return self.object_integer
        elif self.object_string is not None:
            return self.object_string
        elif self.object_boolean is not None:
            return self.object_boolean
        elif self.object_datetime is not None:
            return self.object_datetime

    @object.setter
    def object(self, value):
        if value is None:
            self.object_statement = self
        elif type(value) == int:
            self.object_integer = value
        elif type(value) == str:
            self.object_string = value
        elif type(value) == bool:
            self.object_boolean = value
        elif type(value) == datetime.datetime:
            self.object_datetime = value
        elif type(value) == Statement:
            self.object_statement = value
