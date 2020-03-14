import base64
import datetime
import os

from sqlalchemy import (
    engine_from_config,
    BigInteger,
    Boolean,
    Column,
    DateTime,
    Float,
    ForeignKey,
    Index,
    Integer,
    String,
)

from sqlalchemy.dialects.postgresql import (
    BYTEA,
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

Base = declarative_base()


def init_model(settings):
    """Initialize the application's models and return a scoped session."""
    engine = engine_from_config(settings)
    dbmaker = sessionmaker()
    dbmaker.configure(bind=engine)
    return scoped_session(dbmaker)


class Statement(Base):
    """The Statement this application is centered around."""
    __tablename__ = 'statement'

    id = Column(Integer, primary_key=True)
    uuid = Column(UUID(as_uuid=True), index=True, unique=True, nullable=False)

    subject_id = Column(Integer, ForeignKey('statement.id'), index=True)
    predicate_id = Column(Integer, ForeignKey('statement.id'), index=True)
    object_statement_id = Column(Integer, ForeignKey('statement.id'))
    object_blob_id = Column(Integer, ForeignKey('blob.id'))
    object_integer = Column(Integer)
    object_float = Column(Float)
    object_string = Column(String)
    object_boolean = Column(Boolean)
    object_datetime = Column(DateTime)

    subject = relationship('Statement', backref="subject_statements", remote_side=[id],
        primaryjoin='Statement.subject_id==Statement.id', post_update=True)
    predicate = relationship('Statement', backref="predicate_statements", remote_side=[id],
        primaryjoin='Statement.predicate_id==Statement.id', post_update=True)
    object_statement = relationship('Statement', backref="object_statements", remote_side=[id],
        primaryjoin='Statement.object_statement_id==Statement.id', post_update=True)
    object_blob = relationship('Blob', backref="object_statements", lazy='joined')

    __table_args__ = (
        Index('ix_statement_object_statement_id', 'object_statement_id',
            postgresql_where=object_statement_id!=None),
        Index('ix_statement_object_blob_id', 'object_blob_id',
            postgresql_where=object_blob_id!=None),
        Index('ix_statement_object_integer', 'object_integer',
            postgresql_where=object_integer!=None),
        Index('ix_statement_object_float', 'object_float',
            postgresql_where=object_float!=None),
        Index('ix_statement_object_string', 'object_string',
            postgresql_where=object_string!=None),
        Index('ix_statement_object_boolean', 'object_boolean',
            postgresql_where=object_boolean!=None),
        Index('ix_statement_object_datetime', 'object_datetime',
            postgresql_where=object_datetime!=None),
    )

statement_table = Statement.__table__


class Volume(Base):
    __tablename__ = 'volume'

    id = Column(Integer, primary_key=True)
    reference = Column(String, index=True, unique=True)

    def __json__(self, request):
        return {'id': self.id, 'reference': self.reference}


class Blob(Base):
    __tablename__ = 'blob'

    id = Column(Integer, primary_key=True)
    sha256 = Column(BYTEA, index=True, unique=True)

    is_blob = True

    def __init__(self, sha256):
        self.sha256 = sha256

    def reference(self):
        return 'blob:{}'.format(base64.b64encode(self.sha256).decode('utf-8'))

    def get_identifier(self):
        return base64.b64encode(self.sha256).decode('utf-8')

    def __json__(self, request):
        blob_data = {
            'id': self.id,
            'sha256': base64.b64encode(self.sha256).decode('utf-8')
        }
        return blob_data


class File(Base):
    __tablename__ = 'file'
    __table_args__ = (Index('ix_volume_path', 'volume_id', 'path', unique=True),)

    id = Column(Integer, primary_key=True)
    blob_id = Column(Integer, ForeignKey('blob.id'), index=True)
    volume_id = Column(Integer, ForeignKey('volume.id'), index=True)

    blob = relationship('Blob', backref='files')
    volume = relationship('Volume', backref='files')

    path = Column(BYTEA, index=True)
    size = Column(BigInteger, index=True)
    mtime = Column(DateTime, index=True)
    lastverify = Column(DateTime, index=True)

    def __json__(self, request):
        return {
            'id': self.id,
            'volume_id': self.volume_id,
            'path': os.fsdecode(self.path),
            'sha256': base64.b64encode(self.blob.sha256).decode() if self.blob else None,
            'size': self.size,
            'mtime': self.get_mtime_string() if self.mtime else None,
        }

    def get_mtime_string(self):
        mtime_string = self.mtime.isoformat()
        if len(mtime_string) == 19:
            mtime_string += '.000000'
        return mtime_string
