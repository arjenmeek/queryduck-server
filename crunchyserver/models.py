from sqlalchemy import (
    engine_from_config,
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
    engine = engine_from_config(settings)
    dbmaker = sessionmaker()
    dbmaker.configure(bind=engine)
    return scoped_session(dbmaker)
