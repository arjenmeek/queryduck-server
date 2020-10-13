from sqlalchemy import (
    engine_from_config,
    Column,
    Table,
    MetaData,
    BigInteger,
    Boolean,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    Numeric,
    String,
)

from sqlalchemy.dialects.postgresql import (
    BYTEA,
    UUID,
)


def init_db(settings):
    engine = engine_from_config(settings)
    meta.create_all(engine)
    return engine


meta = MetaData()

statement_table = Table(
    "statement",
    meta,
    Column("id", Integer, primary_key=True),
    Column("handle", UUID(as_uuid=True), index=True, unique=True, nullable=False),
    Column("subject_id", Integer, ForeignKey("statement.id"), index=True),
    Column("predicate_id", Integer, ForeignKey("statement.id"), index=True),
    Column("object_statement_id", Integer, ForeignKey("statement.id")),
    Column("object_blob_id", Integer, ForeignKey("blob.id")),
    Column("object_integer", BigInteger),
    Column("object_decimal", Numeric),
    Column("object_string", String),
    Column("object_boolean", Boolean),
    Column("object_datetime", DateTime),
)
Index(
    "ix_statement_object_statement_id",
    statement_table.c.object_statement_id,
    postgresql_where=statement_table.c.object_statement_id != None,
),
Index(
    "ix_statement_object_blob_id",
    statement_table.c.object_blob_id,
    postgresql_where=statement_table.c.object_blob_id != None,
),
Index(
    "ix_statement_object_integer",
    statement_table.c.object_integer,
    postgresql_where=statement_table.c.object_integer != None,
),
Index(
    "ix_statement_object_decimal",
    statement_table.c.object_decimal,
    postgresql_where=statement_table.c.object_decimal != None,
),
Index(
    "ix_statement_object_string",
    statement_table.c.object_string,
    postgresql_where=statement_table.c.object_string != None,
),
Index(
    "ix_statement_object_boolean",
    statement_table.c.object_boolean,
    postgresql_where=statement_table.c.object_boolean != None,
),
Index(
    "ix_statement_object_datetime",
    statement_table.c.object_datetime,
    postgresql_where=statement_table.c.object_datetime != None,
)


volume_table = Table(
    "volume",
    meta,
    Column("id", Integer, primary_key=True),
    Column("reference", String, index=True, unique=True),
)


blob_table = Table(
    "blob",
    meta,
    Column("id", Integer, primary_key=True),
    Column("handle", BYTEA, index=True, unique=True, nullable=False),
)

file_table = Table(
    "file",
    meta,
    Column("id", Integer, primary_key=True),
    Column("blob_id", Integer, ForeignKey("blob.id"), index=True),
    Column("volume_id", Integer, ForeignKey("volume.id"), index=True),
    Column("path", BYTEA, index=True),
    Column("size", BigInteger, index=True),
    Column("mtime", DateTime, index=True),
    Column("lastverify", DateTime, index=True),
    Index("ix_volume_path", "volume_id", "path", unique=True),
)
