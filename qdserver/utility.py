from sqlalchemy import and_

from .models import statement_table
from .errors import UserError, TodoError

from queryduck.constants import Component
from queryduck.serialization import get_native_vtype
from queryduck.types import Statement, Blob, value_types, value_comparison_methods
from queryduck.query import QueryEntity


class EntitySet:
    def __init__(self, aliases):
        self.aliases = aliases
        self.entities = {"main": self.aliases["main"]}
        self.fromclause = aliases["main"]

    def register_entity(self, key, entity):
        self.entities[key] = entity

    def get_alias(self, key):
        stack = []
        cur = self.entities[key]
        while cur.key is not None and cur.key not in self.aliases:
            stack.append(cur.key)
            cur = cur.target

        for k in reversed(stack):
            self.add_entity(k, self.entities[k])
        return self.aliases[key]

    def add_entity(self, key, entity):
        target = entity.target
        alias = statement_table.alias(key)
        self.aliases[key] = alias

        target_alias = self.aliases[target.key]
        if entity.value_component == Component.OBJECT:
            lhs = alias.c.subject_id
        elif entity.value_component == Component.SUBJECT:
            if target.value_type == Blob:
                lhs = alias.c.object_blob_id
            else:
                lhs = alias.c.object_statement_id

        if entity.meta or target.value_component == Component.SELF:
            rhs = target_alias.c.id
        elif target.value_component == Component.OBJECT:
            if entity.value_type == Blob:
                rhs = target_alias.c.object_blob_id
            else:
                rhs = target_alias.c.object_statement_id
        elif target.value_component == Component.SUBJECT:
            rhs = target_alias.c.subject_id

        where = lhs == rhs
        if len(entity.predicates):
            predicate_ids = [p.id for p in entity.predicates]
            where = and_(where, alias.c.predicate_id.in_(predicate_ids))
        self.fromclause = self.fromclause.join(alias, where, isouter=True)

    def get_alias_column(self, alias, component, vtype):
        if component == Component.SELF:
            if not vtype in ("s", "none"):
                raise UserError(f"Invalid value type for self: {vtype}")
            column = alias.c.id
        elif component == Component.SUBJECT:
            if not vtype in ("s", "none"):
                raise UserError(f"Invalid value type for subject: {vtype}")
            column = alias.c.subject_id
        elif component == Component.OBJECT:
            vtype_info = value_types[vtype]
            column = alias.c[vtype_info["column_name"]]
        return column

    def db_compare(self, f):
        lhs = f.lhs
        op = f.keyword
        rhs = f.rhs
        if isinstance(lhs, QueryEntity):
            lhs_alias = self.get_alias(lhs.key)
            lhs_type = None
        else:
            lhs_alias = None
            lhs_type = get_native_vtype(lhs)

        if isinstance(rhs, QueryEntity):
            rhs_alias = self.get_alias(rhs.key)
            rhs_type = None
        else:
            rhs_alias = None
            rhs_type = get_native_vtype(rhs[0] if type(rhs) == list else rhs)

        if lhs_alias is not None and rhs_alias is not None:
            lhs_operand = self.get_alias_column(lhs_alias, lhs.value_component, "s")
            rhs_operand = self.get_alias_column(rhs_alias, rhs.value_component, "s")
        elif lhs_alias is not None:
            lhs_operand = self.get_alias_column(lhs_alias, lhs.value_component, rhs_type)
            if rhs_type == "file":
                if type(rhs) == list:
                    rhs_operand = [f.blob.id for f in rhs]
                else:
                    rhs_operand = rhs.blob.id
            elif rhs_type in ("s", "blob"):
                if type(rhs) == list:
                    rhs_operand = [e.id for e in rhs]
                else:
                    rhs_operand = rhs.id
            else:
                rhs_operand = rhs
        else:
            raise TodoError()

        op_method = value_comparison_methods[op]
        return getattr(lhs_operand, op_method)(rhs_operand)


def process_db_row(db_row, db_columns, db_entities):
    for try_vtype, options in value_types.items():
        if not "column_name" in options or not options["column_name"] in db_columns:
            continue
        column = db_columns[options["column_name"]]
        db_value = db_row[column]
        if db_value is None or try_vtype == "none":
            continue
        vtype = try_vtype
        break
    else:
        raise QDValueError("Cannot process DB row {}".format(db_row))

    if vtype == "s":
        handle = db_row[db_entities["s"].c.handle]
        v = Statement(handle=handle, id_=db_value)
    elif vtype == "blob":
        handle = db_row[db_entities["blob"].c.handle]
        v = Blob(handle=handle, id_=db_value)
    else:
        v = db_value

    return v, vtype


def prepare_for_db(native_value):
    vtype = get_native_vtype(native_value)
    if vtype in ("s", "blob"):
        value = native_value.id
    else:
        value = native_value
    return value, value_types[vtype]["column_name"]


def column_compare(value, op, columns):
    vtype = get_native_vtype(value[0] if type(value) == list else value)
    column = columns[value_types[vtype]["column_name"]]
    op_method = value_comparison_methods[op]
    if type(value) == list:
        db_value = [v.id if vtype in ("s", "blob") else v for v in value]
    else:
        db_value = value.id if vtype in ("s", "blob") else value
    return getattr(column, op_method)(db_value)


def final_column_compare(value, op, columns):
    vtype = get_native_vtype(value[0] if type(value) == list else value)
    column = columns[value_types[vtype]["column_name"]]
    op_method = value_comparison_methods[op]
    if type(value) == list:
        db_value = [v.id if vtype in ("s", "blob") else v for v in value]
    else:
        db_value = value.id if vtype in ("s", "blob") else value
    return column.label(None), op_method, db_value
