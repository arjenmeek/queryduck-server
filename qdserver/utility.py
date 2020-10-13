from queryduck.serialization import get_native_vtype
from queryduck.types import Statement, Blob, value_types, value_comparison_methods


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
