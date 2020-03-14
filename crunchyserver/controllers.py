import datetime
import traceback

from collections import defaultdict
from uuid import UUID, uuid4

from pyramid.view import view_config

from sqlalchemy import and_
from sqlalchemy.sql import select

from crunchylib.exceptions import GeneralError, NotFoundError
from crunchylib.utility import deserialize_value, serialize_value
from crunchylib.value import Statement, Value

from .models import statement_table

@view_config(context=Exception, renderer='json')
def error_view(e, request):
    #log or do other stuff to exc...
    print(traceback.format_exc())
    return {}


class BaseController(object):
    """Provide a basic Controller class to extend."""

    def __init__(self, request):
        """Make relevant services available."""
        self.request = request
        self.db = self.request.find_service(name='db')


class SimpleStatementController(BaseController):
    """Provide a limited but simplified way to fetch and save Statements"""

    def __init__(self, request):
        """Make relevant services available."""
        self.request = request
        self.db = self.request.find_service(name='db')
        self.t = statement_table

    @view_config(route_name='get_schema', renderer='json')
    def get_schema(self):
        uuid_ = UUID(self.request.matchdict['reference'])
        s = select([self.t.c.id]).select_from(self.t).where(self.t.c.uuid==uuid_)
        (schema_id,) = self.db.execute(s).fetchone()

        subject = self.t.alias()
        s_from = self.t.join(subject, subject.c.id==self.t.c.subject_id)
        s = select([subject.c.uuid, self.t.c.object_string])\
            .select_from(s_from)\
            .where(self.t.c.predicate_id==schema_id)
        results = self.db.execute(s)
        schema_map = {string: 'st:{}'.format(uuid) for uuid, string in results if string is not None}
        return schema_map

    def _process_filter(self, key_string, value_string):
        key = Value(key_string)

        if value_string.startswith('_'):
            op, v_part = value_string[1:].split('_', 1)
        else:
            op = 'eq'
            v_part = value_string
        v = Value(v_part) if len(v_part) else None

        return key, op, v

    def _find_statements(self, filters):
        select_from = self.t
        wheres = []
        for k_str, v_str in filters:
            k, op, v = self._process_filter(k_str, v_str)
            self._fill_statement_ids([k.content, v.content])

            a = self.t.alias()
            select_from = select_from.join(a, and_(a.c.subject_id==self.t.c.id, a.c.predicate_id==k.content.id), isouter=True)
            wheres.append(v.column_compare(op, a.c))
        s = select([self.t.c.id, self.t.c.uuid]).select_from(select_from).where(and_(*wheres))
        results = self.db.execute(s)
        statements = [Statement(uuid_=r_uuid, id_=r_id) for r_id, r_uuid in results]
        #statements_by_id = {r_id: defaultdict(list, uuid=str(r_uuid)) for r_id, r_uuid in results}
        return statements

    def _add_statement_values(self, statements):
        """Modify set of statement dicts in place to add values"""
        predicate = self.t.alias()
        object_statement = self.t.alias()
        statements_by_id = {s.id: s for s in statements}

        select_from = self.t.join(predicate, predicate.c.id==self.t.c.predicate_id, isouter=True)\
            .join(object_statement, object_statement.c.id==self.t.c.object_statement_id, isouter=True)
        where = self.t.c.subject_id.in_(statements_by_id.keys())
        s = select([self.t, predicate.c.uuid, object_statement.c.uuid])\
            .select_from(select_from).where(where)

        for row in self.db.execute(s):
            subject_id = row[self.t.c.subject_id]
            predicate_key = serialize_value({'uuid': row[predicate.c.uuid]})
            row_object = Value(db_columns=self.t.c, db_row=row, db_entities={'st': object_statement})
            statements_by_id[subject_id].attributes[predicate_key].append(row_object)

    @view_config(route_name='find_statements', renderer='json')
    def find_statements(self):
        """Return multiple Statements based on filters."""
        filters = self.request.GET.items()
        statements = self._find_statements(filters=filters)
        self._add_statement_values(statements)
        return statements

    @view_config(route_name='schema_transaction', renderer='json', permission='create')
    def schema_transaction(self):
        schema_reference = Value(self.request.matchdict['reference'])
        self._fill_statement_ids([schema_reference.content])
        schema_id = schema_reference.content.id
        print(schema_reference, schema_id)
        schema = {}
        new_statement_ids = []
        for name in ('Resource', 'Transaction', 'User', 'type', 'created_at', 'created_by', 'transaction_contains', 'statement_count'):
            schema[name] = self._get_schema_attribute(schema_id, name)
        intermediate = []
        for main_statement in self.request.json_body:
            print(main_statement)
            insert_id = self._create_statement(subject_id=None, predicate_id=schema['type'], object_statement_id=schema['Resource'])
            new_statement_ids.append(insert_id)
            intermediate.append((insert_id, main_statement))

        for insert_id, main_statement in intermediate:
            for key_str, values in main_statement.items():
                if ':' in key_str:
                    pass
                else:
                    key_id = self._get_schema_attribute(schema_id, key_str)
                if type(values) != list:
                    values = [values]
                for value_str in values:
                    create_args = {'subject_id': insert_id, 'predicate_id': key_id}
                    if ':' in value_str:
                        v = Value(value_str)
                        create_args[v.column_name] = v.db_value()
                    else:
                        create_args['object_statement_id'] = self._get_schema_attribute(schema_id, value_str)
                    new_id = self._create_statement(**create_args)
                    new_statement_ids.append(new_id)

        transaction_id = self._create_statement(subject_id=None, predicate_id=schema['type'], object_statement_id=schema['Transaction'])
        self._create_statement(subject_id=transaction_id, predicate_id=schema['created_at'], object_datetime=datetime.datetime.now())
        self._create_statement(subject_id=transaction_id, predicate_id=schema['statement_count'], object_integer=len(new_statement_ids))
        for s_id in new_statement_ids:
            self._create_statement(subject_id=transaction_id, predicate_id=schema['transaction_contains'], object_statement_id=s_id)

        print("new", new_statement_ids)
        self.db.commit()

    def _get_schema_attribute(self, schema_id, attribute_name):
        where = and_(self.t.c.predicate_id==schema_id, self.t.c.object_string==attribute_name)
        s = select([self.t.c.subject_id], limit=1).where(where)
        row = self.db.execute(s).first()
        if row:
            return row[self.t.c.subject_id]
        else:
            return self._create_statement(subject_id=None, predicate_id=schema_id, object_string=attribute_name)

    def _create_statement(self, **kwargs):
        """Create a Statement with specified values. None values are changed to be self referential."""
        insert = self.t.insert().values(uuid=uuid4())
        (insert_id,) = self.db.execute(insert).inserted_primary_key
        values = {k: (insert_id if v is None else v) for k, v in kwargs.items()}
        where = self.t.c.id==insert_id
        update = self.t.update().where(where).values(values)
        self.db.execute(update)
        return insert_id

    def _get_object_type_column(self, type_):
        column_names = {
            int: 'object_integer',
            float: 'object_float',
            str: 'object_string',
            bool: 'object_boolean',
            dict: 'object_statement_id',
            type(None): '',
            datetime.datetime: 'object_datetime',
        }
        return column_names[type_]

    def _get_statement_id(self, statement):
        s = select([self.t.c.id], limit=1).where(self.t.c.uuid==statement.uuid)
        result = self.db.execute(s)
        statement.id = result.fetchone()['id']
        return statement.id

    def _fill_statement_ids(self, statements):
        for statement in statements:
            if type(statement) != Statement:
                continue
            s = select([self.t.c.id], limit=1).where(self.t.c.uuid==statement.uuid)
            result = self.db.execute(s)
            statement.id = result.fetchone()['id']
            print("FOUND", statement.id, statement.uuid)
