import datetime
import traceback

from collections import defaultdict
from uuid import UUID, uuid4

from pyramid.view import view_config

from sqlalchemy import and_
from sqlalchemy.sql import select

from crunchylib.exceptions import GeneralError, NotFoundError
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


class StatementController(BaseController):
    """Provide a limited but simplified way to fetch and save Statements"""

    default_schema_keys = [
        'Resource',
        'Transaction',
        'User',
        'type',
        'created_at',
        'created_by',
        'transaction_contains',
        'statement_count'
    ]

    def __init__(self, request):
        """Make relevant services available."""
        self.request = request
        self.db = self.request.find_service(name='db')
        self.t = statement_table

    @view_config(route_name='get_schema', renderer='json')
    def get_schema(self):
        schema = self._get_schema(Value(self.request.matchdict['reference']))
        return {k: v.serialize() for k, v in schema.items()}

    def _get_schema(self, schema_reference):
        self._fill_ids(schema_reference)

        subject = self.t.alias()
        s_from = self.t.join(subject, subject.c.id==self.t.c.subject_id)
        s = select([subject.c.id, subject.c.uuid, self.t.c.object_string])\
            .select_from(s_from)\
            .where(self.t.c.predicate_id==schema_reference.v.id)
        results = self.db.execute(s)

        schema = {string: Value.native(Statement(id_=id_, uuid_=uuid_))
            for id_, uuid_, string in results if string is not None}
        return schema

    @view_config(route_name='establish_schema', renderer='json')
    def establish_schema(self):
        schema_reference = Value(self.request.matchdict['reference'])
        schema = self._establish_schema(schema_reference, self.request.json_body)
        self.db.commit()
        return {k: v.serialize() for k, v in schema.items()}

    def _establish_schema(self, schema_reference, names):
        schema = self._get_schema(schema_reference)

        create_names = []
        for name in names:
            if not name in schema:
                create_names.append(name)

        if len(create_names):
            self._fill_ids(schema_reference)
            for name in create_names:
                self._create_statement(
                    subject_id=None,
                    predicate_id=schema_reference.v.id,
                    object_string=name
                )
            schema = self._get_schema(schema_reference)

        return schema

    @view_config(route_name='create_statements', renderer='json')
    def create_statements(self):
        insert_ids = self._create_statements(self.request.json_body)
        self.db.commit()

    def _create_statements(self, rows):
        insert_ids = []
        for row in rows:
            insert = self.t.insert().values(uuid=uuid4())
            (insert_id,) = self.db.execute(insert).inserted_primary_key
            insert_ids.append(insert_id)

        for idx, row in enumerate(rows):
            statement_values = []
            for e in row:
                if type(e) == int:
                    statement_values.append(insert_ids[e])
                    column_name = 'object_statement_id'
                else:
                    v = Value(e)
                    if v.vtype == 's':
                        self._fill_ids(v)
                    statement_values.append(v.db_value())
                    column_name = v.column_name

            values = {
                'subject_id': statement_values[0],
                'predicate_id': statement_values[1],
                column_name: statement_values[2],
            }
            where = self.t.c.id==insert_ids[idx]
            update = self.t.update().where(where).values(values)
            self.db.execute(update)
        return insert_ids

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
            self._fill_ids([k.v, v.v])

            a = self.t.alias()
            select_from = select_from.join(a, and_(a.c.subject_id==self.t.c.id, a.c.predicate_id==k.v.id), isouter=True)
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
            predicate_key = Value.native(Statement(uuid_=row[predicate.c.uuid]))
            row_object = Value(db_columns=self.t.c, db_row=row, db_entities={'s': object_statement})
            statements_by_id[subject_id].attributes[predicate_key.serialize()].append(row_object)

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
        schema = self._establish_schema(schema_reference, self.default_schema_keys)
        #new_statement_ids = self._schema_transaction(schema, self.request.json_body)
        new_statement_ids = self._create_statements(self.request.json_body)
        self._create_transaction(schema, new_statement_ids)
        print("new", new_statement_ids)
        self.db.commit()

    def _schema_transaction(self, schema, main_statements):
        new_statement_ids = []
        intermediate = []
        for main_statement in main_statements:
            insert_id = self._create_statement(
                subject_id=None,
                predicate_id=schema['type'].v.id,
                object_statement_id=schema['Resource'].v.id
            )
            new_statement_ids.append(insert_id)
            intermediate.append((insert_id, main_statement))

        for insert_id, main_statement in intermediate:
            for p_str, o_strs in main_statement.items():
                predicate = Value(p_str) if ':' in p_str else schema[p_str]
                if type(o_strs) != list:
                    o_strs = [o_strs]
                for o_str in o_strs:
                    object_ = Value(o_str) if ':' in o_str else schema[o_str]
                    create_args = {
                        'subject_id': insert_id,
                        'predicate_id': predicate.v.id,
                        object_.column_name: object_.db_value(),
                    }
                    new_id = self._create_statement(**create_args)
                    new_statement_ids.append(new_id)

        return new_statement_ids

    def _create_transaction(self, schema, statement_ids):
        transaction_id = self._create_statement(
            subject_id=None,
            predicate_id=schema['type'].v.id,
            object_statement_id=schema['Transaction'].v.id
        )
        self._create_statement(
            subject_id=transaction_id,
            predicate_id=schema['created_at'].v.id,
            object_datetime=datetime.datetime.now()
        )
        self._create_statement(
            subject_id=transaction_id,
            predicate_id=schema['statement_count'].v.id,
            object_integer=len(statement_ids)
        )
        for s_id in statement_ids:
            self._create_statement(
                subject_id=transaction_id,
                predicate_id=schema['transaction_contains'].v.id,
                object_statement_id=s_id
            )

    def _create_statement(self, **kwargs):
        """Create a Statement with specified values. None values are changed to be self referential."""
        insert = self.t.insert().values(uuid=uuid4())
        (insert_id,) = self.db.execute(insert).inserted_primary_key
        values = {k: (insert_id if v is None else v) for k, v in kwargs.items()}
        where = self.t.c.id==insert_id
        update = self.t.update().where(where).values(values)
        self.db.execute(update)
        return insert_id

    def _fill_ids(self, statements):
        # TODO: Fetch all in one query
        if type(statements) != list:
            statements = [statements]
        for statement in statements:
            if type(statement) == Value:
                statement = statement.v
            if type(statement) != Statement:
                continue
            s = select([self.t.c.id], limit=1).where(self.t.c.uuid==statement.uuid)
            result = self.db.execute(s)
            statement.id = result.fetchone()['id']
