from datetime import datetime as dt

from pyramid.view import view_config
from sqlalchemy.sql import select

from crunchylib.types import Statement, serialize, deserialize

from ..controllers import BaseController, StatementController
from ..models import statement_table


class SchemaController(BaseController):
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
        self.db = self.request.db
        self.t = statement_table
        self.sc = StatementController(self.request)

    @view_config(route_name='get_schema', renderer='json')
    def get_schema(self):
        schema = self._get_schema(deserialize(self.request.matchdict['reference']))
        return {k: v.serialize() for k, v in schema.items()}

    def _get_schema(self, schema_reference):
        self.sc._fill_ids(schema_reference)

        subject = self.t.alias()
        s_from = self.t.join(subject, subject.c.id==self.t.c.subject_id)
        s = select([subject.c.id, subject.c.uuid, self.t.c.object_string])\
            .select_from(s_from)\
            .where(self.t.c.predicate_id==schema_reference.id)
        results = self.db.execute(s)

        schema = {string: Statement(id_=id_, uuid_=uuid_)
            for id_, uuid_, string in results if string is not None}
        return schema

    @view_config(route_name='establish_schema', renderer='json')
    def establish_schema(self):
        schema_reference = deserialize(self.request.matchdict['reference'])
        schema = self._establish_schema(schema_reference, self.request.json_body)
        return {k: serialize(v) for k, v in schema.items()}

    def _establish_schema(self, schema_reference, names):
        schema = self._get_schema(schema_reference)

        create_names = []
        for name in names:
            if not name in schema:
                create_names.append(name)

        if len(create_names):
            self.sc._fill_ids(schema_reference)
            for name in create_names:
                self.sc._create_statement(
                    subject_id=None,
                    predicate_id=schema_reference.id,
                    object_string=name
                )
            schema = self._get_schema(schema_reference)

        return schema

    @view_config(route_name='schema_transaction', renderer='json', permission='create')
    def schema_transaction(self):
        schema_reference = Value(self.request.matchdict['reference'])
        schema = self._establish_schema(schema_reference, self.default_schema_keys)
        new_statement_ids = self._create_statements(self.request.json_body)
        self._create_transaction(schema, new_statement_ids)

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
            object_datetime=dt.now()
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
