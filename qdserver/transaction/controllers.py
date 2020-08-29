import json
import uuid

from datetime import datetime as dt

from pyramid.view import view_config
from sqlalchemy.sql import select

from queryduck.constants import DEFAULT_SCHEMA_FILES
from queryduck.serialization import serialize, deserialize
from queryduck.types import Statement
from queryduck.schema import Bindings

from ..controllers import BaseController, StatementController
from ..models import statement_table


class TransactionController(BaseController):
    """Provide a limited but simplified way to fetch and save Statements"""

    def __init__(self, request):
        """Make relevant services available."""
        self.request = request
        self.db = self.request.db
        self.t = statement_table
        self.sc = StatementController(self.request)
        self.repo = self.sc.repo
        self._bindings = None

    def _bindings_from_schemas(self, schemas):
        bindings_content = {}
        for schema in schemas:
            for k, v in schema['bindings'].items():
                bindings_content[k] = self.sc.unique_deserialize(v)
        bindings = Bindings(bindings_content)
        return bindings

    def get_bindings(self):
        if self._bindings is None:
            schemas = []
            for filename in DEFAULT_SCHEMA_FILES:
                filepath = '../queryduck/schemas/{}'.format(filename)
                with open(filepath, 'r') as f:
                    schemas.append(json.load(f))
            self._bindings = self._bindings_from_schemas(schemas)
        return self._bindings

    @view_config(route_name='submit_transaction', renderer='json', permission='create')
    def submit_transaction(self):
        statements = self.sc.deserialize_rows(self.request.json_body)
        transaction_statements = self._wrap_transaction(statements)
        all_statements = self.repo.create_statements(statements + transaction_statements)
        [print(s, s.triple) for s in statements]

        result = {
            'statements': [],
        }

        for statement in statements:
            result['statements'].append(serialize(statement))

        return result

    def _wrap_transaction(self, statements):
        b = self.get_bindings()
        transaction = Statement(uuid.uuid4())
        transaction.triple = (transaction, b.type, b.Resource)
        transaction_statements = [
            transaction,
            Statement(uuid.uuid4(), triple=(transaction, b.type, b.Transaction)),
            Statement(uuid.uuid4(), triple=(transaction, b.createdAt, dt.now())),
            Statement(uuid.uuid4(), triple=(transaction, b.createdBy, self.request.authenticated_userid)),
            Statement(uuid.uuid4(), triple=(transaction, b.statementCount, len(statements))),
        ]
        for statement in statements:
            transaction_statements.append(Statement(uuid.uuid4(),
                triple=(transaction, b.transactionContains, statement)))
        final_statements = [self.repo.unique_add(s) for s in transaction_statements]
        return final_statements
