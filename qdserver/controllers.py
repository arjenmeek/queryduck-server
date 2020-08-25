import base64

from uuid import uuid4

from pyramid.view import view_config

from queryduck.types import Statement, Blob, serialize, deserialize
from queryduck.utility import transform_doc

from .repository import PGRepository
from .query import PGQuery, Inverted


class BaseController(object):
    """Provide a basic Controller class to extend."""

    def __init__(self, request):
        """Make relevant services available."""
        self.request = request
        self.db = self.request.db


class StatementController(BaseController):
    """Provide a limited but simplified way to fetch and save Statements"""

    def __init__(self, request):
        """Make relevant services available."""
        self.request = request
        self.repo = PGRepository(self.request.db)

    ### View methods ###

    @view_config(route_name='create_statements', renderer='json')
    def create_statements(self):
        statements = self.deserialize_rows(self.request.json_body)
        statements = self.repo.create_statements(statements)

        result = {
            'statements': [],
        }

        for statement in statements:
            result['statements'].append([serialize(v)
                for v in (statement,) + statement.triple])

        return result

    @view_config(route_name='get_statement', renderer='json')
    def get_statement(self):
        reference = self.request.matchdict['reference']
        statement = deserialize(reference)
        self.repo.fill_ids(statement)
        result = {
            'reference': serialize(statement),
            'statements': self.repo.get_statement_values([statement]),
        }
        return result

    @view_config(route_name='get_statements', renderer='json')
    def get_statements(self):
        quads = self.repo.get_all_statements()

        result = {
            'statements': [],
        }

        for q in quads:
            result['statements'].append([serialize(e) for e in q])

        return result

    @view_config(route_name='query_statements', renderer='json')
    def query_statements(self):
        print("QUERY BODY:", self.request.body)
        body = self.request.json_body
        if 'target' in body:
            target = self.repo.get_target_table(body['target'])
        else:
            target = self.repo.get_target_table('statement')

        if 'after' in body and body['after'] is not None:
            after = self.unique_deserialize(body['after'])
        else:
            after = None

        query = self._prepare_query(body['query'])
        pgquery = PGQuery(self.repo, query, target, after=after)
        reference_statements = pgquery.get_results()
        statements = pgquery.get_result_values()
        files = self.repo.get_blob_files([s.triple[2] for s in statements
            if type(s.triple[2]) == Blob])

        result = {
            'references': [serialize(s) for s in reference_statements],
            'statements': self.statements_to_dict(statements),
            'files': self.serialize_files(files),
        }
        return result

    ### Worker methods ###

    def serialize_files(self, files):
        serialized_files = {}
        for blob, v in files.items():
            k = serialize(blob)
            serialized_files[k] = [serialize(f) for f in v]

        return serialized_files

    def statements_to_dict(self, statements):
        statement_dict = {}
        for s in statements:
            if not s.triple or not s.triple[0]:
                continue
            statement_dict[serialize(s)] = [serialize(e) for e in s.triple]
        return statement_dict

    def unique_deserialize(self, ref):
        """Ensures there is only ever one instance of the same Statement present"""
        v = deserialize(ref)
        v = self.repo.unique_add(v)
        return v

    def deserialize_rows(self, serialized_rows):
        # create initial Statements without values, but with final UUID's
        statements = []
        for row in serialized_rows:
            if row[0] is None:
                statement = Statement(uuid_=uuid4())
                self.repo.unique_add(statement)
            else:
                statement = self.unique_deserialize(row[0])
            statements.append(statement)

        # fill Statement values and create set of all UUID's involved
        for idx, row in enumerate(serialized_rows):
            statements[idx].triple = tuple([self.unique_deserialize(ser)
                if type(ser) == str else statements[ser] for ser in row[1:]])

        return statements

    ### Helper methods ###

    def _prepare_query(self, query):
        """Deserialize any values inside the query, and add database IDs."""
        values = []
        def deserialize_reference(ref):
            if ref.startswith('~') and ':' in ref:
                s = deserialize(ref[1:])
                v = Inverted(s)
                values.append(s)
            elif ':' in ref:
                v = deserialize(ref)
                values.append(v)
            else:
                v = ref
            return v
        query = transform_doc(query, deserialize_reference)
        self.repo.fill_ids(values)
        return query
