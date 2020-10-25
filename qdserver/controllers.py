import base64

from uuid import uuid4

from pyramid.view import view_config

from queryduck.query import (
    QDQuery,
    Main,
    element_classes,
)
from queryduck.types import Statement, Blob
from queryduck.serialization import serialize, deserialize
from queryduck.utility import transform_doc

from .repository import PGRepository
from .query import PGQuery, FromClauseBuilder


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

    @view_config(route_name="create_statements", renderer="json")
    def create_statements(self):
        statements = self.deserialize_rows(self.request.json_body)
        statements = self.repo.create_statements(statements)

        result = {
            "statements": [],
        }

        for statement in statements:
            result["statements"].append(
                [serialize(v) for v in (statement,) + statement.triple]
            )

        return result

    @view_config(route_name="get_statement", renderer="json")
    def get_statement(self):
        reference = self.request.matchdict["reference"]
        statement = deserialize(reference)
        self.repo.fill_ids(statement)
        result = {
            "reference": serialize(statement),
            "statements": self.repo.get_statement_values([statement]),
        }
        return result

    @view_config(route_name="get_statements", renderer="json")
    def get_statements(self):
        quads = self.repo.get_all_statements()

        result = {
            "statements": [],
        }

        for q in quads:
            result["statements"].append([serialize(e) for e in q])

        return result

    def deserialize_query(self, params, target_name):
        target = Blob if target_name == "blob" else Statement
        q = QDQuery(target)
        q.join(Main())
        def callback(string):
            if string.startswith("alias:"):
                return q.joins[string[6:]]
            else:
                print("DESER", string)
                return self.unique_deserialize(string)

        for k, v in params:
            print("PARAM", k, v)
            cls = element_classes[tuple(k.split("."))]
            element = cls.deserialize(v, callback)
            q.add(element)

        return q

    @view_config(route_name="get_query", renderer="json")
    def get_query(self):
        # target = self.repo.get_target_table(self.request.matchdict["target"])
        query = self.deserialize_query(
            self.request.GET.items(),
            self.request.matchdict["target"],
        )
        query.show()
        values, more = self.repo.get_results(query)
        statements = self.repo.get_additional_values(query, values)
        print(
            "Query results: {} primary, {} additional, {} files".format(
                len(values), len(statements), len({})
            )
        )
        result = {
            "references": [serialize(v) for v in values],
            "statements": self.statements_to_dict(statements),
            "files": {},
            "more": more,
        }
        return result

    @view_config(route_name="query", renderer="json")
    def query(self):
        target = self.repo.get_target_table(self.request.matchdict["target"])
        print("QUERY BODY:", self.request.body)
        body = self.request.json_body

        if "after" in body and body["after"] is not None:
            after = self.unique_deserialize(body["after"])
        else:
            after = None

        pgquery = PGQuery(self.repo, target, after=after)

        if type(body["query"]) == dict:
            query = self._prepare_query(body["query"])
            pgquery.apply_query(query)
        elif type(body["query"]) == list:
            pgquery.apply_query(query)
            print("LIST")
        reference_statements, more = pgquery.get_results()
        statements = pgquery.get_result_values()
        files = self.repo.get_blob_files(
            [s.triple[2] for s in statements if s.triple and type(s.triple[2]) == Blob]
        )

        print(
            "Query results: {} primary, {} additional, {} files".format(
                len(reference_statements), len(statements), len(files)
            )
        )
        result = {
            "references": [serialize(s) for s in reference_statements],
            "statements": self.statements_to_dict(statements),
            "files": self.serialize_files(files),
            "more": more,
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
                statement = Statement(handle=uuid4())
                self.repo.unique_add(statement)
            else:
                statement = self.unique_deserialize(row[0])
            statements.append(statement)

        # fill Statement values and create set of all UUID's involved
        for idx, row in enumerate(serialized_rows):
            statements[idx].triple = tuple(
                [
                    self.unique_deserialize(ser)
                    if type(ser) == str
                    else statements[ser]
                    for ser in row[1:]
                ]
            )

        return statements

    ### Helper methods ###

    def _prepare_query(self, query):
        """Deserialize any values inside the query, and add database IDs."""
        values = []

        def deserialize_reference(ref):
            v = deserialize(ref)
            if hasattr(v, "value"):
                values.append(v.value)
            else:
                values.append(v)
            return v

        query = transform_doc(query, deserialize_reference)
        self.repo.fill_ids(values)
        return query
