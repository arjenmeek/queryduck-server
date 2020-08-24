import traceback

from datetime import datetime as dt
from uuid import uuid4

from pyramid.view import view_config
from sqlalchemy import and_, or_
from sqlalchemy.sql import select

from crunchylib.types import Blob, Statement, serialize, deserialize, process_db_row, column_compare, prepare_for_db
from crunchylib.utility import transform_doc

from .models import statement_table, blob_table, file_table, volume_table


@view_config(context=Exception, renderer='json')
def error_view(e, request):
    print(traceback.format_exc())
    return {}


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
        self.db = self.request.db
        self.t = statement_table

    @view_config(route_name='create_statements', renderer='json')
    def create_statements(self):
        insert_ids = self._create_statements(self.request.json_body)

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
                    v = deserialize(e)
                    self._fill_ids(v)
                    value, column_name = prepare_for_db(v)
                    statement_values.append(value)

            values = {
                'subject_id': statement_values[0],
                'predicate_id': statement_values[1],
                column_name: statement_values[2],
            }
            where = self.t.c.id==insert_ids[idx]
            update = self.t.update().where(where).values(values)
            self.db.execute(update)
        return insert_ids

    def _get_statement_values(self, statements):
        su = self.t.alias()
        pr = self.t.alias()
        ob = self.t.alias()
        statement_ids = [s.id for s in statements]

        # If you're reading this and have suggestions on a cleaner style that
        # doesn't exceed 80 columns, please let me know!
        select_from = self.t\
            .join(su,
                su.c.id==self.t.c.subject_id, isouter=True)\
            .join(pr,
                pr.c.id==self.t.c.predicate_id, isouter=True)\
            .join(ob,
                ob.c.id==self.t.c.object_statement_id, isouter=True)\
            .join(blob_table,
                blob_table.c.id==self.t.c.object_blob_id, isouter=True)\
            .join(file_table,
                file_table.c.blob_id==self.t.c.object_blob_id, isouter=True)\
            .join(volume_table,
                volume_table.c.id==file_table.c.volume_id, isouter=True)


        where = or_(self.t.c.subject_id.in_(statement_ids),
            self.t.c.id.in_(statement_ids))

        s = select([
            self.t,
            su.c.uuid,
            pr.c.uuid,
            ob.c.uuid,
            blob_table.c.sha256,
            file_table.c.path,
            volume_table.c.reference,
        ]).select_from(select_from).where(where)
        s = s.distinct(self.t.c.id)

        statement_dict = {}
        for row in self.db.execute(s):
            entities = {
                's': ob,
                'blob': blob_table,
                'volume': volume_table,
                'file': file_table,
            }
            elements = (
                serialize(Statement(uuid_=row[su.c.uuid])),
                serialize(Statement(uuid_=row[pr.c.uuid])),
                serialize(process_db_row(row, self.t.c, entities)[0]),
            )
            key = serialize(Statement(uuid_=row[self.t.c.uuid]))
            statement_dict[key] = elements
        return statement_dict

    @view_config(route_name='get_statement', renderer='json')
    def get_statement(self):
        reference = self.request.matchdict['reference']
        statement = deserialize(reference)
        self._fill_ids(statement)
        result = {
            'reference': serialize(statement),
            'statements': self._get_statement_values([statement]),
        }
        return result

    @view_config(route_name='query_statements', renderer='json')
    def query_statements(self):
        query = self.request.json_body
        statements = self._query_statements(query['query'])

        result = {
            'references': [serialize(s) for s in statements],
            'statements': self._get_statement_values(statements),
        }
        return result

    def _prepare_query(self, query):
        """Deserialize any values inside the query, and add database IDs."""
        values = []
        def deserialize_reference(ref):
            if ':' in ref:
                v = deserialize(ref)
                values.append(v)
            else:
                v = ref
            return v
        query = transform_doc(query, deserialize_reference)
        self._fill_ids(values)
        return query

    def _query_statements(self, query):
        query = self._prepare_query(query)

        select_from = self.t
        wheres = []
        stack = [(query, self.t)]
        while stack:
            q, t = stack.pop()
            if type(q) == dict:
                for k, v in q.items():
                    if type(k) == Statement:
                        a = self.t.alias()
                        select_from = select_from.join(a,
                            and_(a.c.subject_id==t.c.id,
                                a.c.predicate_id==k.id),
                            isouter=True)
                        stack.append((v, a))
                    else:
                        wheres.append(column_compare(v, k, t.c))
            else:
                wheres.append(column_compare(q, 'eq', t.c))

        s = select([self.t.c.id, self.t.c.uuid]).select_from(select_from)
        s = s.where(and_(*wheres)).distinct(self.t.c.id)
        results = self.db.execute(s)
        statements = [Statement(uuid_=r_uuid, id_=r_id)
            for r_id, r_uuid in results]
        return statements

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
            if type(statement) == list:
                self._fill_ids(statement)
            if type(statement) not in (Statement, Blob):
                continue
            if type(statement) == Statement:
                s = select([self.t.c.id], limit=1).where(self.t.c.uuid==statement.uuid)
                result = self.db.execute(s)
            elif type(statement) == Blob:
                s = select([blob_table.c.id], limit=1).where(blob_table.c.sha256==statement.sha256)
                result = self.db.execute(s)
            statement.id = result.fetchone()['id']
