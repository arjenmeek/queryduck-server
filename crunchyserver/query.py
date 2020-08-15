from sqlalchemy import and_, or_
from sqlalchemy.sql import select

from crunchylib.types import Blob, Statement, serialize, deserialize, process_db_row, column_compare, prepare_for_db

from .models import statement_table, blob_table, file_table, volume_table

class Inverted:
    def __init__(self, value):
        self.value = value

class PGQuery:

    def __init__(self, query, target):
        self.query = query
        self.target = target
        self.select_from = self.target
        self.wheres = []
        self.results = None
        self.process_query()

    def process_query(self):
        print(self.query)
        stack = [(self.query, self.target, self.target.c.id)]
        while stack:
            q, t, i = stack.pop()
            # q = subquery
            # t = entity against which this is applied
            # i = id column to match further subqueries against
            #     ( = the column the next entity represents)
            if type(q) == dict:
                for k, v in q.items():
                    if type(k) == Statement:
                        a = statement_table.alias()
                        self.select_from = self.select_from.join(a,
                            and_(a.c.subject_id==i,
                                a.c.predicate_id==k.id),
                            isouter=True)
                        stack.append((v, a, a.c.object_statement_id))
                    elif type(k) == Inverted:
                        a = statement_table.alias()
                        if type(v) == Blob or v is None:
                            col = a.c.object_blob_id
                        else:
                            col = a.c.object_statement_id
                        self.select_from = self.select_from.join(a,
                            and_(col==i,
                                a.c.predicate_id==k.value.id),
                            isouter=True)
                        stack.append((v, a, a.c.subject_id))
                    else:
                        self.wheres.append(column_compare(v, k, t.c))
            else:
                if i.name in ('object_statement_id',):
                    self.wheres.append(column_compare(q, 'eq', t.c))
                else:
                    self.wheres.append(i==(q.id if type(q) in (Statement,) else q))

    def get_results(self, db):
        extra_column = self.target.c.uuid if self.target == statement_table \
            else blob_table.c.sha256
        s = select([self.target.c.id, extra_column]).select_from(self.select_from)
        s = s.where(and_(*self.wheres)).distinct(self.target.c.id).limit(100)

        resultset = db.execute(s)
        if self.target == statement_table:
            self.results = [Statement(uuid_=r_uuid, id_=r_id)
                for r_id, r_uuid in resultset]
        elif self.target == blob_table:
            self.results = [Blob(sha256=r_sha256, id_=r_id)
                for r_id, r_sha256 in resultset]
        return self.results
