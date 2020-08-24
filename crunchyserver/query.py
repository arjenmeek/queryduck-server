from sqlalchemy import and_, or_
from sqlalchemy.sql import select

from crunchylib.types import Blob, Statement, Inverted, serialize, deserialize, process_db_row, column_compare, prepare_for_db

from .models import statement_table, blob_table, file_table, volume_table


class PGQuery:

    def __init__(self, repo, query, target, after=None):
        self.repo = repo
        self.query = query
        self.target = target
        self.after = after
        self.db = self.repo.db
        self.select_from = self.target
        self.wheres = []
        self.results = None
        self.process_query()

    def process_query(self):
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

    def get_results(self):
        if self.target == blob_table:
            return self._get_blob_results()
        elif self.target == statement_table:
            return self._get_statement_results()

    def _get_blob_results(self):
        s = select([self.target.c.id, blob_table.c.sha256]).select_from(self.select_from)
        s = s.where(and_(*self.wheres)).distinct(self.target.c.sha256).limit(1000)
        if self.after is not None:
            s = s.where(self.target.c.sha256 > self.after.sha256)
        s = s.order_by(self.target.c.sha256)
        resultset = self.db.execute(s)
        self.results = [Blob(sha256=r_sha256, id_=r_id)
            for r_id, r_sha256 in resultset]
        return self.results

    def _get_statement_results(self):
        s = select([self.target.c.id, self.target.c.uuid]).select_from(self.select_from)
        s = s.where(and_(*self.wheres)).distinct(self.target.c.uuid).limit(1000)
        if self.after is not None:
            s = s.where(self.target.c.uui > self.after.uuid)
        s = s.order_by(self.target.c.uuid)
        resultset = self.db.execute(s)
        self.results = [Statement(uuid_=r_uuid, id_=r_id)
            for r_id, r_uuid in resultset]
        return self.results

    def get_result_values(self):
        if self.target == blob_table:
            statements = self._get_blob_values()
        else:
            statements = self._get_statement_values()
        return statements

    def _get_blob_values(self):
        blob_ids = [b.id for b in self.results]

        s, entities = self.repo.select_full_statements(statement_table)

        main_alias = statement_table.alias('main')

        sub_alias = statement_table.alias('sub')
        sub_from = main_alias.join(sub_alias,
            sub_alias.c.subject_id==main_alias.c.subject_id)
        sub = select([sub_alias.c.id]).select_from(sub_from)
        sub = sub.where(main_alias.c.object_blob_id.in_(blob_ids))

        sub_res = self.db.execute(sub)
        sub_ids = [i[0] for i in sub_res.fetchall()]

        where = or_(
            statement_table.c.id.in_(sub_ids),
        )
        s = s.where(and_(where, statement_table.c.subject_id!=None)).distinct(statement_table.c.id)

        results = self.db.execute(s)
        statements = self.repo.process_result_statements(results, entities)
        return statements

    def _get_statement_values(self):
        statement_ids = [s.id for s in self.results]

        s, entities = self.repo.select_full_statements(self.target)

        sub_alias = statement_table.alias()
        sub_from = self.target.join(sub_alias, sub_alias.c.id==self.target.c.subject_id)
        sub = select([self.target.c.id]).select_from(sub_from)
        sub = sub.where(sub_alias.c.subject_id.in_(statement_ids))

        obj_alias = statement_table.alias()
        obj_from = self.target.join(obj_alias,
            obj_alias.c.object_statement_id==self.target.c.subject_id)
        obj = select([self.target.c.id]).select_from(obj_from)
        obj = obj.where(obj_alias.c.subject_id.in_(statement_ids))

        where = or_(
            self.target.c.subject_id.in_(statement_ids),
            self.target.c.id.in_(statement_ids),
            self.target.c.id.in_(sub),
            self.target.c.id.in_(obj),
        )
        s = s.where(where).distinct(statement_table.c.id)

        results = self.db.execute(s)
        statements = self.repo.process_result_statements(results, entities)
        return statements
