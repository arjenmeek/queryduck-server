from sqlalchemy import and_, or_
from sqlalchemy.sql import select

from crunchylib.types import Blob, Statement, serialize, deserialize, process_db_row, column_compare, prepare_for_db

from .models import statement_table, blob_table, file_table, volume_table

class Inverted:
    def __init__(self, value):
        self.value = value

class PGQuery:

    def __init__(self, repo, query, target):
        self.repo = repo
        self.query = query
        self.target = target
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
        extra_column = self.target.c.uuid if self.target == statement_table \
            else blob_table.c.sha256
        s = select([self.target.c.id, extra_column]).select_from(self.select_from)
        s = s.where(and_(*self.wheres)).distinct(self.target.c.id).limit(100)

        resultset = self.db.execute(s)
        if self.target == statement_table:
            self.results = [Statement(uuid_=r_uuid, id_=r_id)
                for r_id, r_uuid in resultset]
        elif self.target == blob_table:
            self.results = [Blob(sha256=r_sha256, id_=r_id)
                for r_id, r_sha256 in resultset]
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
        main_from = self.target.join(main_alias, main_alias.c.object_blob_id==self.target.c.id)
        main = select([main_alias.c.id]).select_from(main_from)
        main = main.where(main_alias.c.object_blob_id.in_(blob_ids))

        sub_alias = statement_table.alias('sub')
        sub_from = main_alias.join(sub_alias,
            sub_alias.c.subject_id==main_alias.c.subject_id)
        sub = select([sub_alias.c.id]).select_from(sub_from)
        sub = sub.where(main_alias.c.object_blob_id.in_(blob_ids))

        where = or_(
            statement_table.c.id.in_(sub),
        )
        s = s.where(and_(where, statement_table.c.subject_id!=None)).distinct(statement_table.c.id)

        statement_dict = {}
        results = self.db.execute(s)
        for r in self.repo.process_result_quads(results, entities):
            if r[1] is None:
                continue
            ser = [serialize(e) for e in r]
            statement_dict[ser[0]] = ser[1:]
        return statement_dict

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

        statement_dict = {}
        results = self.db.execute(s)
        for r in self.repo.process_result_quads(results, entities):
            ser = [serialize(e) for e in r]
            statement_dict[ser[0]] = ser[1:]
        return statement_dict
