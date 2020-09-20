from itertools import islice

from sqlalchemy import and_, or_
from sqlalchemy.sql import select

from queryduck.query import (
    MatchObject,
    MatchSubject,
    MetaObject,
    MetaSubject,
    FetchObject,
    FetchSubject,
)

from queryduck.serialization import serialize, deserialize
from queryduck.types import (
    Blob,
    Statement,
    File,
)

from .models import statement_table, blob_table, file_table, volume_table
from .utility import process_db_row, column_compare, prepare_for_db


class PGQuery:

    def __init__(self, repo, query, target, after=None):
        self.repo = repo
        self.query = query
        self.target = target
        self.after = after
        self.info_predicates = []
        self.db = self.repo.db
        self.select_from = self.target
        self.wheres = []
        self.results = None
        self.stack = []
        self.additional_stack = []
        self.additionals = []
        self.apply_query(query)
        self.limit = 1000

    def _apply_join(self, key, rhs_column, v, t):
        lhs_name, id_name = key.get_join_columns(v, t)
        a = statement_table.alias()
        self.select_from = self.select_from.join(a,
            and_(a.c[lhs_name]==rhs_column,
                a.c.predicate_id==key.value.id),
            isouter=True)
        self.stack.append((v, a, id_name))

    def apply_query(self, query):
        self.stack.append((query, self.target, "id"))
        while self.stack:
            q, t, c = self.stack.pop()
            # q = subquery
            # t = entity against which this is applied
            # c = id column name to match further subqueries against
            #     ( = the column the next entity represents)
            if type(q) == dict:
                for k, v in q.items():
                    if type(k) in (MatchObject, MatchSubject, MetaObject, MetaSubject):
                        rhs_column = t.c.id if type(k) == MetaObject else t.c[c]
                        self._apply_join(k, rhs_column, v, t)
                    elif type(k) in (FetchObject, FetchSubject):
                        pass
                    else:
                        self.wheres.append(column_compare(v, k, t.c))
            else:
                if c in ("object_statement_id",):
                    if type(q) == File:
                        q = self.repo.get_file_blob(q)
                    self.wheres.append(column_compare(q, "eq", t.c))
                else:
                    self.wheres.append(t.c[c]==(q.id if type(q) in (Statement,) else q))

    def apply_additonal_query(self):
        main_ids = [s.id for s in self.results]
        self.additional_stack.append((self.query, main_ids, self.target))
        while self.additional_stack:
            q, subq, target = self.additional_stack.pop()
            # q = subquery
            object_predicates = []
            subject_predicates = []
            if type(q) == dict:
                for k, v in q.items():
                    if type(k) in (MatchObject, FetchObject):
                        object_predicates.append(k.value)
                        if type(v) == dict:
                            new_subq = select([statement_table.c.object_statement_id])\
                                .where(and_(
                                    statement_table.c.subject_id.in_(subq),
                                    statement_table.c.predicate_id==k.value.id,
                                ))
                            self.additional_stack.append((v, new_subq, statement_table))
                    elif type(k) in (MatchSubject, FetchSubject):
                        subject_predicates.append(k.value)
                        if type(v) == dict:
                            if target == blob_table:
                                new_subq = select([statement_table.c.subject_id])\
                                    .where(and_(
                                        statement_table.c.object_blob_id.in_(subq),
                                        statement_table.c.object_blob_id!=None,
                                        statement_table.c.predicate_id==k.value.id,
                                    ))
                                self.additional_stack.append((v, new_subq, statement_table))
                            else:
                                new_subq = select([statement_table.c.subject_id])\
                                    .where(and_(
                                        statement_table.c.object_statement_id.in_(subq),
                                        statement_table.c.predicate_id==k.value.id,
                                    ))
                                self.additional_stack.append((v, new_subq, statement_table))
            if len(object_predicates):
                sbj = select([statement_table.c.id]).where(statement_table.c.subject_id.in_(subq))

                if not None in object_predicates:
                    pred_ids = [s.id for s in object_predicates]
                    sbj = sbj.where(statement_table.c.predicate_id.in_(pred_ids))
                self.additionals.append(sbj)

            if len(subject_predicates):
                if target == blob_table:
                    obj = select([statement_table.c.id]).where(and_(
                        statement_table.c.object_blob_id.in_(subq),
                        statement_table.c.object_blob_id!=None
                    ))
                else:
                    obj = select([statement_table.c.id]).where(statement_table.c.object_statement_id.in_(subq))

                if not None in subject_predicates:
                    pred_ids = [s.id for s in subject_predicates]
                    obj = obj.where(statement_table.c.predicate_id.in_(pred_ids))
                self.additionals.append(obj)

    def get_results(self):
        if self.target == blob_table:
            return self._get_blob_results()
        elif self.target == statement_table:
            return self._get_statement_results()

    def _get_blob_results(self):
        s = select([self.target.c.id, blob_table.c.sha256]).select_from(self.select_from)
        s = s.where(and_(*self.wheres)).distinct(self.target.c.sha256).limit(self.limit + 1)
        if self.after is not None:
            s = s.where(self.target.c.sha256 > self.after.sha256)
        s = s.order_by(self.target.c.sha256)
        #print("DBQUERY", s.compile(dialect=self.db.dialect, compile_kwargs={"literal_binds": True}))
        resultset = self.db.execute(s)
        self.results = [Blob(sha256=r_sha256, id_=r_id)
            for r_id, r_sha256 in islice(resultset, self.limit)]
        more = (resultset.rowcount > self.limit)
        return self.results, more

    def _get_statement_results(self):
        s = select([self.target.c.id, self.target.c.uuid]).select_from(self.select_from)
        s = s.where(and_(*self.wheres)).distinct(self.target.c.uuid).limit(self.limit + 1)
        if self.after is not None:
            s = s.where(self.target.c.uuid > self.after.uuid)
        s = s.order_by(self.target.c.uuid)
        #print("DBQUERY", s.compile(dialect=self.db.dialect, compile_kwargs={"literal_binds": True}))
        resultset = self.db.execute(s)
        self.results = [Statement(uuid_=r_uuid, id_=r_id)
            for r_id, r_uuid in islice(resultset, self.limit)]
        more = (resultset.rowcount > self.limit)
        return self.results, more

    def get_result_values(self):
        if self.target == blob_table:
            statements = self._get_blob_values()
        else:
            statements = self._get_statement_values()
        return statements

    def _get_blob_values(self):
        self.apply_additonal_query()
        blob_ids = [s.id for s in self.results]
        ids = []
        #[print("ADDITIONAL", a.compile(dialect=self.db.dialect, compile_kwargs={"literal_binds": True})) for a in self.additionals]

        for additional in self.additionals:
            res = self.db.execute(additional)
            ids += [i[0] for i in res.fetchall()]
        allids = set(ids)
        s, entities = self.repo.select_full_statements(statement_table)
        where = statement_table.c.id.in_(allids)
        s = s.where(where).distinct(statement_table.c.id)
        #print("ADBQUERY", s.compile(dialect=self.db.dialect, compile_kwargs={"literal_binds": True}))

        results = self.db.execute(s)
        statements = self.repo.process_result_statements(results, entities)
        #print("RETURNING", statements)
        return statements


        blob_ids = [b.id for b in self.results]

        s, entities = self.repo.select_full_statements(statement_table)

        main_alias = statement_table.alias("main")

        sub_alias = statement_table.alias("sub")
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
        self.apply_additonal_query()
        main_ids = [s.id for s in self.results]
        ids = main_ids[:]
        #[print("ADDITIONAL", a.compile(dialect=self.db.dialect, compile_kwargs={"literal_binds": True})) for a in self.additionals]

        for additional in self.additionals:
            res = self.db.execute(additional)
            ids += [i[0] for i in res.fetchall()]

        allids = set(ids)
        s, entities = self.repo.select_full_statements(self.target)
        where = self.target.c.id.in_(allids)
        s = s.where(where).distinct(statement_table.c.id)

        results = self.db.execute(s)
        statements = self.repo.process_result_statements(results, entities)
        return statements
