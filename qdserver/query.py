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
from .utility import (
    process_db_row,
    column_compare,
    final_column_compare,
    prepare_for_db,
)


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
        self.final_wheres = []
        self.extra_columns = []
        self.results = None
        self.stack = []
        self.additional_stack = []
        self.additionals = []
        self.prefer_by = []
        self.order_by = []
        self.apply_query(query)
        self.limit = 5000

    def _apply_join(self, key, rhs_column, v, t):
        lhs_name, id_name = key.get_join_columns(v, t)
        a = statement_table.alias()
        self.select_from = self.select_from.join(
            a,
            and_(a.c[lhs_name] == rhs_column, a.c.predicate_id == key.value.id),
            isouter=True,
        )
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
                        if k == "sort":
                            self.order_by.append(t.c["object_" + v].label(None))
                        elif k == "prefer+":
                            self.prefer_by.append(t.c["object_" + v].desc())
                        else:
                            if k.endswith("."):
                                (
                                    column_label,
                                    op_method,
                                    db_value,
                                ) = final_column_compare(v, k[:-1], t.c)
                                self.final_wheres.append(
                                    (column_label, op_method, db_value)
                                )
                                self.extra_columns.append(column_label)
                            else:
                                self.wheres.append(column_compare(v, k, t.c))
            else:
                if c in ("object_statement_id",):
                    if type(q) == File:
                        q = self.repo.get_file_blob(q)
                    self.wheres.append(column_compare(q, "eq", t.c))
                else:
                    self.wheres.append(
                        t.c[c] == (q.id if type(q) in (Statement,) else q)
                    )

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
                            new_subq = select(
                                [statement_table.c.object_statement_id]
                            ).where(
                                and_(
                                    statement_table.c.subject_id.in_(subq),
                                    statement_table.c.predicate_id == k.value.id,
                                )
                            )
                            self.additional_stack.append((v, new_subq, statement_table))
                    elif type(k) in (MatchSubject, FetchSubject):
                        subject_predicates.append(k.value)
                        if type(v) == dict:
                            if target == blob_table:
                                new_subq = select([statement_table.c.subject_id]).where(
                                    and_(
                                        statement_table.c.object_blob_id.in_(subq),
                                        statement_table.c.object_blob_id != None,
                                        statement_table.c.predicate_id == k.value.id,
                                    )
                                )
                                self.additional_stack.append(
                                    (v, new_subq, statement_table)
                                )
                            else:
                                new_subq = select([statement_table.c.subject_id]).where(
                                    and_(
                                        statement_table.c.object_statement_id.in_(subq),
                                        statement_table.c.predicate_id == k.value.id,
                                    )
                                )
                                self.additional_stack.append(
                                    (v, new_subq, statement_table)
                                )
            if len(object_predicates):
                sbj = select([statement_table.c.id]).where(
                    statement_table.c.subject_id.in_(subq)
                )

                if not None in object_predicates:
                    pred_ids = [s.id for s in object_predicates]
                    sbj = sbj.where(statement_table.c.predicate_id.in_(pred_ids))
                self.additionals.append(sbj)

            if len(subject_predicates):
                if target == blob_table:
                    obj = select([statement_table.c.id]).where(
                        and_(
                            statement_table.c.object_blob_id.in_(subq),
                            statement_table.c.object_blob_id != None,
                        )
                    )
                else:
                    obj = select([statement_table.c.id]).where(
                        statement_table.c.object_statement_id.in_(subq)
                    )

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
        s = select([self.target.c.id, blob_table.c.sha256]).select_from(
            self.select_from
        )
        s = (
            s.where(and_(*self.wheres))
            .distinct(self.target.c.sha256)
            .limit(self.limit + 1)
        )
        if self.after is not None:
            s = s.where(self.target.c.sha256 > self.after.sha256)
        s = s.order_by(self.target.c.sha256)
        # print("DBQUERY", s.compile(dialect=self.db.dialect, compile_kwargs={"literal_binds": True}))
        resultset = self.db.execute(s)
        self.results = [
            Blob(sha256=r_sha256, id_=r_id)
            for r_id, r_sha256 in islice(resultset, self.limit)
        ]
        more = resultset.rowcount > self.limit
        return self.results, more

    def _get_statement_results(self):
        sub = select(
            [self.target.c.id, self.target.c.uuid] + self.order_by + self.extra_columns
        ).select_from(self.select_from)
        sub = (
            sub.where(and_(*self.wheres))
            .distinct(self.target.c.uuid)
            .order_by(self.target.c.uuid, *self.prefer_by)
        )
        if self.after is not None:
            sub = sub.where(self.target.c.uuid > self.after.uuid)
        #            .limit(self.limit + 1)
        sub = sub.alias("mysubquery")
        if self.order_by or self.final_wheres:
            s = select([sub]).select_from(sub)
            wheres = []
            for column_label, op_method, db_value in self.final_wheres:
                column = sub.c[column_label.name]
                wheres.append(getattr(column, op_method)(db_value))
            if wheres:
                s = s.where(and_(*wheres))
            if self.order_by is None:
                s = s.order_by(self.target.c.uuid)
            else:
                order_by = [sub.c[e.name] for e in self.order_by]
                s = s.order_by(*order_by)
        else:
            s = sub
            s = s.order_by(self.target.c.uuid)
        print(
            "DBQUERY",
            s.compile(dialect=self.db.dialect, compile_kwargs={"literal_binds": True}),
        )
        resultset = self.db.execute(s)
        self.results = [
            Statement(uuid_=row[1], id_=row[0]) for row in islice(resultset, self.limit)
        ]
        more = resultset.rowcount > self.limit
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
        # [print("ADDITIONAL", a.compile(dialect=self.db.dialect, compile_kwargs={"literal_binds": True})) for a in self.additionals]

        for additional in self.additionals:
            res = self.db.execute(additional)
            ids += [i[0] for i in res.fetchall()]
        allids = set(ids)
        s, entities = self.repo.select_full_statements(statement_table)
        where = statement_table.c.id.in_(allids)
        s = s.where(where).distinct(statement_table.c.id)
        # print("ADBQUERY", s.compile(dialect=self.db.dialect, compile_kwargs={"literal_binds": True}))

        results = self.db.execute(s)
        statements = self.repo.process_result_statements(results, entities)
        # print("RETURNING", statements)
        return statements

    def _get_statement_values(self):
        self.apply_additonal_query()
        main_ids = [s.id for s in self.results]
        ids = main_ids[:]
        # [print("ADDITIONAL", a.compile(dialect=self.db.dialect, compile_kwargs={"literal_binds": True})) for a in self.additionals]

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
