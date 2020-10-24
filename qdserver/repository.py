from collections import defaultdict
from itertools import islice

from sqlalchemy import and_, or_
from sqlalchemy.sql import select
from sqlalchemy.dialects.postgresql import insert as pg_insert

from queryduck.types import Blob, Statement, File

from .models import statement_table, blob_table, file_table, volume_table
from .utility import (
    EntitySet,
    process_db_row,
    column_compare,
    prepare_for_db,
)


class PGRepository:
    def __init__(self, db):
        """Make relevant services available."""
        self.db = db
        self.statement_map = {}
        self.blob_map = {}

    def unique_add(self, new_value):
        if type(new_value) == Statement:
            if new_value.handle in self.statement_map:
                value = self.statement_map[new_value.handle]
                if new_value.triple is not None and value.triple is None:
                    value.triple = new_value.triple
                if new_value.id is not None and value.id is None:
                    value.id = new_value.id
                if new_value.saved and not value.saved:
                    value.saved = True
            else:
                value = new_value
                self.statement_map[value.handle] = value
        elif type(new_value) == Blob:
            if new_value.handle in self.blob_map:
                value = self.blob_map[new_value.handle]
                if new_value.id is not None and value.id is None:
                    value.id = new_value.id
            else:
                value = new_value
                self.blob_map[value.handle] = value
        else:
            # simple scalar value, doesn't need to be uniqueified
            value = new_value

        return value

    def create_statement(self, handle=None, **kwargs):
        """Create a Statement with specified values. None values are changed to be self referential."""
        if handle is None:
            handle = uuid4()
        insert = statement_table.insert().values(handle=handle)
        (insert_id,) = self.db.execute(insert).inserted_primary_key
        values = {k: (insert_id if v is None else v) for k, v in kwargs.items()}
        where = statement_table.c.id == insert_id
        update = statement_table.update().where(where).values(values)
        self.db.execute(update)
        return insert_id

    def create_statements(self, statements):
        all_values = set(
            [v for statement in statements for v in (statement,) + statement.triple]
        )
        self.fill_ids(all_values, allow_create=True)

        # convert the supplied rows into values to be upserted
        insert_values = []
        all_column_names = set()
        for statement in statements:
            statement = self.unique_add(statement)
            if statement.saved:
                print("Exists!", statement)
                continue
            value, column_name = prepare_for_db(statement.triple[2])
            insert_value = {
                "handle": statement.handle,
                "subject_id": statement.triple[0].id,
                "predicate_id": statement.triple[1].id,
                column_name: value,
            }
            insert_values.append(insert_value)
            all_column_names |= insert_value.keys()

        # ensure every row has a value for every column, even if it's None
        for insert_value in insert_values:
            for column_name in all_column_names:
                if column_name not in insert_value:
                    insert_value[column_name] = None

        # actually upsert the rows
        if insert_values:
            ins = pg_insert(statement_table).values(insert_values)
            on_conflict_set = {
                cn: getattr(ins.excluded, cn)
                for cn in all_column_names
                if cn != "handle"
            }
            upd = ins.on_conflict_do_update(
                index_elements=["handle"], set_=on_conflict_set
            )
            self.db.execute(upd)

        return statements

    def get_all_statements(self):
        s, entities = self.select_full_statements(statement_table, blob_files=False)
        s = s.order_by(statement_table.c.handle)
        results = self.db.execute(s)
        quads = self.process_result_quads(results, entities)
        return quads

    def get_statements_by_handles(self, handles):
        s, entities = self.select_full_statements(statement_table, blob_files=False)
        s = s.where(statement_table.c.handle.in_(handles))
        results = self.db.execute(s)
        statements = self.process_result_statements(results, entities)
        return statements

    def get_blobs_by_sums(self, sums):
        s, entities = self.select_full_statements(statement_table, blob_files=False)
        s = select([blob_table]).where(blob_table.c.handle.in_(sums))
        results = self.db.execute(s)
        blobs = [Blob(handle=r["handle"], id_=r["id"]) for r in results]
        return blobs

    @staticmethod
    def select_full_statements(main, blob_files=True):
        """Construct a select() to fetch all necessary Statement fields."""
        su = statement_table.alias()
        pr = statement_table.alias()
        ob = statement_table.alias()
        entities = {
            "main": main,
            "s": ob,
            "su": su,
            "pr": pr,
            "ob": ob,
            "blob": blob_table,
        }

        # If you're reading this and have suggestions on a cleaner style that
        # doesn't exceed 80 columns, please let me know!
        select_from = (
            main.join(su, su.c.id == main.c.subject_id, isouter=True)
            .join(pr, pr.c.id == main.c.predicate_id, isouter=True)
            .join(ob, ob.c.id == main.c.object_statement_id, isouter=True)
            .join(blob_table, blob_table.c.id == main.c.object_blob_id, isouter=True)
        )
        columns = [
            main,
            su.c.handle,
            pr.c.handle,
            ob.c.handle,
            blob_table.c.handle,
        ]

        s = select(columns).select_from(select_from)
        return s, entities

    @staticmethod
    def process_result_rows(results, entities):
        processed = []
        for row in results:
            statement = self.unique_add(
                Statement(handle=row[entities["main"].c.handle])
            )
            statement.triple = (
                self.unique_add(Statement(handle=row[entities["su"].c.handle])),
                self.unique_add(Statement(handle=row[entities["pr"].c.handle])),
                self.unique_add(process_db_row(row, entities["main"].c, entities)[0]),
            )
        return processed

    @staticmethod
    def process_result_quads(results, entities):
        processed = []
        for row in results:
            elements = (
                Statement(handle=row[entities["main"].c.handle]),
                Statement(handle=row[entities["su"].c.handle]),
                Statement(handle=row[entities["pr"].c.handle]),
                process_db_row(row, entities["main"].c, entities)[0],
            )
            processed.append(elements)
        return processed

    def process_result_statements(self, results, entities):
        processed = []
        for row in results:
            statement = Statement(
                handle=row[entities["main"].c.handle], id_=row[entities["main"].c.id]
            )
            if row[entities["su"].c.handle]:
                statement.triple = (
                    Statement(handle=row[entities["su"].c.handle]),
                    Statement(handle=row[entities["pr"].c.handle]),
                    process_db_row(row, entities["main"].c, entities)[0],
                )
                statement.saved = True
            statement = self.unique_add(statement)
            processed.append(statement)
        return processed

    def get_statement_id_map(self, statements):
        print(statements)
        handles = [s.handle for s in statements]
        sel = select([statement_table.c.id, statement_table.c.handle]).where(
            statement_table.c.handle.in_(handles)
        )
        result = self.db.execute(sel)
        id_map = {u: i for i, u in result.fetchall()}
        return id_map

    def get_blob_id_map(self, blobs):
        handles = [b.handle for b in blobs]
        sel = select([blob_table.c.id, blob_table.c.handle]).where(
            blob_table.c.handle.in_(handles)
        )
        result = self.db.execute(sel)
        id_map = {u: i for i, u in result.fetchall()}
        return id_map

    def fill_ids(self, values, allow_create=False):
        statements = list(filter(lambda v: type(v) == Statement, values))
        self.fill_statement_ids(statements, allow_create)
        blobs = list(filter(lambda v: type(v) == Blob, values))
        self.fill_blob_ids(blobs, allow_create)

    def fill_statement_ids(self, statements, allow_create=False):
        id_map = self.get_statement_id_map(statements)
        missing = []
        for s in statements:
            if s.handle in id_map:
                s.id = id_map[s.handle]
            else:
                missing.append(s)

        if not missing:
            return

        if allow_create:
            ins = statement_table.insert().values(
                [{"handle": s.handle} for s in missing]
            )
            self.db.execute(ins)
            id_map = self.get_statement_id_map(missing)
            for s in missing:
                s.id = id_map[s.handle]
        else:
            for s in missing:
                s.id = -1

    def fill_blob_ids(self, blobs, allow_create=False):
        id_map = self.get_blob_id_map(blobs)
        missing = []
        for b in blobs:
            if b.handle in id_map:
                b.id = id_map[b.handle]
            else:
                missing.append(b)

        if not missing:
            return

        if allow_create:
            ins = blob_table.insert().values([{"handle": b.handle} for b in missing])
            self.db.execute(ins)
            id_map = self.get_blob_id_map(missing)
            for b in missing:
                b.id = id_map[b.handle]
        else:
            for b in missing:
                b.id = -1

    def get_target_table(self, target_name):
        if target_name == "blob":
            target = blob_table
        elif target_name == "statement":
            target = statement_table
        return target

    def get_blob_files(self, blobs):
        if not blobs:
            return {}

        blobs_by_id = {b.id: b for b in blobs}

        select_from = file_table.join(
            volume_table, volume_table.c.id == file_table.c.volume_id, isouter=True
        )

        sel = (
            select(
                [
                    file_table.c.blob_id,
                    file_table.c.path,
                    volume_table.c.reference,
                ]
            )
            .select_from(select_from)
            .where(file_table.c.blob_id.in_(blobs_by_id.keys()))
        )
        result = self.db.execute(sel)

        files = defaultdict(list)
        for row in result.fetchall():
            key = blobs_by_id[row[file_table.c.blob_id]]
            f = File(
                volume=row[volume_table.c.reference],
                path=row[file_table.c.path],
            )
            files[key].append(f)
        return files

    def get_file_blob(self, file_):
        select_from = file_table.join(
            volume_table, volume_table.c.id == file_table.c.volume_id, isouter=True
        ).join(blob_table, blob_table.c.id == file_table.c.blob_id, isouter=True)

        sel = (
            select(
                [
                    blob_table.c.id,
                    blob_table.c.handle,
                ]
            )
            .select_from(select_from)
            .where(
                and_(
                    volume_table.c.reference == file_.volume,
                    file_table.c.path == file_.path,
                )
            )
        )
        res = self.db.execute(sel)
        id_, handle = res.fetchone()
        blob = Blob(handle=handle, id_=id_)
        return blob

    def _show_db_query(self, db_query):
        print("------ START DB STATEMENT ------")
        try:
            compiled = db_query.compile(
                dialect=self.db.dialect, compile_kwargs={"literal_binds": True}
            )
            print(compiled)
        except:
            compiled = db_query.compile(dialect=self.db.dialect)
            print(compiled)
            print(compiled.params)
        print("------ END DB STATEMENT ------")

    def _query_to_select(self, query):
        self.fill_ids(query.seen_values)
        table = blob_table if query.target == Blob else statement_table
        es = EntitySet({"main": table.alias("main")})

        for k, v in query.joins.items():
            if k == "main":
                continue
            es.register_entity(k, v)

        wheres = []
        for f in query.filters:
            lhs = es.get_alias(f.lhs.key)
            wheres.append(lhs.c.object_statement_id == f.rhs.id)

        inner = select(
            [es.aliases["main"].c.id, es.aliases["main"].c.handle]
        ).select_from(es.fromclause)
        inner = inner.where(and_(*wheres))
        outer = inner.limit(query.limit + 1)
        return outer

    def get_results(self, query):
        db_select = self._query_to_select(query)
        self._show_db_query(db_select)
        resultset = self.db.execute(db_select)
        results = [
            query.target(handle=row[1], id_=row[0])
            for row in islice(resultset, query.limit)
        ]
        more = resultset.rowcount > query.limit
        return results, more

    def get_additional_values(self, query, results):
        main_ids = [s.id for s in results]
        ids = main_ids[:]
        table = blob_table if query.target == Blob else statement_table

        for f in query.fetches:
            es = EntitySet({"main": table.alias("main")})
            for k, v in query.joins.items():
                if k == "main":
                    continue
                es.register_entity(k, v)

            alias = es.get_alias(f.operand.key)

            sel = (
                select([alias.c.id])
                .select_from(es.fromclause)
                .where(es.aliases['main'].c.id.in_(main_ids))
            )
            print("SEL", sel)
            res = self.db.execute(sel)
            ids += [i[0] for i in res.fetchall()]

        allids = set(ids)
        table = blob_table if query.target == Blob else statement_table
        s, entities = self.select_full_statements(table)
        where = table.c.id.in_(allids)
        s = s.where(where).distinct(table.c.id)

        results = self.db.execute(s)
        statements = self.process_result_statements(results, entities)
        return statements
