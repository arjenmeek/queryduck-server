from collections import defaultdict

from sqlalchemy import and_, or_
from sqlalchemy.sql import select
from sqlalchemy.dialects.postgresql import insert as pg_insert

from queryduck.types import Blob, Statement, File, process_db_row, column_compare, prepare_for_db

from .models import statement_table, blob_table, file_table, volume_table


class PGRepository:

    def __init__(self, db):
        """Make relevant services available."""
        self.db = db
        self.statement_map = {}
        self.blob_map = {}

    def unique_add(self, new_value):
        if type(new_value) == Statement:
            if new_value.uuid in self.statement_map:
                value = self.statement_map[new_value.uuid]
                if new_value.triple is not None and value.triple is None:
                    value.triple = new_value.triple
                if new_value.id is not None and value.id is None:
                    value.id = new_value.id
                if new_value.saved and not value.saved:
                    value.saved = True
            else:
                value = new_value
                self.statement_map[value.uuid] = value
        elif type(new_value) == Blob:
            if new_value.sha256 in self.blob_map:
                value = self.blob_map[new_value.sha256]
                if new_value.id is not None and value.id is None:
                    value.id = new_value.id
                if new_value.volume and not value.volume:
                    value.volume = new_value.volume
                    value.path = new_value.path
            else:
                value = new_value
                self.blob_map[value.sha256] = value
        else:
            # simple scalar value, doesn't need to be uniqueified
            value = new_value

        return value

    def create_statement(self, uuid_=None, **kwargs):
        """Create a Statement with specified values. None values are changed to be self referential."""
        if uuid_ is None:
            uuid_ = uuid4()
        insert = statement_table.insert().values(uuid=uuid_)
        (insert_id,) = self.db.execute(insert).inserted_primary_key
        values = {k: (insert_id if v is None else v) for k, v in kwargs.items()}
        where = statement_table.c.id==insert_id
        update = statement_table.update().where(where).values(values)
        self.db.execute(update)
        return insert_id

    def create_statements(self, statements):
        all_values = set([v for statement in statements
            for v in (statement,) + statement.triple])

        all_uuids = set([v.uuid for v in all_values if type(v) == Statement])

        all_blob_sums = set([v.sha256 for v in all_values if type(v) == Blob])
        all_blobs = self.get_blobs_by_sums(all_blob_sums)
        for b in all_blobs:
            self.unique_add(b)

        # create all Statements without values, ignoring duplicates
        # (duplicates are OK if they are identical, we'll check this later)
        values = [{'uuid': u} for u in all_uuids]
        stmt = pg_insert(statement_table).values(values)
        stmt = stmt.on_conflict_do_nothing(index_elements=['uuid'])
        self.db.execute(stmt)

        # retrieve all statements as they are now
        all_statements = self.get_statements_by_uuids(all_uuids)
        all_statements_by_uuid = {s.uuid: s for s in all_statements}

        # convert the supplied rows into values to be upserted
        insert_values = []
        all_column_names = set()
        for statement in statements:
            self.unique_add(all_statements_by_uuid[statement.uuid])
            if statement.saved:
                print("Exists!", statement)
                continue
            value, column_name = prepare_for_db(statement.triple[2])
            insert_value = {
                'uuid': statement.uuid,
                'subject_id': statement.triple[0].id,
                'predicate_id': statement.triple[1].id,
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
            on_conflict_set = {cn: getattr(ins.excluded, cn)
                for cn in all_column_names if cn != 'uuid'}
            upd = ins.on_conflict_do_update(index_elements=['uuid'], set_=on_conflict_set)
            self.db.execute(upd)

        return statements

    def get_all_statements(self):
        s, entities = self.select_full_statements(statement_table, blob_files=False)
        s = s.order_by(statement_table.c.uuid)
        results = self.db.execute(s)
        quads = self.process_result_quads(results, entities)
        return quads

    def get_statements_by_uuids(self, uuids):
        s, entities = self.select_full_statements(statement_table, blob_files=False)
        s = s.where(statement_table.c.uuid.in_(uuids))
        results = self.db.execute(s)
        statements = self.process_result_statements(results, entities)
        return statements

    def get_blobs_by_sums(self, sums):
        s, entities = self.select_full_statements(statement_table, blob_files=False)
        s = select([blob_table]).where(blob_table.c.sha256.in_(sums))
        results = self.db.execute(s)
        blobs = [Blob(sha256=r['sha256'], id_=r['id']) for r in results]
        return blobs

    @staticmethod
    def select_full_statements(main, blob_files=True):
        """Construct a select() to fetch all necessary Statement fields."""
        su = statement_table.alias()
        pr = statement_table.alias()
        ob = statement_table.alias()
        entities = {
            'main': main,
            's': ob,
            'su': su,
            'pr': pr,
            'ob': ob,
            'blob': blob_table,
        }

        # If you're reading this and have suggestions on a cleaner style that
        # doesn't exceed 80 columns, please let me know!
        select_from = main\
            .join(su, su.c.id==main.c.subject_id, isouter=True)\
            .join(pr, pr.c.id==main.c.predicate_id, isouter=True)\
            .join(ob, ob.c.id==main.c.object_statement_id, isouter=True)\
            .join(blob_table,
                blob_table.c.id==main.c.object_blob_id, isouter=True)\

        columns = [
            main,
            su.c.uuid,
            pr.c.uuid,
            ob.c.uuid,
            blob_table.c.sha256,
        ]

        s = select(columns).select_from(select_from)
        return s, entities

    @staticmethod
    def process_result_rows(results, entities):
        processed = []
        for row in results:
            statement = self.unique_add(Statement(uuid_=row[entities['main'].c.uuid]))
            statement.triple = (
                self.unique_add(Statement(uuid_=row[entities['su'].c.uuid])),
                self.unique_add(Statement(uuid_=row[entities['pr'].c.uuid])),
                self.unique_add(process_db_row(row, entities['main'].c, entities)[0]),
            )
        return processed

    @staticmethod
    def process_result_quads(results, entities):
        processed = []
        for row in results:
            elements = (
                Statement(uuid_=row[entities['main'].c.uuid]),
                Statement(uuid_=row[entities['su'].c.uuid]),
                Statement(uuid_=row[entities['pr'].c.uuid]),
                process_db_row(row, entities['main'].c, entities)[0],
            )
            processed.append(elements)
        return processed

    def process_result_statements(self, results, entities):
        processed = []
        for row in results:
            statement = Statement(
                uuid_=row[entities['main'].c.uuid],
                id_=row[entities['main'].c.id])
            if row[entities['su'].c.uuid]:
                statement.triple = (
                    Statement(uuid_=row[entities['su'].c.uuid]),
                    Statement(uuid_=row[entities['pr'].c.uuid]),
                    process_db_row(row, entities['main'].c, entities)[0],
                )
                statement.saved = True
            statement = self.unique_add(statement)
            processed.append(statement)
        return processed

    def fill_ids(self, statements):
        # TODO: Fetch all in one query
        if type(statements) != list:
            statements = [statements]
        for statement in statements:
            if type(statement) == list:
                self.fill_ids(statement)
            if type(statement) not in (Statement, Blob):
                continue
            if type(statement) == Statement:
                s = select([statement_table.c.id], limit=1).where(statement_table.c.uuid==statement.uuid)
                result = self.db.execute(s)
            elif type(statement) == Blob:
                s = select([blob_table.c.id], limit=1).where(blob_table.c.sha256==statement.sha256)
                result = self.db.execute(s)
            row = result.fetchone()
            statement.id = row['id'] if row else None

    def get_statement_id_map(self, statements):
        uuids = [s.uuid for s in statements]
        sel = select([statement_table.c.id, statement_table.c.uuid]).where(statement_table.c.uuid.in_(uuids))
        result = self.db.execute(sel)
        id_map = {u: i for i, u in result.fetchall()}
        return id_map

    def bulk_fill_ids(self, values, allow_create=False):
        statements = [v for v in values if type(v) == Statement]
        id_map = self.get_statement_id_map(statements)

        missing = []
        for s in statements:
            if s.uuid in id_map:
                s.id = id_map[s.uuid]
            else:
                missing.append(s)

        if missing and allow_create:
            ins = statement_table.insert().values([{'uuid': s.uuid} for s in missing])
            self.db.execute(ins)
            id_map = self.get_statement_id_map(missing)
            for s in missing:
                s.id = id_map[s.uuid]

    def get_target_table(self, target_name):
        if target_name == 'blob':
            target = blob_table
        elif target_name == 'statement':
            target = statement_table
        return target

    def get_blob_files(self, blobs):
        if not blobs:
            return {}

        blobs_by_id = {b.id: b for b in blobs}

        select_from = file_table.join(volume_table,
            volume_table.c.id==file_table.c.volume_id, isouter=True)

        sel = select([
            file_table.c.blob_id,
            file_table.c.path,
            volume_table.c.reference,
        ]).select_from(select_from).where(file_table.c.blob_id.in_(blobs_by_id.keys()))
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
