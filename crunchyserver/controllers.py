from datetime import datetime as dt
from uuid import uuid4, UUID

from pyramid.view import view_config
from sqlalchemy import and_, or_
from sqlalchemy.sql import select
from sqlalchemy.dialects.postgresql import insert as pg_insert

from crunchylib.types import Blob, Statement, serialize, deserialize, process_db_row, column_compare, prepare_for_db
from crunchylib.utility import transform_doc

from .models import statement_table, blob_table, file_table, volume_table


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
            else:
                value = new_value
                self.statement_map[value.uuid] = value
        elif type(new_value) == Blob:
            if new_value.sha256 in self.blob_map:
                value = self.blob_map[new_value.sha256]
                if new_value.id is not None and value.id is None:
                    value.id = new_value.id
            else:
                value = new_value
                self.blob_map[blob.sha256] = blob

        return value

    def unique_deserialize(self, ref):
        """Ensures there is only ever one instance of the same Statement present"""
        s = deserialize(ref)
        if type(s) == Statement:
            self.unique_add(s)
            return self.statement_map[s.uuid]
        elif type(s) == Blob:
            if s.sha256 not in self.blob_map:
                self.blob_map[s.sha256] = s
            elif s.volume and not self.blob_map[s.sha256].volume:
                self.blob_map[s.sha256].volume = s.volume
                self.blob_map[s.sha256].path = s.path
            return self.blob_map[s.sha256]
        else:
            return s


    ### View methods ###

    @view_config(route_name='create_statements', renderer='json')
    def create_statements(self):
        rows = self._create_statements(self.request.json_body)

        result = {
            'statements': [],
        }

        for row in rows:
            result['statements'].append([serialize(e) for e in row])

        return result

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

    @view_config(route_name='get_statements', renderer='json')
    def get_statements(self):
        quads = self._get_all_statements()

        result = {
            'statements': [],
        }

        for q in quads:
            result['statements'].append([serialize(e) for e in q])

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

    ### Worker methods ###

    def _create_statement(self, uuid_=None, **kwargs):
        """Create a Statement with specified values. None values are changed to be self referential."""
        if uuid_ is None:
            uuid_ = uuid4()
        insert = self.t.insert().values(uuid=uuid_)
        (insert_id,) = self.db.execute(insert).inserted_primary_key
        values = {k: (insert_id if v is None else v) for k, v in kwargs.items()}
        where = self.t.c.id==insert_id
        update = self.t.update().where(where).values(values)
        self.db.execute(update)
        return insert_id

    def _create_statements(self, serialized_rows):

        # create initial Statements without values, but with final UUID's
        statements = []
        for sr in serialized_rows:
            if sr[0] is None:
                statement = Statement(uuid_=uuid4())
                self.unique_add(statement)
            else:
                statement = self.unique_deserialize(sr[0])
            statements.append(statement)

        # create all Statements without values, ignoring duplicates
        # (duplicates are OK if they are identical, we'll check this later)
        values = [{'uuid': s.uuid} for s in statements]
        stmt = pg_insert(self.t).values(values)
        stmt = stmt.on_conflict_do_nothing(index_elements=['uuid'])
        self.db.execute(stmt)

        # fill Statement values and create set of all UUID's involved
        all_uuids = set()
        all_blob_sums = set()
        rows = []
        for idx, sr in enumerate(serialized_rows):
            statement = statements[idx]
            row = [statement]
            all_uuids.add(statement.uuid)
            for ser_v in sr[1:]:
                v = self.unique_deserialize(ser_v) if type(ser_v) == str else statements[ser_v]
                row.append(v)
                if type(v) == Statement:
                    all_uuids.add(v.uuid)
                elif type(v) == Blob:
                    all_blob_sums.add(v.sha256)
            rows.append(row)

        all_statements = self._get_statements_by_uuids(all_uuids)
        all_statements_by_uuid = {s.uuid: s for s in all_statements}

        all_blobs = self._get_blobs_by_sums(all_blob_sums)
        for b in all_blobs:
            self.unique_add(b)

        # convert the supplied rows into values to be upserted
        insert_values = []
        all_column_names = set()
        for statement, s, p, o in rows:
            self.unique_add(all_statements_by_uuid[statement.uuid])
            if statement.triple is not None:
                print("Exists!", statement)
                continue
            value, column_name = prepare_for_db(o)
            insert_value = {
                'uuid': statement.uuid,
                'subject_id': s.id,
                'predicate_id': p.id,
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
            ins = pg_insert(self.t).values(insert_values)
            on_conflict_set = {cn: getattr(ins.excluded, cn)
                for cn in all_column_names if cn != 'uuid'}
            upd = ins.on_conflict_do_update(index_elements=['uuid'], set_=on_conflict_set)
            self.db.execute(upd)

        return rows

    def _get_all_statements(self):
        s, entities = self._select_full_statements(self.t, blob_files=False)
        s = s.order_by(self.t.c.uuid)
        results = self.db.execute(s)
        quads = self._process_result_quads(results, entities)
        return quads

    def _get_statements_by_uuids(self, uuids):
        s, entities = self._select_full_statements(self.t, blob_files=False)
        s = s.where(self.t.c.uuid.in_(uuids))
        results = self.db.execute(s)
        statements = self._process_result_statements(results, entities)
        return statements

    def _get_statement_values(self, statements):
        statement_ids = [s.id for s in statements]

        s, entities = self._select_full_statements(self.t)

        sub_alias = self.t.alias()
        sub_from = self.t.join(sub_alias, sub_alias.c.id==self.t.c.subject_id)
        sub = select([self.t.c.id]).select_from(sub_from)
        sub = sub.where(sub_alias.c.subject_id.in_(statement_ids))

        obj_alias = self.t.alias()
        obj_from = self.t.join(obj_alias,
            obj_alias.c.object_statement_id==self.t.c.subject_id)
        obj = select([self.t.c.id]).select_from(obj_from)
        obj = obj.where(obj_alias.c.subject_id.in_(statement_ids))

        where = or_(
            self.t.c.subject_id.in_(statement_ids),
            self.t.c.id.in_(statement_ids),
            self.t.c.id.in_(sub),
            self.t.c.id.in_(obj),
        )
        s = s.where(where).distinct(self.t.c.id)

        statement_dict = {}
        results = self.db.execute(s)
        for r in self._process_result_quads(results, entities):
            ser = [serialize(e) for e in r]
            statement_dict[ser[0]] = ser[1:]
        return statement_dict

    def _query_statements(self, query):
        query = self._prepare_query(query)

        select_from = self.t
        wheres = []
        stack = [(query, self.t, self.t.c.id)]
        while stack:
            q, t, i = stack.pop()
            if type(q) == dict:
                for k, v in q.items():
                    if type(k) == Statement:
                        a = self.t.alias()
                        select_from = select_from.join(a,
                            and_(a.c.subject_id==i,
                                a.c.predicate_id==k.id),
                            isouter=True)
                        stack.append((v, a, a.c.object_statement_id))
                    else:
                        wheres.append(column_compare(v, k, t.c))
            else:
                wheres.append(column_compare(q, 'eq', t.c))

        s = select([self.t.c.id, self.t.c.uuid]).select_from(select_from)
        s = s.where(and_(*wheres)).distinct(self.t.c.id).limit(100)
        results = self.db.execute(s)
        statements = [Statement(uuid_=r_uuid, id_=r_id)
            for r_id, r_uuid in results]
        return statements

    def _get_blobs_by_sums(self, sums):
        s, entities = self._select_full_statements(self.t, blob_files=False)
        s = select([blob_table]).where(blob_table.c.sha256.in_(sums))
        results = self.db.execute(s)
        blobs = [Blob(sha256=r['sha256'], id_=r['id']) for r in results]
        return blobs

    ### Helper methods ###

    @staticmethod
    def _select_full_statements(main, blob_files=True):
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
            'volume': volume_table,
            'file': file_table,
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

        if blob_files:
            select_from = select_from.join(file_table,
                    file_table.c.blob_id==main.c.object_blob_id, isouter=True)\
                .join(volume_table,
                    volume_table.c.id==file_table.c.volume_id, isouter=True)

            columns += [
                file_table.c.path,
                volume_table.c.reference,
            ]

        s = select(columns).select_from(select_from)
        return s, entities

    @staticmethod
    def _process_result_quads(results, entities):
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

    def _process_result_statements(self, results, entities):
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
            statement = self.unique_add(statement)
            processed.append(statement)
        return processed

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
            row = result.fetchone()
            statement.id = row['id'] if row else None

    def _get_statement_id_map(self, statements):
        uuids = [s.uuid for s in statements]
        sel = select([self.t.c.id, self.t.c.uuid]).where(self.t.c.uuid.in_(uuids))
        result = self.db.execute(sel)
        id_map = {u: i for i, u in result.fetchall()}
        return id_map

    def _bulk_fill_ids(self, values, allow_create=False):
        statements = [v for v in values if type(v) == Statement]
        id_map = self._get_statement_id_map(statements)

        missing = []
        for s in statements:
            if s.uuid in id_map:
                s.id = id_map[s.uuid]
            else:
                missing.append(s)

        if missing and allow_create:
            ins = self.t.insert().values([{'uuid': s.uuid} for s in missing])
            self.db.execute(ins)
            id_map = self._get_statement_id_map(missing)
            for s in missing:
                s.id = id_map[s.uuid]
