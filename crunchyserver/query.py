from collections import defaultdict
from uuid import UUID

from sqlalchemy import and_
from sqlalchemy.sql import select
from sqlalchemy.sql.expression import and_
from sqlalchemy.orm import aliased

from crunchylib.exceptions import GeneralError
from crunchylib.utility import deserialize_value, get_value_type

from .models import Statement, Blob


class StatementQuery(object):

    def __init__(self, db, statements):
        self.db = db
        self.statements = statements
        self.aliases = {'main': Statement}
        self.multiple_entities = False
        self.q = self.db.query(self.aliases['main'])

    def _parse_reference(self, reference, object_type=None):
        if reference.startswith('column:'):
            dummy, column_reference = reference.split(':', 2)
            alias_name, attribute_name = column_reference.split('.')
            if attribute_name == 'object' and object_type is not None:
                attribute_name = 'object_{}'.format(object_type)
            if object_type == 'statement':
                attribute_name += '_id'
            if not alias_name in self.aliases:
                raise GeneralError("Unknown alias name: {}".format(alias_name))
            value = getattr(self.aliases[alias_name], attribute_name)
        elif ':' in reference:
            value = deserialize_value(reference)
            value = self.statements.resolve_reference(value)
        else:
            raise GeneralError("Invalid reference: {}".format(reference))
        return value

    def apply_filter(self, lhs_str, op_str, rhs_str=None):
        rhs_type = None
        if rhs_str is None:
            rhs = None
        else:
            rhs = self._parse_reference(rhs_str)
            if ':' in rhs_str:
                rhs_type = get_value_type(rhs)
        lhs = self._parse_reference(lhs_str, object_type=rhs_type)

        if op_str == 'eq':
            print('filter: {} == {}'.format(lhs, rhs))
            if isinstance(rhs, Statement):
                self.q = self.q.filter(lhs==rhs.id)
            else:
                self.q = self.q.filter(lhs==rhs)
        else:
            raise GeneralError("Unknown filter operation: {}".format(op_str))

    def apply_join(self, name, lhs_str, op_str, rhs_str=None):
        self.multiple_entities = True
        self.aliases[name] = aliased(Statement)
        lhs = self._parse_reference(lhs_str, 'statement')
        rhs = self._parse_reference(rhs_str, 'statement')
        print("LHS", lhs)
        print("RHS", rhs)
        #self.q = self.q.join(self.aliases[name], and_(self.aliases[name].subject==self.aliases['main'].id, lhs==rhs), isouter=True).add_entity(self.aliases[name])
        self.q = self.q.join(self.aliases[name], and_(self.aliases[name].subject_id==self.aliases['main'].id, lhs==rhs.id), isouter=True).add_entity(self.aliases[name])

    def all(self):
        if self.multiple_entities:
            rows = self.q.distinct(self.aliases['main'].id).all()
        else:
            rows = [[s] for s in self.q.all()]

        results = []
        statements = {}

        for r in rows:
            result = []
            for s in r:
                if s is not None:
                    if not str(s.uuid) in statements:
                        statements[str(s.uuid)] = s
                    result.append(str(s.uuid))
                else:
                    result.append(None)
            results.append(result)

        return results, statements


class CoreStatementQuery(object):

    def __init__(self, db):
        self.db = db
        self.st_table = Statement.__table__
        self.aliases = {}
        self.visible_aliases = []
        self.s = select()

    def add_join(self, name, options):
        print("  ADD JOIN:", name, options)
        if options == ['', '', '']:
            self.aliases[name] = Statement.__table__.alias(name)
        else:
            alias = Statement.__table__.alias(name)
            cols = {c.name: c for c in tuple(alias.c)}
            join_clauses = []
            for idx, option in enumerate(options):
                if option == '':
                    continue
                if ':' in option:
                    value, column_name = self.get_value_info(option, idx)
                    join_clauses.append(cols[column_name]==value)
                else:
                    target = self.aliases[option]
                    if idx == 0:
                        join_clauses.append(alias.c.subject_id==target.c.id)
                    elif idx == 1:
                        join_clauses.append(alias.c.predicate_id==target.c.id)
                    elif idx == 2:
                        join_clauses.append(alias.c.object_statement_id==target.c.id)
            self.aliases[name] = alias
            self.create_join(alias, join_clauses)
            #self.s = self.s.select_from(self.aliases[name])

    def create_join(self, alias, clauses):
        if len(self.s.froms):
            fr = self.s.froms[0]
        else:
            fr = self.aliases['main']
        if len(clauses) == 1:
            join = fr.join(alias, clauses[0], isouter=True)
        else:
            join = fr.join(alias, and_(*clauses), isouter=True)
        self.s = self.s.select_from(join)
        return join

    def add_column(self, name):
        print("  ADD COLUMN:", name)
        alias = self.aliases[name]
        for c in tuple(alias.c):
            self.s = self.s.column(c.label("{}_{}".format(name, c.name)))

    def add_filter(self, name, options):
        print("  ADD FILTER:", name, options)
        alias = self.aliases[name]
        cols = {c.name: c for c in tuple(alias.c)}
        for idx, option in enumerate(options):
            if option == '':
                continue
            value, column_name = self.get_value_info(option, idx)
            self.s = self.s.where(cols[column_name]==value)

    def get_value_info(self, value, element_idx=2):
        type_str, value_str = value.split(':', 1)
        if type_str == 'st':
            uuid_ = UUID(value_str)
            query = select([self.st_table.c.id]).where(self.st_table.c.uuid==uuid_)
            column_name = [
                'subject_id',
                'predicate_id',
                'object_statement_id'
            ][element_idx]
            return query, column_name

        if element_idx != 2:
            # all non-Statement types are only valid as the 3rd value element
            return None, None

        if type_str == 'uuid':
            return UUID(value_str), None
        elif type_str == 'blob':
            query = select([Blob.__table__.c.id]).where(Blob.__table__.c.sha256==value_str)
            return query, 'object_blob_id'
        elif type_str == 'int':
            return int(value_str), 'object_integer'
        elif type_str == 'str':
            return str(value_str), 'object_string'
        elif type_str == 'datetime':
            return datetime.datetime.strptime(value_str, '%Y-%m-%dT%H:%M:%S.%f'), 'object_datetime'
        else:
            return None, None

    def serialize_column_value(self, column_name, value):
        if column_name in ['subject_id', 'predicate_id', 'object_statement_id']:
            return 'st:{}'.format(value)
        elif column_name == 'uuid':
            return 'uuid:{}'.format(value)
        else:
            return value

    def process_row(self, row):
        by_alias = defaultdict(dict)
        for k, v in row.items():
            alias_name, column_name = k.split('_', 1)
            by_alias[alias_name][column_name] = v
        return by_alias

    def process_quad(self, values, statement_uuids):
        if values['id'] is None:
            return None
        parts = ['uuid:{}'.format(values['uuid'])]
        parts.append('st:{}'.format(statement_uuids[values['subject_id']]))
        parts.append('st:{}'.format(statement_uuids[values['predicate_id']]))
        if values['object_statement_id'] is not None:
            obj = 'st:{}'.format(statement_uuids[values['object_statement_id']])
        elif values['object_integer'] is not None:
            obj = 'int:{}'.format(values['object_integer'])
        elif values['object_string'] is not None:
            obj = 'str:{}'.format(values['object_string'])
        else:
            # TODO: Support all types
            obj = None
        parts.append(obj)
        return parts

    def process_result(self, result):
        processed_rows = []
        statement_uuids = {}
        need_statement_ids = set()
        need_blob_ids = set()

        for row in result:
            by_alias = self.process_row(row)
            for alias_name, values in by_alias.items():
                if values['id'] is None:
                    continue
                statement_uuids[values['id']] = values['uuid']
                need_statement_ids.add(values['subject_id'])
                need_statement_ids.add(values['predicate_id'])
                if values['object_statement_id']:
                    need_statement_ids.add(values['object_statement_id'])
                if values['object_blob_id']:
                    need_blob_ids.add(values['object_blob_id'])

            processed_rows.append(by_alias)

        fetch_ids = list(need_statement_ids - statement_uuids.keys())
        s = select([self.st_table.c.id, self.st_table.c.uuid]).where(self.st_table.c.id.in_(fetch_ids))
        for statement_id, statement_uuid in self.db.connection().execute(s):
            statement_uuids[statement_id] = statement_uuid

        resultrows = []
        for by_alias in processed_rows:
            resultrow = {}
            for alias_name, values in by_alias.items():
                quad = self.process_quad(values, statement_uuids)
                resultrow[alias_name] = quad
            resultrows.append(resultrow)

        return resultrows

    def all(self):
        result = self.db.connection().execute(self.s)
        return self.process_result(result)
