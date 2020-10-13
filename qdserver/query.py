from itertools import islice

from sqlalchemy import and_, or_
from sqlalchemy.sql import select

from queryduck.query import (
    QueryElement,
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
    value_types_by_native,
)

from .errors import UserError
from .models import statement_table, blob_table, file_table, volume_table
from .utility import (
    process_db_row,
    column_compare,
    final_column_compare,
    prepare_for_db,
)


class QueryEntity:
    pass

class MainEntity(QueryEntity):
    id_name = "id"

    def __init__(self):
        self.target = None

class JoinEntity(QueryEntity):

    def __init__(self, predicate, target):
        self.predicate = predicate
        self.target = target

class ObjJoinEntity(JoinEntity):
    id_name = "object_statement_id"
    lhs_name = "subject_id"

class SubJoinEntity(JoinEntity):
    id_name = "subject_id"
    lhs_name = "object_statement_id"


class FromClauseBuilder:

    def __init__(self, target, entities):
        self.target = target
        self.entities = entities
        self.fromclause = None
        self.aliases = {}

    def _join_entity(self, entity, alias):
        """Join an entity to the fromclause"""
        target = self.entities[entity.target]
        target_alias = self.aliases[entity.target]
        where = alias.c[entity.lhs_name] == target_alias.c[target.id_name]
        if entity.predicate:
            where = and_(where, alias.c.predicate_id == entity.predicate.id)
        self.fromclause = self.fromclause.join(alias, where, isouter=True)

    def _find_join_chain(self, key):
        """Determine how much of the required join chain needs to be added"""
        stack = []
        nextkey = key
        while not (nextkey is None or nextkey in self.aliases):
            stack.append(nextkey)
            nextkey = self.entities[nextkey].target
        return reversed(stack)

    def get_entity_alias(self, key):
        """Create the required aliases and join them to the existing fromclause"""
        chain = self._find_join_chain(key)

        for join_key in chain:
            alias = statement_table.alias(join_key)
            self.aliases[join_key] = alias
            entity = self.entities[join_key]
            if entity.target is None:
                self.fromclause = alias
                continue
            self._join_entity(entity, alias)

        return self.aliases[key]


class PGQuery:
    def __init__(self, repo, target, after=None):
        self.repo = repo
        self.target = target
        self.after = after
        self.db = self.repo.db

        self.entities = {'main': MainEntity()}
        self.filters = []
        self.final_filters = []
        self.sorts = []
        self.prefers = []
        self.fetches = []

        self.final_wheres = []
        self.extra_columns = []
        self.results = None
        self.order_by = []
        self.limit = 5000

    def _get_entity_key(self, prefix=None):
        if prefix is None:
            prefix = 'statement'
        for i in range(1000):
            key = f"{prefix}_{i}"
            if not key in self.entities:
                break
        else:
            raise UserError("Too many entities")
        return key

    def apply_query(self, query):
        stack = [(query, 'main')]
        while stack:
            q, e = stack.pop()
            if type(q) == dict:
                for k, v in q.items():
                    if isinstance(k, QueryElement):
                        ekey = self._get_entity_key()
                        target_object = type(k) in (MatchObject, FetchObject)
                        self.fetches.append((ekey, target_object, k.value))
                        cls = ObjJoinEntity if target_object else SubJoinEntity
                        self.entities[ekey] = cls(k.value, e)
                        if type(v) == dict:
                            stack.append((v, ekey))
                    elif k in ("sort", "sort+"):
                        self.sorts.append((e, v, True))
                    elif k in ("prefer", "prefer+"):
                        self.prefers.append((e, v, True))
                    elif k.endswith("."):
                        self.final_filters.append((e, k[:-1], v))
                    else:
                        self.filters.append((e, k, v))
            else:
                self.filters.append((e, "eq", q))
        print("ENTITIES", self.entities)
        print("FILTERS", self.filters)
        print("FETCHES", self.fetches)

    def get_results(self):
        #joins = {"main": self.target}

        builder = FromClauseBuilder(self.target, self.entities)
        #select_from = self.target
        wheres = []

        for entity_key, op, value in self.filters:
            #joins, select_from = self._add_entity_chain(joins, select_from, entity_key)
            #a = joins[entity_key]
            a = builder.get_entity_alias(entity_key)
            wheres.append(column_compare(value, op, a.c))

        main = builder.get_entity_alias("main")

        sub = select(
            [main.c.id, main.c.uuid] + self.order_by + self.extra_columns
        ).select_from(builder.fromclause)
        sub = (
            sub.where(and_(*wheres))
#            .distinct(self.target.c.uuid)
#            .order_by(self.target.c.uuid, *self.prefer_by)
        )
        print("SUB", builder.fromclause)
        print("--------")
        if self.after is not None:
            sub = sub.where(self.target.c.uuid > self.after.uuid)
        if self.order_by or self.final_wheres:
            sub = sub.alias("mysubquery")
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
        s = s.limit(self.limit + 1)
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
        main_ids = [s.id for s in self.results]
        ids = main_ids[:]

        fetches_by_entity = {}
        for entity, target_object, predicate in self.fetches:
            key = (entity, target_object)
            print("FETCH", entity, target_object, predicate)
            if predicate is None:
                fetches_by_entity[key] = None
                continue
            if not key in fetches_by_entity:
                fetches_by_entity[key] = set()
            if fetches_by_entity[key] is None:
                continue
            fetches_by_entity[key].add(predicate.id)

        for (entity, target_object), predicate_ids in fetches_by_entity.items():
            builder = FromClauseBuilder(self.target, self.entities)
            a = builder.get_entity_alias(entity)
            main = builder.get_entity_alias("main")
            sel = select([a.c.id]).select_from(builder.fromclause).where(main.c.id.in_(main_ids))
            if predicate_ids is not None:
                sel = sel.where(a.c.predicate_id.in_(predicate_ids))
            print("FETCH SEL", sel)
            res = self.db.execute(sel)
            ids += [i[0] for i in res.fetchall()]

        allids = set(ids)
        s, entities = self.repo.select_full_statements(self.target)
        where = self.target.c.id.in_(allids)
        s = s.where(where).distinct(statement_table.c.id)

        results = self.db.execute(s)
        statements = self.repo.process_result_statements(results, entities)
        return statements
