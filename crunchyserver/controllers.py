import base64
import datetime
import os
import urllib

from pyramid.view import view_config
from pyramid.httpexceptions import HTTPNotFound
from uuid import UUID, uuid4

from sqlalchemy.orm import joinedload

from crunchylib.exceptions import GeneralError, NotFoundError
from crunchylib.utility import deserialize_value, get_value_type


from .query import CoreStatementQuery
from .models import Statement, Volume, Blob, File


class BaseController(object):
    """Provide a basic Controller class to extend."""

    def __init__(self, request):
        """Make relevant services available."""
        self.request = request
        self.db = self.request.find_service(name='db')


class StatementController(BaseController):
    """Handle requests primarily concerned with Statements."""

    def __init__(self, request):
        """Make relevant services available."""
        self.request = request
        self.db = self.request.find_service(name='db')
        self.statements = self.request.find_service(name='statement_repository')

    def _parse_filter_string(self, filter_string):
        parts = filter_string.split(',')
        if len(parts) == 2:
            lhs, op = parts
            rhs = None
        elif len(parts) == 3:
            lhs, op, rhs = parts
        else:
            raise GeneralError("Invalid filter string: {}".format(filter_string))

        return lhs, op, rhs

    def _parse_join_string(self, join_string):
        parts = join_string.split(',')
        if len(parts) == 3:
            name, lhs, op = parts
            rhs = None
        elif len(parts) == 4:
            name, lhs, op, rhs = parts
        else:
            raise GeneralError("Invalid filter string: {}".format(filter_string))

        return name, lhs, op, rhs

    def parse_uuid_reference(self, reference):
        """Deserialize the reference if it's a UUID, raise an exception otherwise."""
        uuid_ = deserialize_value(reference)
        if type(uuid_) != UUID:
            raise GeneralError("Invalid reference type")

        return uuid_

    @view_config(route_name='find_statements', renderer='json')
    def find_statements(self):
        """Return multiple Statements."""
        qc = self.statements.query()

        join_strings = self.request.GET.getall('join')
        for js in join_strings:
            name, lhs, op, rhs = self._parse_join_string(js)
            qc.apply_join(name, lhs, op, rhs)

        filter_strings = self.request.GET.getall('filter')
        for fs in filter_strings:
            lhs, op, rhs = self._parse_filter_string(fs)
            qc.apply_filter(lhs, op, rhs)

        results, statements = qc.all()
        return {'results': results, 'statements': statements}

    @view_config(route_name='get_statement', renderer='json')
    def get_statement(self):
        """Get one Statement by its UUID."""
        uuid_ = self.parse_uuid_reference(self.request.matchdict['reference'])
        try:
            statement = self.statements.get_by_uuid(uuid_)
        except NotFoundError:
            raise HTTPNotFound()
        return statement

    @view_config(route_name='put_statement', renderer='json')
    def put_statement(self):
        """Insert one Statement by its UUID."""
        print('put_statement:', self.request.json_body)
        raw_st = self.request.json_body
        uuid_ = self.parse_uuid_reference(self.request.matchdict['reference'])

        subject_r, predicate_r, object_r = [deserialize_value(v) for v in self.request.json_body[1:]]

        statement = self.statements.new(uuid_, subject_r, predicate_r, object_r)

        self.db.add(statement)
        self.db.commit()
        return statement

    @view_config(route_name='create_statements', renderer='json')
    def create_statements(self):
        """Insert multiple statements"""
        print("MULTI INSERT")
        inserted = []
        for row in self.request.json_body:
            print(row)
            parts = [uuid4()]
            for v in row:
                if type(v) == int:
                    parts.append(inserted[v])
                else:
                    print("PART", type(v))
                    parts.append(deserialize_value(v))
            statement = self.statements.new(*parts)
            inserted.append(statement)
            self.db.add(statement)
        self.db.commit()
        return {}

    @view_config(route_name='delete_statement', renderer='json')
    def delete_statement(self):
        """Delete a Statement by its UUID."""
        uuid_ = self.parse_uuid_reference(self.request.matchdict['reference'])
        statement = self.statements.get_by_uuid(uuid_)
        self.db.delete(statement)
        self.db.commit()
        return {}

    def _parse_query_string(self, query_string):
        q = CoreStatementQuery(self.db)
        clauses = query_string.split('&')
        options = {}
        for clause in clauses:
            key, value_str = clause.split('=', 1)
            if '.' in value_str:
                value = [urllib.parse.unquote(p) for p in value_str.split('.')]
            else:
                value = urllib.parse.unquote(value_str)

            if key.startswith('c_'):
                q.add_join(key[2:], value)
                q.add_column(key[2:])
            elif key.startswith('j_'):
                q.add_join(key[2:], value)
            elif key.startswith('f_'):
                q.add_filter(key[2:], value)

        return q

    def serialize_row(self, row):
        values = []
        for v in tuple(row):
            if type(v) == UUID:
                values.append(str(v))
            else:
                values.append(v)
        return values

    @view_config(route_name='query_statements', renderer='json')
    def query_statements(self):
        """Query Statements."""
        query = self._parse_query_string(self.request.query_string)
        return query.all()


class VolumeController(BaseController):

    @view_config(route_name='get_volume', renderer='json')
    def get_volume(self):
        volume = self.db.query(Volume).filter_by(reference=self.request.matchdict['reference']).one()
        return volume

    @view_config(route_name='list_volumes', renderer='json')
    def list_volumes(self):
        volumes = self.db.query(Volume).all()
        return volumes

    @view_config(route_name='create_volume', renderer='json')
    def create_volume(self):
        volume = Volume()
        volume.reference = self.request.matchdict['reference']
        self.db.add(volume)
        self.db.commit()
        return volume

    @view_config(route_name='delete_volume', renderer='json')
    def delete_volume(self):
        volume = self.db.query(Volume).filter_by(reference=self.request.matchdict['reference']).one()
        self.db.delete(volume)
        self.db.commit()
        return {}


class BlobController(BaseController):

    @view_config(route_name='get_blob', renderer='json')
    def get_blob(self):
        return [self.request.matchdict['reference']]

    @view_config(route_name='list_blobs', renderer='json')
    def list_blobs(self):
        blobs = self.db.query(Blob).all()
        return blobs

    @view_config(route_name='create_blob', renderer='json')
    def create_blob(self):
        blob_data = self.request.json_body['blob']
        print(blob_data)
        blob = Blob()
        self.db.add(blob)
        return blob


class FileController(BaseController):

    max_limit = 10000

    @view_config(route_name='list_volume_files', renderer='json')
    def list_volume_files(self):
        limit = 1000
        if 'limit' in self.request.GET:
            limit = min(int(self.request.GET['limit']), self.max_limit)

        volume = self.db.query(Volume).filter(Volume.reference==self.request.matchdict['volume_reference']).one()

        q = self.db.query(File).filter(File.volume==volume).options(joinedload(File.blob)).order_by(File.path)
        if 'after' in self.request.GET:
            after = base64.b64decode(self.request.GET['after'])
            q = q.filter(File.path > after)
        files = q.order_by(File.path).limit(limit).all()

        response = {
            'results': files,
            'limit': limit,
        }
        return response

    def _process_file_blobs(self, files):
        """Determines which required blobs don't exist yet, and construct them."""
        file_checksums = {f['sha256'] for f in files}
        db_checksums = {r for (r,) in self.db.query(Blob.sha256).filter(Blob.sha256.in_(file_checksums)).all()}
        new_checksums = file_checksums - db_checksums
        return new_checksums

    def _process_files(self, files):
        file_checksums = {f['sha256'] for f in files}
        db_fields = self.db.query(Blob.id, Blob.sha256).filter(Blob.sha256.in_(file_checksums)).all()
        blob_ids = {blob_checksum: blob_id for blob_id, blob_checksum in db_fields}
        for f in files:
            f['blob_id'] = blob_ids[f['sha256']]
            del f['sha256']
        return files

    @view_config(route_name='mutate_volume_files', renderer='json')
    def mutate_volume_files(self):
        volume = self.db.query(Volume).filter(Volume.reference==self.request.matchdict['volume_reference']).one()

        files = [{
            'volume_id': volume.id,
            'path': os.fsencode(path),
            'sha256': base64.b64decode(rf['sha256']),
            'mtime': datetime.datetime.strptime(rf['mtime'], "%Y-%m-%dT%H:%M:%S.%f"),
            'lastverify': datetime.datetime.strptime(rf['lastverify'], "%Y-%m-%dT%H:%M:%S.%f"),
            'size': rf['size'],
        } for path, rf in self.request.json_body.items() if rf is not None]

        new_checksums = self._process_file_blobs(files)

        # Using multi insert seems orders of magnitude faster on Postgres than multiparam/executemany inserts.
        self.db.connection().execute(Blob.__table__.insert().values([{'sha256': c} for c in new_checksums]))

        # We also delete all files that need updating, so we can simply insert them again.
        delete_paths = [os.fsencode(path) for path, rf in self.request.json_body.items()
            if rf is None or not rf['new']]
        delete_query = self.db.query(File).filter(File.volume==volume).filter(File.path.in_(delete_paths))
        delete_query.delete(synchronize_session=False)

        # (Re-)insert files in bulk
        files = self._process_files(files)
        self.db.connection().execute(File.__table__.insert().values(files))

        self.db.commit()

        return {}

    @view_config(route_name='get_volume_file', renderer='json')
    def get_volume_file(self):
        volume = self.db.query(Volume).filter(Volume.reference==self.request.matchdict['volume_reference']).one()
        file_path = base64.b64decode(self.request.matchdict['file_path'], '-_')
        file_ = self.db.query(File).filter(File.volume==volume).filter(File.path==file_path).first()
        print(file_)
        return file_
