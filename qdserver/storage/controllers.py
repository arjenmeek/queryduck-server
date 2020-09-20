import base64
import datetime
import os

from pyramid.view import view_config
from sqlalchemy.sql import select, and_, not_
from sqlalchemy.sql.expression import exists
from sqlalchemy.dialects.postgresql import insert as pg_insert

from ..controllers import BaseController
from ..models import statement_table, volume_table, blob_table, file_table


class StorageController(BaseController):

    max_limit = 10000

    @view_config(route_name="get_volume", renderer="json")
    def get_volume(self):
        ref = self.request.matchdict["reference"]
        s = select([volume_table]).where(volume_table.c.reference==ref)
        volume = self.db.execute(s).fetchone()
        return dict(volume)

    @view_config(route_name="list_volumes", renderer="json")
    def list_volumes(self):
        s = select([volume_table])
        volumes = [dict(v) for v in self.db.execute(s)]
        return volumes

    @view_config(route_name="create_volume", renderer="json")
    def create_volume(self):
        ref = self.request.matchdict["reference"]
        ins = volume_table.insert().values(reference=ref)
        (insert_id,) = self.db.execute(ins).inserted_primary_key
        return insert_id

    @view_config(route_name="delete_volume", renderer="json")
    def delete_volume(self):
        ref = self.request.matchdict["reference"]
        delete = volume_table.delete().where(volume_table.c.reference==ref)
        self.db.execute(delete)
        return {}

    def _get_volume(self, reference):
        s = select([volume_table]).where(volume_table.c.reference==reference)
        volume = self.db.execute(s).fetchone()
        return volume

    @view_config(route_name="list_volume_files", renderer="json")
    def list_volume_files(self):
        volume = self._get_volume(self.request.matchdict["volume_reference"])

        j = file_table.join(blob_table, file_table.c.blob_id==blob_table.c.id)
        s = select([file_table, blob_table.c.sha256]).select_from(j).\
            where(file_table.c.volume_id==volume["id"])

        if "without_statements" in self.request.GET:
            filter_query = select([statement_table.c.id]).select_from(statement_table).where(
                statement_table.c.object_blob_id==blob_table.c.id)
            s = s.where(not_(exists(filter_query)))

        if "path" in self.request.GET:
            paths = [base64.urlsafe_b64decode(p) for p in self.request.GET.getall("path")]
            s = s.where(file_table.c.path.in_(paths))

        if "after" in self.request.GET:
            after = base64.urlsafe_b64decode(self.request.GET["after"])
            s = s.where(file_table.c.path > after)

        limit = 1000
        if "limit" in self.request.GET:
            limit = min(int(self.request.GET["limit"]), self.max_limit)
        s = s.order_by(file_table.c.path).limit(limit)

        files = [{
            "path": os.fsdecode(r[file_table.c.path]),
            "size": r[file_table.c.size],
            "mtime": r[file_table.c.mtime].isoformat(),
            "lastverify": r[file_table.c.lastverify].isoformat(),
            "sha256": base64.urlsafe_b64encode(r[blob_table.c.sha256]).decode("utf-8")
        } for r in self.db.execute(s)]

        return {
            "results": files,
            "limit": limit,
        }

    def _process_file_blobs(self, files):
        """Determines which required blobs don't exist yet, and construct them."""
        file_checksums = {f["sha256"] for f in files}
        s = select([blob_table.c.sha256]).where(blob_table.c.sha256.in_(file_checksums))
        db_checksums = {r for (r,) in self.db.execute(s)}
        new_checksums = file_checksums - db_checksums
        return new_checksums

    def _process_files(self, files):
        file_checksums = {f["sha256"] for f in files}
        s = select([blob_table.c.id, blob_table.c.sha256]).where(blob_table.c.sha256.in_(file_checksums))
        blob_ids = {blob_checksum: blob_id for blob_id, blob_checksum in self.db.execute(s)}
        for f in files:
            f["blob_id"] = blob_ids[f["sha256"]]
            del f["sha256"]
        return files

    @view_config(route_name="mutate_volume_files", renderer="json")
    def mutate_volume_files(self):
        volume = self._get_volume(self.request.matchdict["volume_reference"])
        self._mutate_volume_files(volume, self.request.json_body)
        return {}

    def _mutate_volume_files(self, volume, files_info):
        files = [{
            "volume_id": volume.id,
            "path": os.fsencode(path),
            "sha256": base64.urlsafe_b64decode(rf["sha256"]),
            "mtime": datetime.datetime.fromisoformat(rf["mtime"]),
            "lastverify": datetime.datetime.fromisoformat(rf["lastverify"]),
            "size": rf["size"],
        } for path, rf in files_info.items() if rf is not None]

        # Using multi insert seems orders of magnitude faster on Postgres than
        # multiparam/executemany inserts.
        new_checksums = self._process_file_blobs(files)
        if len(new_checksums):
            self.db.execute(blob_table.insert().values([{"sha256": c} for c in new_checksums]))

        delete_paths = [os.fsencode(path) for path, rf in files_info.items() if rf is None]
        if len(delete_paths):
            delete = file_table.delete().where(file_table.c.volume_id==volume["id"]).\
                where(file_table.c.path.in_(delete_paths))
            self.db.execute(delete)

        # Upsert files in bulk
        files = self._process_files(files)
        ins = pg_insert(file_table).values(files)
        upd = ins.on_conflict_do_update(index_elements=["volume_id", "path"], set_={
            "blob_id": ins.excluded.blob_id,
            "size": ins.excluded.size,
            "mtime":  ins.excluded.mtime,
            "lastverify": ins.excluded.lastverify,
        })
        self.db.execute(upd)
