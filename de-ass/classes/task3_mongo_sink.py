# Author: Low Qing Ying (Task 3)

from __future__ import annotations
from typing import Any, Dict, List
from pymongo import MongoClient, UpdateOne, ASCENDING, DESCENDING

class MongoSink:
    """
    Writes the transformed Spark DataFrame (one row per country) to MongoDB.
    Upserts by (country, timestamps.updated) and creates helpful indexes.
    """

    def __init__(self, uri: str, db: str, coll: str):
        self.uri = uri
        self.db = db
        self.coll = coll

    @staticmethod
    def _row_to_doc(row) -> Dict[str, Any]:
        doc = row.asDict(recursive=True)

        ts = doc.get("timestamps") or {}
        ts["updated"] = ts.get("updated") or ts.get("event_ts") or ts.get("event_date")
        doc["timestamps"] = ts

        return doc

    def upsert_by_country_updated(self, df) -> None:
        uri, db, coll = self.uri, self.db, self.coll

        def _write(partition):
            client = MongoClient(uri, tls="mongodb+srv" in uri)
            c = client[db][coll]
            ops: List[UpdateOne] = []

            for row in partition:
                d = MongoSink._row_to_doc(row)
                key = {
                    "country": d.get("country"),
                    "timestamps.updated": d.get("timestamps", {}).get("updated")
                }
                ops.append(UpdateOne(key, {"$set": d}, upsert=True))

                if len(ops) >= 1000:
                    c.bulk_write(ops, ordered=False)
                    ops.clear()

            if ops:
                c.bulk_write(ops, ordered=False)

        df.foreachPartition(_write)

    def ensure_indexes(self) -> None:
        client = MongoClient(self.uri, tls="mongodb+srv" in self.uri)
        c = client[self.db][self.coll]
        c.create_index([("country", ASCENDING), ("timestamps.updated", DESCENDING)], name="country_updated")
        c.create_index([("region", ASCENDING)])
        c.create_index([("metrics.cases.total", DESCENDING)])
        c.create_index([("metrics.tests.per_million", DESCENDING)])

    def dedupe_keep_latest_per_country(self) -> int:
        client = MongoClient(self.uri, tls="mongodb+srv" in self.uri)
        c = client[self.db][self.coll]
        pipeline = [
            {"$sort": {"country": 1, "timestamps.updated": -1}},
            {"$group": {"_id": "$country", "ids": {"$push": "$_id"}}},
            {"$project": {"_id": 0, "ids": {"$slice": ["$ids", 1, {"$size": "$ids"}]}}}
        ]
        ids = []
        for g in c.aggregate(pipeline, allowDiskUse=True):
            ids.extend(g.get("ids", []))
        if ids:
            res = c.delete_many({"_id": {"$in": ids}})
            return res.deleted_count
        return 0
