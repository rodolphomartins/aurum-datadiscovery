"""
One-time migration: re-index profile documents under their correct integer ID.

push_to_es.py originally did not pass id=p.id to es.index(), so ES 7 auto-generated
random string IDs. The networkbuilder expects integer IDs (CRC32) as ES _id, because
Hit.__hash__ does int(nid). The correct ID is stored inside each doc body as "id".

This script:
  1. Scrolls all docs in the 'profile' index
  2. Re-indexes each under id=doc["id"] (the CRC32 value)
  3. Deletes the old doc if its _id was wrong

Safe to re-run: re-indexing under the same ID is an upsert (no-op if already correct).
"""

from elasticsearch import Elasticsearch

INDEX = "profile"
PAGE_SIZE = 500

es = Elasticsearch()

resp = es.search(index=INDEX, body={"query": {"match_all": {}}}, size=PAGE_SIZE, scroll="5m")
scroll_id = resp["_scroll_id"]
hits = resp["hits"]["hits"]

fixed = 0
already_correct = 0

while hits:
    for doc in hits:
        body = doc["_source"]
        correct_id = str(body["id"])   # CRC32 integer stored in doc body
        current_id = doc["_id"]        # ES-assigned _id (may be auto-generated string)

        es.index(index=INDEX, id=correct_id, body=body)

        if current_id != correct_id:
            es.delete(index=INDEX, id=current_id)
            fixed += 1
        else:
            already_correct += 1

    resp = es.scroll(scroll_id=scroll_id, scroll="5m")
    scroll_id = resp["_scroll_id"]
    hits = resp["hits"]["hits"]

es.clear_scroll(scroll_id=scroll_id)

print(f"Done. Fixed: {fixed}, already correct: {already_correct}")
