import requests, json, csv, esprit, sys
from portality.core import app
from doaj2oag import oag, oagr
from datetime import datetime, timedelta

conn = esprit.raw.Connection(app.config.get("ELASTIC_SEARCH_HOST"), app.config.get("ELASTIC_SEARCH_DB"))

query = {
    "query" : {
        "term" : {
            "bibjson.identifier.type.exact" : "doi"
        }
    }
}

def do_query(query):
    source = json.dumps(query)
    base_url = app.config.get("QUERY_ENDPOINT")
    url = base_url + "?source=" + source
    resp = requests.get(url)
    return json.loads(resp.text)

def iterate(q, page_size=1000, limit=None):
    q["size"] = page_size
    q["from"] = 0
    if "sort" not in q: # to ensure complete coverage on a changing index, sort by id is our best bet
        q["sort"] = [{"id" : {"order" : "asc"}}]
    counter = 0
    while True:
        # apply the limit
        if limit is not None and counter >= limit:
            break

        res = do_query(q)
        rs = [r.get("_source") if "_source" in r else r.get("fields") for r in res.get("hits", {}).get("hits", [])]

        if len(rs) == 0:
            break
        for r in rs:
            # apply the limit (again)
            if limit is not None and counter >= limit:
                break
            counter += 1
            yield r
        q["from"] += page_size

print "Initialising ... requesting all DOIs from DOAJ ..."
iterator = iterate(query, limit=100000)
doibatch = []
job_size = 2000
start = None
lag = 900 # 900s = 15 mins
for res in iterator:
    thisdois = [ident.get("id").strip() for ident in res.get("bibjson", {}).get("identifier", [])
                if ident.get("type") == "doi" and ident.get("id") is not None]
    # FIXME: OAG does not currently handle unicode characters
    cleaned = []
    for d in thisdois:
        try:
            d.decode("ascii")
            cleaned.append(d)
        except:
            pass
    # FIXME: note that we do not de-duplicate, but there probably are duplicates
    doibatch += cleaned
    if len(doibatch) >= job_size:
        start = datetime.now() if start is None else start + timedelta(seconds=lag)
        print "Creating job with", len(doibatch), "DOIs, to start at " + start.strftime("%Y-%m-%d %H:%M:%S")
        state = oag.RequestState(doibatch, timeout=None, back_off_factor=10, max_back_off=1800, max_retries=100, batch_size=100, start=start)
        oagr.JobRunner.make_job(state)
        doibatch = []

if len(doibatch) > 0:
    start = datetime.now() if start is None else start + timedelta(seconds=lag)
    print "Creating job with", len(doibatch), "DOIs, to start at " + start.strftime("%Y-%m-%d %H:%M:%S")
    state = oag.RequestState(doibatch, timeout=None, back_off_factor=10, max_back_off=1800, max_retries=100, batch_size=100, start=start)
    oagr.JobRunner.make_job(state)


