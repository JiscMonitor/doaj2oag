import requests, json, csv, esprit, sys
from portality.core import app
from doaj2oag import oag, oagr

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
        # print counter, len(rs), res.get("hits", {}).get("total"), len(res.get("hits", {}).get("hits", [])), json.dumps(q)
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
iterator = iterate(query, limit=10000)
alldois = []
# doi_id_map = {}
for res in iterator:
    thisdois = [ident.get("id") for ident in res.get("bibjson", {}).get("identifier", []) if ident.get("type") == "doi"]
    #for d in thisdois:
    #    doi_id_map[d] = res.get("id")
    alldois += thisdois
print "Received ", len(alldois), "DOIs"

state = oag.RequestState(alldois, timeout=None, back_off_factor=10, max_back_off=1800, max_retries=100, batch_size=100)
oagr.JobRunner.make_job(state)

# oag.oag_it(app.config.get("OAG_LOOKUP_URL"), alldois, timeout=14400, batch_size=100, callback=output_callback, save_state=save_callback)

