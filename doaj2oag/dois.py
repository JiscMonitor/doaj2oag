import requests, json, csv, esprit
from portality.core import app
from doaj2oag import oag

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

iterator = iterate(query, limit=5000)
alldois = []
doi_id_map = {}
for res in iterator:
    thisdois = [ident.get("id") for ident in res.get("bibjson", {}).get("identifier", []) if ident.get("type") == "doi"]
    for d in thisdois:
        doi_id_map[d] = res.get("id")
    alldois += thisdois

def output_callback(state):
    conn = esprit.raw.Connection(app.config.get("ELASTIC_SEARCH_HOST"), app.config.get("ELASTIC_SEARCH_DB"))
    success = state.flush_success()
    for s in success:
        # first locate the id of the doaj object to be updated
        doajid = None
        for i in s.get("identifier", [{}]):
            if i.get("type") == "doi":
                doajid = doi_id_map.get(i.get("id"))
                break
        if doajid is None:
            print "could not backtrack to doaj id"
            continue

        # now get the object from the index
        doajobj = esprit.raw.unpack_get(esprit.raw.get(conn, "article", doajid))

        # add the licence information
        bibjson = doajobj.get("bibjson")
        if bibjson is None:
            print "no bibjson record"
            continue
        bibjson["license"] = s.get("license")

        # now save the record
        esprit.raw.store(conn, "article", doajobj, doajid)
        print "saved", doajid

    errors = state.flush_error()
    with open("oag_error.csv", "a") as f:
        writer = csv.writer(f)
        for e in errors:
            identifier = e.get("identifier", [{}]).get("id")
            msg = e.get("error")
            csv_row = [identifier, msg]
            clean_row = [unicode(c).encode("utf8", "replace") if c is not None else "" for c in csv_row]
            writer.writerow(clean_row)


oag.oag_it(app.config.get("OAG_LOOKUP_URL"), alldois, timeout=500, callback=output_callback)

