import esprit
from portality.core import app
from doaj2oag import oag
import time, sys, csv
from datetime import datetime

class JobRunner(object):
    def __init__(self, lookup_url, es_throttle=2, oag_throttle=5, verbose=True, callback=None):
        self.es_throttle = es_throttle
        self.conn = esprit.raw.Connection(app.config.get("ELASTIC_SEARCH_HOST"), app.config.get("ELASTIC_SEARCH_DB"))
        self.index = "jobs"
        self.lookup_url = lookup_url
        self.client = oag.OAGClient(lookup_url)
        self.verbose = verbose
        self.oag_throttle = oag_throttle
        self.callback = callback

    @classmethod
    def make_job(cls, state):
        r = JobRunner(None)
        r.save_job(state)

    def has_due(self):
        now = datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")
        query = {
            "query" : {
                "range" : {
                    "pending.due" : {
                        "lte" : now
                    }
                }
            },
            "sort" : [{"pending.due" : {"order" : "asc", "mode" : "min"}}],
            "fields" : ["id"],
            "size" : 10000
        }

        resp = esprit.raw.search(self.conn, self.index, query=query)
        results = esprit.raw.unpack_result(resp)
        counter = 0
        for res in results:
            id = res.get("id")
            resp2 = esprit.raw.get(self.conn, self.index, id)
            obj = esprit.raw.unpack_get(resp2)
            state = oag.RequestState.from_json(obj)
            counter += 1
            yield state, counter, len(results)

    def get_finished(self):
        pass

    def save_job(self, state):
        print "Saving state to ElasticSearch ...",
        sys.stdout.flush()
        j = state.json()
        esprit.raw.store(self.conn, self.index, j, j.get("id"))
        print "Complete"

    def cycle_state(self, state):
        if self.verbose:
            print "Processing job " + state.id
            print state.print_parameters()
            print state.print_status_report()

        # issue the cycle request
        self.client.cycle(state, self.oag_throttle, self.verbose)
        if self.verbose:
            print state.print_status_report()

        # run the callback on the state
        if self.callback is not None:
            self.callback(state)

        # run the save method if there is one
        self.save_job(state)

        # if we have done work here, update the next due time for the busy
        # loop aboge
        next = state.next_due()
        print "Next request is due at", datetime.strftime(next, "%Y-%m-%d %H:%M:%S"), "\n"

    def run(self):
        print "Starting OAGR ... Started"
        col_counter = 0
        while True:
            time.sleep(self.es_throttle)
            states = self.has_due()
            found = False
            for state, n, of in states:
                found = True
                col_counter = 0
                print ""
                print "Processing", n, "of", of, "in this round of jobs\n"
                self.cycle_state(state)
            if not found:
                print ".",
                sys.stdout.flush()
                col_counter += 1
                if col_counter >= 36:
                    print ""
                    col_counter = 0

def doaj_closure():

    conn = esprit.raw.Connection(app.config.get("ELASTIC_SEARCH_HOST"), app.config.get("ELASTIC_SEARCH_DB"))

    def get_by_doi(doi):
        query = {
            "query" : {
                "term" : {
                    "bibjson.identifier.id.exact" : doi
                }
            }
        }
        resp = esprit.raw.search(conn, "article", query=query)
        res = esprit.raw.unpack_result(resp)
        if len(res) == 0:
            print "Unable to find DOAJ item for DOI " + doi
            return None
        elif len(res) > 1:
            print "Multiple DOAJ items with DOI " + doi
            return None
        return res[0]

    def doaj_callback(state):
        success = state.flush_success()
        if len(success) > 0:
            print "Writing", len(success), "detected licences to DOAJ ..."
        for s in success:
            # first locate the id of the doaj object to be updated
            doajobj = None
            for i in s.get("identifier", [{}]):
                if i.get("type") == "doi":
                    doajobj = get_by_doi(i.get("id"))
                    break
            if doajobj is None:
                print "could not write result to DOAJ"
                continue

            # add the licence information
            bibjson = doajobj.get("bibjson")
            if bibjson is None:
                print "no bibjson record"
                continue
            bibjson["license"] = s.get("license")

            # now save the record
            esprit.raw.store(conn, "article", doajobj, doajobj.get("id"))
        if len(success) > 0:
            print "Complete"

        errors = state.flush_error()
        if len(errors) > 0:
            print "Writing", len(errors), "errors to CSV ...",
            sys.stdout.flush()
        with open("oag_error.csv", "a") as f:
            writer = csv.writer(f)
            for e in errors:
                identifier = e.get("identifier", [{}]).get("id")
                msg = e.get("error")
                csv_row = [identifier, msg]
                clean_row = [unicode(c).encode("utf8", "replace") if c is not None else "" for c in csv_row]
                writer.writerow(clean_row)
        if len(success) > 0:
            print "Complete"

    return doaj_callback

if __name__ == "__main__":
    runner = JobRunner(app.config.get("OAG_LOOKUP_URL"), callback=doaj_closure())
    runner.run()