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
                "bool" : {
                    "must" : [
                        {
                            "range" : {
                                "pending.due" : {
                                    "lte" : now
                                }
                            }
                        },
                        {
                            "range" : {
                                "start" : {
                                    "lte" : now
                                }
                            }
                        }
                    ]
                }
            },
            # FIXME: removed because of a bug in ES around date sorting
            # "sort" : [{"pending.due" : {"order" : "asc", "mode" : "min"}}],
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

    def job_statuses(self):
        query = {
            "query" : { "match_all" : {}},
            "fields" : ["id", "status"],
            "size" : 100
        }

        resp = esprit.raw.search(self.conn, self.index, query=query)
        results = esprit.raw.unpack_result(resp)

        statuses = {}
        for res in results:
            statuses[res.get("id")] = res.get("status")
        return statuses

    def save_job(self, state):
        print "Saving state to ElasticSearch ...",
        sys.stdout.flush()
        j = state.json()
        is_finished = state.finished()
        j["status"] = "finished" if is_finished else "active"
        esprit.raw.store(self.conn, self.index, j, j.get("id"))
        print "Complete"

    def cycle_state(self, state):
        if self.verbose:
            now = datetime.now()
            print now.strftime("%Y-%m-%d %H:%M:%S") + " Processing job " + state.id
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
        if next is not None:
            print "Next request is due at", datetime.strftime(next, "%Y-%m-%d %H:%M:%S"), "\n"
        else:
            print "JOB " + state.id + " HAS COMPLETED!!!"

    def print_job_statuses(self):
        stats = self.job_statuses()
        for id, stat in stats.iteritems():
            print id, stat

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
            else:
                print "Job Statuses"
                self.print_job_statuses()

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
        return res

    def doaj_callback(state):
        success = state.flush_success()
        if len(success) > 0:
            print "Writing", len(success), "detected licences to DOAJ ..."
        for s in success:
            # first locate the id of the doaj object to be updated
            doajobjs = []
            identifier = None
            for i in s.get("identifier", [{}]):
                if i.get("type") == "doi":
                    identifier = i.get("id")
                    doajobjs = get_by_doi(i.get("id"))
                    break
            if len(doajobjs) == 0:
                print "Unable to trace DOAJ item to write to", identifier
                continue

            # add the licence information to every record we get back
            for doajobj in doajobjs:
                bibjson = doajobj.get("bibjson")
                if bibjson is None:
                    print "no bibjson record"
                    continue
                bibjson["license"] = s.get("license")

                # now save the record
                esprit.raw.store(conn, "article", doajobj, doajobj.get("id"))

        if len(success) > 0:
            print "Finished writing to DOAJ"

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