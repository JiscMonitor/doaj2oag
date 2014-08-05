from datetime import datetime, timedelta
import json, requests, time, csv, sys
from copy import deepcopy

class RequestState(object):
    def __init__(self, identifiers, timeout=None, back_off_factor=1, max_back_off=120, max_retries=None, batch_size=1000):
        self.success = {}
        self.error = {}
        self.pending = {}

        self.success_buffer = []
        self.error_buffer = []

        self.start = datetime.now()

        self.timeout = self.start + timedelta(seconds=timeout) if timeout is not None else None
        self.back_off_factor = back_off_factor
        self.max_back_off = max_back_off
        self.max_retries = max_retries
        self.batch_size = batch_size

        for ident in identifiers:
            self.pending[ident] = {"init" : self.start, "due" : self.start, "requested" : 0, "maxed" : False}

    def print_parameters(self):
        params = "Timeout: " + str(self.timeout) + "\n"
        params += "Back Off Factor: " + str(self.back_off_factor) + "\n"
        params += "Max Back Off: " + str(self.max_back_off) + "\n"
        params += "Max Tries per Identifier: " + str(self.max_retries) + "\n"
        params += "Batch Size: " + str(self.batch_size) + "\n"
        return params

    def print_status_report(self):
        status = str(len(self.success.keys())) + " received; " + str(len(self.error.keys())) + " errors; " + str(len(self.pending.keys())) + " pending"
        return status

    def finished(self):
        if len(self.pending.keys()) == 0:
            return True
        if self.timeout is not None:
            if datetime.now() > self.timeout:
                return True
        unmaxed = [p for p in self.pending.keys() if not self.pending[p].get("maxed")]
        if len(unmaxed) == 0:
            return True
        return False

    def get_due(self):
        now = datetime.now()
        return [p for p in self.pending.keys() if self.pending[p].get("due") < now and not self.pending[p].get("maxed")]

    def next_due(self):
        latest = None
        for p, o in self.pending.iteritems():
            if latest is None or o.get("due") > latest:
                latest = o.get("due")
        return latest

    def record_result(self, result):
        now = datetime.now()

        successes = result.get("results", [])
        errors = result.get("errors", [])
        processing = result.get("processing", [])

        for s in successes:
            id = s.get("identifier")[0].get("id")
            self.success[id] = deepcopy(self.pending[id])
            self.success[id]["requested"] += 1
            self.success[id]["found"] = now
            del self.success[id]["due"]
            del self.pending[id]
        self.success_buffer.extend(successes)

        for e in errors:
            id = e.get("identifier").get("id")
            self.error[id] = deepcopy(self.pending[id])
            self.error[id]["requested"] += 1
            self.error[id]["found"] = now
            del self.error[id]["due"]
            del self.pending[id]
        self.error_buffer.extend(errors)

        for p in processing:
            id = p.get("identifier").get("id")
            self.pending[id]["requested"] += 1
            self.pending[id]["due"] = self._backoff(self.pending[id]["requested"])
            if self.max_retries is not None and self.pending[id]["requested"] >= self.max_retries:
                self.pending[id]["maxed"] = True

    def flush_success(self):
        buffer = self.success_buffer
        self.success_buffer = []
        return buffer

    def flush_error(self):
        buffer = self.error_buffer
        self.error_buffer = []
        return buffer

    def json(self):
        data = {}

        data["start"] = datetime.strftime(self.start, "%Y-%m%dT%H:%M:%S.%fZ")
        if self.timeout is not None:
            data["timeout"] = datetime.strftime(self.start, "%Y-%m%dT%H:%M:%S.%fZ")
        data["back_off_factor"] = self.back_off_factor
        data["max_back_off"] = self.max_back_off
        data["batch_size"] = self.batch_size

        jsuccess = deepcopy(self.success)
        for k in jsuccess:
            jsuccess[k]["init"] = datetime.strftime(jsuccess[k]["init"], "%Y-%m%dT%H:%M:%S.%fZ")
            jsuccess[k]["found"] = datetime.strftime(jsuccess[k]["found"], "%Y-%m%dT%H:%M:%S.%fZ")

        jerror = deepcopy(self.error)
        for k in jerror:
            jerror[k]["init"] = datetime.strftime(jerror[k]["init"], "%Y-%m%dT%H:%M:%S.%fZ")
            jerror[k]["found"] = datetime.strftime(jerror[k]["found"], "%Y-%m%dT%H:%M:%S.%fZ")

        jpending = deepcopy(self.pending)
        for k in jpending:
            jpending[k]["init"] = datetime.strftime(jpending[k]["init"], "%Y-%m%dT%H:%M:%S.%fZ")
            jpending[k]["due"] = datetime.strftime(jpending[k]["due"], "%Y-%m%dT%H:%M:%S.%fZ")

        data["success"] = jsuccess
        data["error"] = jerror
        data["pending"] = jpending

        return data

    def _backoff(self, times):
        now = datetime.now()
        seconds = 2**times * self.back_off_factor
        seconds = seconds if seconds < self.max_back_off else self.max_back_off
        return now + timedelta(seconds=seconds)


class OAGClient(object):
    def __init__(self, lookup_url):
        self.lookup_url = lookup_url

    def cycle(self, state, throttle=0, verbose=False):
        due = state.get_due()
        batches = self._batch(due, state.batch_size)
        if verbose:
            print str(len(due)) + " due; requesting in " + str(len(batches)) + " batches"
        first = True
        i = 1
        for batch in batches:
            if first:
                first = False
            elif throttle > 0:
                time.sleep(throttle)
            result = self._query(batch)
            state.record_result(result)

            print i, " ",
            sys.stdout.flush()
            i += 1
        print ""
        return state

    def _batch(self, ids, batch_size=1000):
        batches = []
        start = 0
        while True:
            batch = ids[start:start + batch_size]
            if len(batch) == 0: break
            batches.append(batch)
            start += batch_size
        return batches

    def _query(self, batch):
        data = json.dumps(batch)
        resp = requests.post(self.lookup_url, headers={'Accept':'application/json'}, data=data)
        resp.raise_for_status()
        return resp.json()

def csv_closure(success_file, error_file):
    def csv_callback(state):
        successes = state.flush_success()
        errors = state.flush_error()
        with open(success_file, "a") as f:
            writer = csv.writer(f)
            for s in successes:
                identifier = s.get("identifier", [{}])[0].get("id")
                ltitle = s.get("license", [{}])[0].get("title")
                csv_row = [identifier, ltitle]
                clean_row = [unicode(c).encode("utf8", "replace") if c is not None else "" for c in csv_row]
                writer.writerow(clean_row)

        with open(error_file, "a") as f:
            writer = csv.writer(f)
            for e in errors:
                identifier = e.get("identifier", [{}]).get("id")
                msg = e.get("error")
                csv_row = [identifier, msg]
                clean_row = [unicode(c).encode("utf8", "replace") if c is not None else "" for c in csv_row]
                writer.writerow(clean_row)
    return csv_callback

def oag_it(lookup_url, identifiers,
           timeout=None, back_off_factor=1, max_back_off=120, max_retries=None, batch_size=1000,
            verbose=True, throttle=5,
            callback=None, save_state=None):
    state = RequestState(identifiers, timeout=timeout, back_off_factor=back_off_factor, max_back_off=max_back_off, max_retries=max_retries, batch_size=batch_size)
    client = OAGClient(lookup_url)

    if verbose:
        print "Making requests to " + lookup_url + " for " + str(len(identifiers)) + " identifiers"
        print state.print_parameters()
        print state.print_status_report()

    next = state.next_due()
    while True:
        # check whether we're supposed to do anything yet
        now = datetime.now()
        if now < next:
            continue

        # if a cycle is due, issue it
        client.cycle(state, throttle, verbose)
        if verbose:
            print state.print_status_report()

        # run the callback on the state
        if callback is not None:
            callback(state)

        # run the save method if there is one
        if save_state is not None:
            save_state(state)

        # if we are finished, break
        if state.finished():
            print "FINISHED"
            break

        # if we have done work here, update the next due time for the busy
        # loop aboge
        next = state.next_due()
        print "Next request is due at", datetime.strftime(next, "%Y-%m-%d %H:%M:%S")
