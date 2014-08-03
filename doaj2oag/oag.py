from datetime import datetime, timedelta
import json, requests, time, csv
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

    def _backoff(self, times):
        now = datetime.now()
        seconds = 2**times * self.back_off_factor
        seconds = seconds if seconds < self.max_back_off else self.max_back_off
        return now + timedelta(seconds=seconds)


class OAGClient(object):
    def __init__(self, lookup_url):
        self.lookup_url = lookup_url

    def cycle(self, state, verbose=False):
        due = state.get_due()
        batches = self._batch(due, state.batch_size)
        if verbose:
            print str(len(due)) + " due; requesting in " + str(len(batches)) + " batches"
        for batch in batches:
            result = self._query(batch)
            state.record_result(result)
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
            verbose=True, throttle=5, callback=None):
    state = RequestState(identifiers, timeout=timeout, back_off_factor=back_off_factor, max_back_off=max_back_off, max_retries=max_retries, batch_size=batch_size)
    client = OAGClient(lookup_url)

    if verbose:
        print "Making requests to " + lookup_url + " for " + str(len(identifiers)) + " identifiers"
        print state.print_parameters()
        print state.print_status_report()

    first = True
    while not state.finished():
        if first:
            first = False
        else:
            time.sleep(throttle)
        client.cycle(state, verbose)
        if verbose:
            print state.print_status_report()
        if callback is not None:
            callback(state)