from datetime import datetime, timedelta
import json, requests
from copy import deepcopy

class RequestState(object):
    def __init__(self, identifiers, timeout=None, back_off_factor=1, max_back_off=120, max_retries=None):
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

        for ident in identifiers:
            self.pending[ident] = {"init" : self.start, "due" : self.start, "requested" : 0, "maxed" : False}

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
        buffer = deepcopy(self.success_buffer)
        del self.success_buffer[:]
        return buffer

    def flush_error(self):
        buffer = deepcopy(self.errpr_buffer)
        del self.error_buffer[:]
        return buffer

    def _backoff(self, times):
        now = datetime.now()
        seconds = 2**times * self.back_off_factor
        seconds = seconds if seconds < self.max_back_off else self.max_back_off
        return now + timedelta(seconds=seconds)


class OAGClient(object):
    def __init__(self, lookup_url, batch_size=1000):
        self.lookup_url = lookup_url
        self.batch_size = batch_size

    def cycle(self, state):
        due = state.get_due()
        batches = self._batch(due)
        for batch in batches:
            result = self._query(batch)
            state.record_result(result)
        return state

    def _batch(self, ids):
        batches = []
        start = 0
        while True:
            batch = ids[start:start + self.batch_size]
            if len(batch) == 0: break
            batches.append(batch)
            start += self.batch_size
        return batches

    def _query(self, batch):
        data = json.dumps(batch)
        resp = requests.post(self.lookup_url, headers={'Accept':'application/json'}, data=data)
        resp.raise_for_status()
        return resp.json()

def oag_it(lookup_url, identifiers,
           timeout=None, back_off_factor=1, max_back_off=120, max_retries=None, batch_size=1000,
            callback=None):
    state = RequestState(identifiers, timeout=timeout, back_off_factor=back_off_factor, max_back_off=max_back_off, max_retries=max_retries)
    client = OAGClient(lookup_url, batch_size=batch_size)
    while not state.finished():
        client.cycle(state)
        if callback is not None:
            callback(state)