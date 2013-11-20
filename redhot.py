from datetime import datetime, timedelta
from dateutil.rrule import *

#stat.name.YEAR.DAY.HOUR.MINUTE
BASE_KEY = "{key}.{d.year}.{d.month}.{d.day}.{d.hour}.{d.minute}"

# Averages a list
AVERAGE_LI = lambda l: reduce(lambda x, y: x + y, l) / float(len(l))

def util_get_keys(key, dt):
    """
    Returns a list of recursive date formated keys based on BASE_KEY.
    """
    res = []
    key = BASE_KEY.format(key=key, d=dt).split(".")
    for i in range(0, len(key)):
        res.append('.'.join(key[:i+1]))
    return res


class Graph(object):
    def __init__(self, name, formatter=float, parent=None, redis=None):
        self.name = name
        self.formatter = formatter
        self.parent = parent

        self.key = self.parent.key % self.name if self.parent else "graph.%s" % (name+".%s")
        self.redis = self.parent.redis if self.parent else redis

        # Debug Stuff
        self.debug = False
        self._dq = []

    def incr(self, value):
        return self.incr_at(datetime.now(), value)

    def set(self, value):
        return self.set_at(datetime.now(), value)

    def clear(self):
        for key in self.redis.keys(self.key+"*"):
            self.redis.delete(key)

    def set_at(self, time, value):
        for key in util_get_keys(self.key, time):
            if self.debug: self._dq.append(("set", key, value))
            self.redis.set(key, value)

    def get_at(self, time):
        key = util_get_keys(self.key, time)[-1]
        if self.debug: self._dq.append(("get", key))
        return self.formatter(self.redis.get(key) or 0.0)

    def incr_at(self, time, value):
        for key in util_get_keys(self.key, time):
            if self.debug: self._dq.append(("incr", key, value))
            self.red.incr(key, value)

    # TODO this should allow for more control over by (aka resolution)
    def util_generate_graph(self, graph_type, start=None):
        by, keyid, count = None, 0, 0
        if graph_type == "halfhour":
            by = MINUTELY
            keyid = -1
            count = 30
            start = start+timedelta(minutes=-29)
        elif graph_type == "hour":
            by = MINUTELY
            keyid = -1
            count = 30
            start = start+timedelta(minutes=-59)
        elif graph_type == "day":
            by = HOURLY
            keyid = -2
            count = 24
            start = start+timedelta(hours=-23)
        elif graph_type == "week":
            by = DAILY
            keyid = -3
            count = 7
            start = start+timedelta(days=-6)
        elif graph_type == "month":
            by = DAILY
            keyid = -3
            count = 30
            start = start+timedelta(days=-29)
        else:
            raise ValueError("Graph")
        return by, keyid, count, start

    def graph(self, graph_type, start=None):
        b, k, c, s = self.util_generate_graph(graph_type, start or datetime.now())
        result = []
        for time in rrule(b, count=c, dtstart=s):
            keys = get_keys(self.key, time)
            result.append([dt, self.formatter(self.redis.get(keys))])
        return result
