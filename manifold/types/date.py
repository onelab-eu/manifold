from datetime import datetime

class date(object):
    __typename__ = 'date'

    def __init__(self, value):
        self._datetime = datetime.strptime(value, "%Y-%m-%d")

    def __str__(self):
        return self._datetime

    def __repr__(self):
        return "<date: %s>" % self
