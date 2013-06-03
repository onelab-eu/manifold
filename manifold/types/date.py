from datetime import datetime

class date(object):
    __typename__ = 'date'

    def __init__(self, value):
        self._datetime = datetime.strptime(value, "%Y-%m-%d")
