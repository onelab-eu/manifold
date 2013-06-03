from datetime import datetime

oldint = int

class int(oldint):
    __typename__ = 'int'
