from manifold.operators.From import From

DUMPSTR_FROMTABLE  = "SELECT %s FROM [%r, ...]" 

class FromTable(From):
    """
    \brief FROM node querying a list of records
    """

    def __init__(self, query, records, key):
        """
        \brief Constructor
        \param query A Query instance
        \param records A list of records (dictionnary instances)
        """
        assert isinstance(query,   Query), "Invalid query = %r (%r)"   % (query,   type(query))
        assert isinstance(records, list),  "Invalid records = %r (%r)" % (records, type(records))

        self.records, self.key = records, key
        super(FromTable, self).__init__(None, query, Capabilities(), key)

    def __repr__(self, indent = 0):
        return DUMPSTR_FROMTABLE % (
            ', '.join(self.get_query().get_select()),
            self.records[0]
        )

    def start(self):
        """
        \brief Propagates a START message through the node
        """
        for record in self.records:
            if not isinstance(record, dict):
                record = {self.key: record}
            self.send(record)
        self.send(LAST_RECORD)
