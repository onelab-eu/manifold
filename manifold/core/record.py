# Currently, records are dictionary, but we will need a special class for typed
# operations, and faster processing. Hence, consider the current code as a
# transition towards this new class.

from types import StringTypes

class Record(object):

    @classmethod
    def get_value(self, record, key):
        """
        \param record
        \param key (a field name, or a set of field names)
        \return single value, or a tuple of values
        """
        if isinstance(key, StringTypes):
            return record[key]
        else:
            return tuple(map(lambda x: record[x], key))

    @classmethod
    def from_key_value(self, key, value):
        if isinstance(key, StringTypes):
            return { key: value }
        else:
            return dict(izip(key, value))

    @classmethod
    def has_fields(self, record, fields):
        if isinstance(fields, StringTypes):
            return fields in record
        else:
            return fields <= set(record.keys())

