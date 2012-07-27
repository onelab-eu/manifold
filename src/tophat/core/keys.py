# $Id: Keys.py 14587 2009-07-19 13:18:50Z thierry $
# $URL: http://svn.planet-lab.org/svn/PLCAPI/tags/PLCAPI-4.3-29/PLC/Keys.py $
import re

from tophat.util.faults import *
from tophat.util.parameter import Parameter
from tophat.core.filter import Filter
from tophat.core.debug import profile
from tophat.core.table import Row, Table
from tophat.core.keytypes import KeyType, KeyTypes

class Key(Row):
    """
    Representation of a row in the keys table. To use, instantiate with a 
    dict of values. Update as you would a dict. Commit to the database 
    with sync().
    """

    table_name = 'keys'
    primary_key = 'key_id'
    join_tables = ['person_key', 'peer_key']
    fields = {
        'key_id': Parameter(int, "Key identifier"),
        'key_type': Parameter(str, "Key type"),
        'key': Parameter(str, "Key value", max = 4096),
        'person_id': Parameter(int, "User to which this key belongs", nullok = True),
        'peer_id': Parameter(int, "Peer to which this key belongs", nullok = True),
        'peer_key_id': Parameter(int, "Foreign key identifier at peer", nullok = True),
        }

    def validate_key_type(self, key_type):
        key_types = [row['key_type'] for row in KeyTypes(self.api)]
        if key_type not in key_types:
            raise PLCInvalidArgument, "Invalid key type"
	return key_type

    def validate_key(self, key):
	# Key must not be blacklisted
	rows = self.api.db.selectall("SELECT 1 from keys" \
				     " WHERE key = %(key)s" \
                                     " AND is_blacklisted IS True",
                                     locals())
	if rows:
            raise PLCInvalidArgument, "Key is blacklisted and cannot be used"

	return key

    def validate(self):
        # Basic validation
        Row.validate(self)

        assert 'key' in self
        key = self['key']

        if self['key_type'] == 'ssh':
            # Accept only SSH version 2 keys without options. From
            # sshd(8):
            #
            # Each protocol version 2 public key consists of: options,
            # keytype, base64 encoded key, comment.  The options field
            # is optional...The comment field is not used for anything
            # (but may be convenient for the user to identify the
            # key). For protocol version 2 the keytype is ``ssh-dss''
            # or ``ssh-rsa''.

            good_ssh_key = r'^.*(?:ssh-dss|ssh-rsa)[ ]+[A-Za-z0-9+/=]+(?: .*)?$'
            if not re.match(good_ssh_key, key, re.IGNORECASE):
                raise PLCInvalidArgument, "Invalid SSH version 2 public key"

    def blacklist(self, commit = True):
        """
	Permanently blacklist key (and all other identical keys),
	preventing it from ever being added again. Because this could
	affect multiple keys associated with multiple accounts, it
	should be admin only.        
	"""

	assert 'key_id' in self
        assert 'key' in self

        # Get all matching keys
        rows = self.api.db.selectall("SELECT key_id FROM keys WHERE key = %(key)s",
                                     self)
        key_ids = [row['key_id'] for row in rows]
        assert key_ids
        assert self['key_id'] in key_ids

        # Keep the keys in the table
        self.api.db.do("UPDATE keys SET is_blacklisted = True" \
                       " WHERE key_id IN (%s)" % ", ".join(map(str, key_ids)))

	# But disassociate them from all join tables
        for table in self.join_tables:
            self.api.db.do("DELETE FROM %s WHERE key_id IN (%s)" % \
                           (table, ", ".join(map(str, key_ids))))

        if commit:
            self.api.db.commit()

class Keys(Table):
    """
    Representation of row(s) from the keys table in the
    database.
    """

    def __init__(self, api, key_filter = None, columns = None):
        Table.__init__(self, api, Key, columns)
	
	sql = "SELECT %s FROM view_keys WHERE is_blacklisted IS False" % \
              ", ".join(self.columns)

        if key_filter is not None:
            if isinstance(key_filter, (list, tuple, set)):
                key_filter = Filter(Key.fields, {'key_id': key_filter})
            elif isinstance(key_filter, dict):
                key_filter = Filter(Key.fields, key_filter)
            sql += " AND (%s) %s" % key_filter.sql(api)

	self.selectall(sql)
