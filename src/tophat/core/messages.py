#
# Functions for interacting with the messages table in the database
#
# Tony Mack <tmack@cs.princeton.edu>
# Copyright (C) 2006 The Trustees of Princeton University
#
# $Id: Messages.py 14587 2009-07-19 13:18:50Z thierry $
# $URL: http://svn.planet-lab.org/svn/PLCAPI/tags/PLCAPI-4.3-29/PLC/Messages.py $
#

from tophat.util.parameter import Parameter
from tophat.core.table import Row, Table
from tophat.core.filter import Filter

class Message(Row):
    """
    Representation of a row in the messages table. 
    """
    
    table_name = 'messages'
    primary_key = 'message_id'
    fields = {
        'message_id': Parameter(str, "Message identifier"),
        'subject': Parameter(str, "Message summary", nullok = True),
        'template': Parameter(str, "Message template", nullok = True),
        'enabled': Parameter(bool, "Message is enabled"),
        }
    
class Messages(Table):
    """
    Representation of row(s) from the messages table in the database. 
    """

    def __init__(self, api, message_filter = None, columns = None, enabled = None):
        Table.__init__(self, api, Message, columns)
    
        sql = "SELECT %s from messages WHERE True" % \
              ", ".join(self.columns)

        if enabled is not None:
            sql += " AND enabled IS %s" % enabled

        if message_filter is not None:
            if isinstance(message_filter, (list, tuple, set)):
                message_filter = Filter(Message.fields, {'message_id': message_filter})
                sql += " AND (%s) %s" % message_filter.sql(api, "OR")
            elif isinstance(message_filter, dict):
                message_filter = Filter(Message.fields, message_filter)
                sql += " AND (%s) %s" % message_filter.sql(api, "AND")

        self.selectall(sql)
