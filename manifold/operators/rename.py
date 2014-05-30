from types                import StringTypes
from manifold.core.record import Record, Records
from manifold.operators   import Node
from manifold.util.type   import returns
from manifold.util.log    import Log

FIELD_SEPARATOR = '.'
DUMPSTR_RENAME = "RENAME %r" 

#------------------------------------------------------------------
# RENAME node
#------------------------------------------------------------------

#NEW|def do_rename(record, aliases):
#NEW|    """ 
#NEW|    This function modifies the record packet in place.
#NEW|
#NEW|    NOTES:
#NEW|     . It might be better to iterate on the record fields
#NEW|     . It seems we only handle two levels of hierarchy. This is okay I think
#NEW|    since in the query plan, further layers will be broken down across
#NEW|    several subqueries.
#NEW|    """
#NEW|
#NEW|    if record.is_empty():
#NEW|        return record
#NEW|
#NEW|    def collect(key, record):
#NEW|        if isinstance(subrecord, Records):
#NEW|            # 1..N
#NEW|            return [collect(key, r) for r in record]
#NEW|        elif isinstance(subrecord, Record):
#NEW|            key_head, _, key_tail = key.partition(FIELD_SEPARATOR)
#NEW|            return collect(key_tail, record[key_head])
#NEW|            # 1..1
#NEW|        else:
#NEW|            assert not key, "Field not found"
#NEW|            return record[key_head]
#NEW|
#NEW|    def handle_record(k, v, myrecord, data = None):
#NEW|        """
#NEW|        Convert the field name from k to v in myrecord. k and v will eventually
#NEW|        have several dots.
#NEW|        . cases when field length are not of same length are not handled
#NEW|        """
#NEW|        k_head, _, k_tail = k.partition(FIELD_SEPARATOR)
#NEW|        v_head, _, v_tail = v.partition(FIELD_SEPARATOR)
#NEW|
#NEW|        if k_tail and v_tail:
#NEW|            if not k_head in myrecord:
#NEW|                return
#NEW|
#NEW|            if k_head != v_head:
#NEW|                myrecord[v_head] = myrecord.pop(k_head)
#NEW|
#NEW|            subrecord = myrecord[v_tail]
#NEW|            
#NEW|            if isinstance(subrecord, Records):
#NEW|                # 1..N
#NEW|                for _myrecord in subrecord:
#NEW|                    handle_record(k_tail, v_tail, _myrecord)
#NEW|            elif isinstance(subrecord, Record):
#NEW|                # 1..1
#NEW|                handle_record(k_tail, v_tail, subrecord)
#NEW|            else:
#NEW|                return
#NEW|
#NEW|        elif not k_tail and not v_tail:
#NEW|            # XXX Maybe such cases should never be reached.
#NEW|            if k_head and k_head != v_head:
#NEW|                myrecord[v_head] = myrecord.pop(k_head)
#NEW|            else:
#NEW|                myrecord[v_head] = data
#NEW|
#NEW|        else:
#NEW|            # We have either ktail or vtail"
#NEW|            if k_tail: # and not v_tail
#NEW|                # We will gather everything and put it in v_head
#NEW|                my_record[v_head] = collect(k_tail, my_record[v_head]) 
#NEW|
#NEW|            else: # v_tail and not k_tail
#NEW|                # We have some data in subrecord, that needs to be affected to
#NEW|                # some dictionaries whose key sequence is specified in v_tail.
#NEW|                # This should allow a single level of indirection.
#NEW|                # 
#NEW|                # for example: users = [A, B, C]   =>    users = [{user_hrn: A}, {...}, {...}]
#NEW|                data = myrecord[v_head]
#NEW|                # eg. data = [A, B, C]
#NEW|
#NEW|                if isinstance(data, Records):
#NEW|                    raise Exception, "Not implemented"
#NEW|                elif isinstance(data, Record):
#NEW|                    raise Exception, "Not implemented"
#NEW|                elif isinstance(data, list):
#NEW|                    myrecord[v_head] = list()
#NEW|                    for element in data:
#NEW|                        myrecord[v_head].append({v_tail: element})
#NEW|                else:
#NEW|                    raise Exception, "Not implemented"
#NEW|
#NEW|    for k, v in aliases.items():
#NEW|        handle_record(k, v, record)
#NEW|
#NEW|    return record


def do_rename(record, map_fields):
    for k, v in map_fields.items():
        if k in record:
            tmp = record.pop(k)
            if '.' in v: # users.hrn
                method, key = v.split('.')

                if not method in record:
                    record[method] = []
                # ROUTERV2
                if isinstance(tmp, StringTypes):
                    record[method] = {key: tmp}

                # XXX WARNING: Not sure if this doesn't have side effects !!!
                elif tmp is not None:
                    for x in tmp:
                        record[method].append({key: x})        
                else:
                    Log.tmp("This record has a tmp None record = %s , tmp = %s , v = %s" % (record,tmp,v))
            else:
                record[v] = tmp
    return record

class Rename(Node):
    """
    RENAME operator node (cf SELECT clause in SQL)
    """

    def __init__(self, child, map_fields):
        """
        \brief Constructor
        """
        super(Rename, self).__init__()
        self.child, self.map_fields = child, map_fields

        # Callbacks
        old_cb = child.get_callback()
        child.set_callback(self.child_callback)
        self.set_callback(old_cb)

        self.query = self.child.get_query().copy()
        # XXX Need to rename some fields !!

    @returns(dict)
    def get_map_fields(self):
        """
        \returns The list of Field instances selected in this node.
        """
        return self.map_fields

    def get_child(self):
        """
        \return A Node instance (the child Node) of this Demux instance.
        """
        return self.child

    def dump(self, indent=0):
        """
        \brief Dump the current node
        \param indent current indentation
        """
        return "%s\n%s" % (
            Node.dump(self, indent),
            self.child.dump(indent+1),
        )

    def __repr__(self):
        return DUMPSTR_RENAME % self.get_map_fields()

    def start(self):
        """
        \brief Propagates a START message through the node
        """
        self.child.start()

    def child_callback(self, record):
        """
        \brief Processes records received by the child node
        \param record dictionary representing the received record
        """
        if record.is_last():
            self.send(record)
            return

        record = do_rename(record, self.map_fields)
        self.send(record)

    def optimize_selection(self, filter):
        self.child = self.child.optimize_selection(filter)
        return self

    def optimize_projection(self, fields):
        rmap = { v : k for k, v in self.map_fields.items() }
        new_fields = set()
        for field in fields:
            new_fields.add(rmap.get(field, field))
        self.child = self.child.optimize_projection(new_fields)
        return self
