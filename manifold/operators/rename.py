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

def do_rename(record, aliases):
    """ 
    This function modifies the record packet in place.

    NOTES:
     . It might be better to iterate on the record fields
     . It seems we only handle two levels of hierarchy. This is okay I think
    since in the query plan, further layers will be broken down across
    several subqueries.
    """

    print "*" * 80
    print "DO RENAME", record
    print "aliases", aliases
    print "-" * 80
    if record.is_empty():
        return record

    def collect(key, record):
        if isinstance(record, (list, Records)):
            # 1..N
            return [collect(key, r) for r in record]
        elif isinstance(record, Record):
            key_head, _, key_tail = key.partition(FIELD_SEPARATOR)
            return collect(key_tail, record[key_head])
            # 1..1
        else:
            assert not key, "Field not found"
            return record

    def handle_record(k, v, myrecord, data = None):
        """
        Convert the field name from k to v in myrecord. k and v will eventually
        have several dots.
        . cases when field length are not of same length are not handled
        """
        k_head, _, k_tail = k.partition(FIELD_SEPARATOR)
        v_head, _, v_tail = v.partition(FIELD_SEPARATOR)

        if not k_head in myrecord:
            return

        if k_tail and v_tail:

            if k_head != v_head:
                myrecord[v_head] = myrecord.pop(k_head)

            subrecord = myrecord[v_tail]
            
            if isinstance(subrecord, Records):
                # 1..N
                for _myrecord in subrecord:
                    handle_record(k_tail, v_tail, _myrecord)
            elif isinstance(subrecord, Record):
                # 1..1
                handle_record(k_tail, v_tail, subrecord)
            else:
                return

        elif not k_tail and not v_tail:
            # XXX Maybe such cases should never be reached.
            if k_head:# and k_head != v_head:
                myrecord[v_head] = myrecord.pop(k_head)
            else:
                myrecord[v_head] = data

        else:
            # We have either ktail or vtail"
            if k_tail: # and not v_tail
                # We will gather everything and put it in v_head
                myrecord[k_head] = collect(k_tail, myrecord[k_head]) 

            else: # v_tail and not k_tail
                # We have some data in subrecord, that needs to be affected to
                # some dictionaries whose key sequence is specified in v_tail.
                # This should allow a single level of indirection.
                # 
                # for example: users = [A, B, C]   =>    users = [{user_hrn: A}, {...}, {...}]
                data = myrecord[v_head]
                # eg. data = [A, B, C]

                if isinstance(data, Records):
                    raise Exception, "Not implemented"
                elif isinstance(data, Record):
                    raise Exception, "Not implemented"
                elif isinstance(data, list):
                    myrecord[v_head] = list()
                    for element in data:
                        myrecord[v_head].append({v_tail: element})
                else:
                    raise Exception, "Not implemented"

    for k, v in aliases.items():
        # Rename fields in place in the record
        handle_record(k, v, record)

    print "OUTPUT", record
    return record


def do_rename_old(record, map_fields):
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
                #else:
                    #Log.tmp("This record has a tmp None record = %s , tmp = %s , v = %s" % (record,tmp,v))
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

        try:
            record = do_rename(record, self.map_fields)
        except Exception, e:
            print "EEE:", e
            import traceback
            traceback.print_exc()
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
