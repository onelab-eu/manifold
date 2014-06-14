# -*- coding: utf-8 -*-

class SlotMixin(object):
    def __init__(self):
        # XXX How to force operators to implement MiXins
        pass#raise Exception("Operator %r is not defining a Mixin" % self.__class__.__name__)

class BaseSlotMixin(SlotMixin):
    def __init__(self):
        self._slot_dict = {}
    
    def _set(self, slot_id, producer = None, data = None, cascade = True):
        if slot_id in self._slot_dict:
            prev_producer, prev_data = self._slot_dict[slot_id]
            if prev_producer and prev_producer != producer:
                prev_producer.del_consumer(self, cascade = False)
        self._slot_dict[slot_id] = (producer, data)
        if producer:
            producer.add_consumer(self, cascade = False)

    def _set_producer(self, slot_id, producer, cascade = True):
        prev_producer, prev_data = self._slot_dict[slot_id]
        if prev_producer and prev_producer != producer:
            prev_producer.del_consumer(self, cascade = False)
        self._slot_dict[slot_id] = (producer, prev_data)
        if producer:
            producer.add_consumer(self, cascade = False)
        
    def _get_data(self, slot_id):
        _, data = self._slot_dict[slot_id]
        return data

    def _set_data(self, slot_id, data):
        prev_producer, prev_data = self._slot_dict[slot_id]
        self._slot_dict[slot_id] = (prev_producer, data)

    def _get(self, slot_id, get_data):
        producer, data = self._slot_dict[slot_id]
        return (producer, data) if get_data else producer

    def _update_producer(self, slot_id, function):
        producer, data = self._get(slot_id, get_data = True)
        self._set_producer(slot_id, function(producer, data))

    def _clear(self):
        for slot_id, (prev_producer, prev_data) in self._slot_dict.items():
            prev_producer.del_consumer(self, cascade=True)
            self._set(slot_id)

    def _iter_slots(self):
        for producer, data in self._slot_dict.values():
            if not producer:
                continue
            yield producer, data
        

LEFT_SLOT   = 0
RIGHT_SLOT  = 1

class LeftRightSlotMixin(BaseSlotMixin):

    def __init__(self):
        BaseSlotMixin.__init__(self)
        self._set(LEFT_SLOT)
        self._set(RIGHT_SLOT)

    def _get_left(self, get_data = False):
        return self._get(LEFT_SLOT, get_data)

    def _set_left(self, producer, data = None, cascade = True):
        self._set(LEFT_SLOT, producer, data, cascade)

    def _set_left_producer(self, producer, cascade = True):
        self._set_producer(LEFT_SLOT, producer, cascade)

    def _set_left_data(self, data):
        self._set_data(LEFT_SLOT, data)

    def _update_left_producer(self, function):
        self._update_producer(LEFT_SLOT, function)

    def _get_right(self, get_data = False):
        return self._get(RIGHT_SLOT, get_data)

    def _set_right(self, producer, data = None, cascade = True):
        self._set(RIGHT_SLOT, producer, data, cascade)

    def _set_right_producer(self, producer, cascade = True):
        self._set_producer(RIGHT_SLOT, producer, cascade)

    def _set_right_data(self, data):
        self._set_data(RIGHT_SLOT, data)

    def _update_right_producer(self, function):
        self._update_producer(RIGHT_SLOT, function)

PARENT = 0

DUMMY_STRING = 'xyzzy'

class ChildrenSlotMixin(BaseSlotMixin):
    def __init__(self):
        BaseSlotMixin.__init__(self)
        self._next_child_id = 1

    def _set_child(self, producer = None, data = None, child_id = None, cascade = True):
        if not child_id:
            child_id = '%s-%s' % (DUMMY_STRING, self._next_child_id)
            self._next_child_id += 1
        # XXX ensure uniqueness
        self._set(child_id, producer, data, cascade)

    def _get_child(self, child_id, get_data = False):
        return self._get(child_id, get_data)

    def _get_child_data(self, child_id):
        return self._get_data(child_id)

    def _update_child_data(self, child_id, function):
        self._set_data(child_id, function(self._get_data(child_id)))

    def _iter_children(self):
        for child_id, (child_producer, child_data) in self._slot_dict.items():
            if child_id == PARENT:
                continue
            yield (child_id, child_producer, child_data)

    def _iter_children_ids(self):
        for child_id in self._slot_dict.keys():
            if child_id == PARENT:
                continue
            yield child_id

    def _update_children_producers(self, function):
        for child_id in self._iter_children_ids():
            self._update_producer(child_id, function)

    def _get_num_children(self):
        return len(self._slot_dict.keys())

    def _get_source_child_id(self, packet):
        source = packet.get_source()
        for id, (producer, data) in self._slot_dict.iteritems():
            if producer == source:
                return id
        return None

    def _get_children(self, get_data = False):
        children = []
        for id, producer, data in self._iter_children():
            if get_data:
                children.append((producer, data))
            else:
                children.append(producer)
        return children

class ParentChildrenSlotMixin(ChildrenSlotMixin):

    def __init__(self):
        ChildrenSlotMixin.__init__(self)
        self._set_parent()

        # We might want to store status and results for each slot

    def _get_parent(self, get_data = False):
        return self._get(PARENT, get_data)

    def _set_parent(self, producer = None, data = None, cascade = True):
        self._set(PARENT, producer, data, cascade)

    def _update_parent_producer(self, function):
        self._update_producer(PARENT, function)

    def _get_num_children(self):
        return len(self._slot_dict.keys()) - 1
        

        

CHILD = 0

class ChildSlotMixin(BaseSlotMixin):
    def __init__(self):
        BaseSlotMixin.__init__(self)
        self._set(CHILD)

    def _get_child(self, get_data = False):
        return self._get(CHILD, get_data)

    def _set_child(self, producer, data = None, cascade = True):
        self._set(CHILD, producer, data, cascade)

    def _update_child(self, function):
        return self._update_producer(CHILD, function)
