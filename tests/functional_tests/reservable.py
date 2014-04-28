#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Run all tests:
# ./nitos.py
#
# Run a single test
# ./nitos.py NITOSTests.test_list_resources
#
# NOTE:
# - This test suite could be common to several testbeds, only a few parameters differ
# - We could define a SchedulerMixin that determines whether the testbed supports leases
#
# NOT TESTED YET:
# - sfa/iotlab/iotlabshell.py:    _MINIMUM_DURATION = 10  # 10 units of granularity 60 s, 10 mins

import unittest

import time
from manifold.bin.shell     import Shell
from manifold.util.test     import ManifoldTestCase
from manifold.util.options  import Options

test_options = {
   'log_level': 'CRITICAL', # DEBUG, INFO, WARNING, ERROR, CRITICAL
   'verbosity': 2, # 0
}

class TestbedWithReservationTestCase(ManifoldTestCase):

    @classmethod
    def setUpClass(self):
        # TODO Enable NITOS Only 
        self._shell = Shell(interactive=False)
        self._shell.select_auth_method('local')

    @classmethod
    def tearDownClass(self):
        self._shell.terminate()

    def _run(self, command):
        result_value = self._shell.evaluate(command)
        records = self.assert_rv_success(result_value)
        return records

    def test_list_resources(self):
        """
        List all the resources from the testbed
        """
        Q = 'SELECT hrn, type FROM resource'
        records = self._run(Q)

        # Let's check we have the requested fields (this should not be part of this test ?)
        assert len(records) > 0
        first_record = records[0]
        assert set(first_record.keys()) == {'hrn', 'type'}

        return records
            
    def test_list_leases(self):
        """
        List all the leases from the testbed
        """
        Q = 'SELECT resource, start_time, end_time FROM lease'
        records = self._run(Q)

        # We might count the leases, add one, and check that we have one more...

    def test_list_slice_resources(self):
        Q = 'SELECT hrn FROM resource WHERE slice == "%s"' % (self.SLICE_URN, )
        records = self._run(Q)
        

    def test_list_slice_leases(self):
        Q = 'SELECT resource, start_time, end_time FROM lease WHERE slice == "%s"' % (self.SLICE_URN, )
        records = self._run(Q)

    def test_add_lease(self):
        """
        Add a lease to the slice by adding both a sliver to the node, and a lease.

        KNOWN BUGS:
        - If in metadata we don't have resource[] and lease[], which we handle
          artificially in the code, then the query plan becomes very complex,
          and has wrong rename in the path. For example it finds resource in
          lease.resource, which is true in fact, and renames it to resource.urn
          to find this default field...
        - Need to reproduce... when adding a second lease
          [Failure instance: Traceback (failure with no frames): <type
          'exceptions.ValueError'>: Error in SFA Proxy [Failure instance:
          Traceback: <class 'xmlrpclib.Fault'>: <Fault 2: "Uncaught exception
          Cannot lease 'omf.nitos.node037', because it is unavailable for the
          requested time. in method CreateSliver">
        """
        Q = 'UPDATE slice SET resource = ["%s"], lease = [{resource: "%s", start_time: "%s", end_time: "%s"}] where slice_hrn == "%s"' % (self.NODE_1, self.NODE_1, self.START, self.END, self.SLICE_HRN, )
        records = self._run(Q)

    def test_add_two_leases(self):
        """
        Same resource twice, not in resources (no need here).
        """
        Q = 'UPDATE slice SET resource = ["%s"], lease = [{resource: "%s", start_time: "%s", end_time: "%s"}, {resource: "%s", start_time: "%s", end_time: "%s"}] where slice_hrn == "%s"' % (self.NODE_1, self.NODE_1, self.START, self.END, self.NODE_1, self.START+3600, self.END+3600, self.SLICE_HRN, )
        records = self._run(Q)

    def test_add_two_leases_2(self):
        """
        Different resources, one is not in resources.
        """
        Q = 'UPDATE slice SET resource = ["%s"], lease = [{resource: "%s", start_time: "%s", end_time: "%s"}, {resource: "%s", start_time: "%s", end_time: "%s"}] where slice_hrn == "%s"' % (self.NODE_1, self.NODE_1, self.START, self.END, self.NODE_2, self.START+3600, self.END+3600, self.SLICE_HRN, )
        records = self._run(Q)

    def test_add_two_leases_3(self):
        """
        Different resources, both in resources.
        """
        Q = 'UPDATE slice SET resource = ["%s", "%s"], lease = [{resource: "%s", start_time: "%s", end_time: "%s"}, {resource: "%s", start_time: "%s", end_time: "%s"}] where slice_hrn == "%s"' % (self.NODE_1, self.NODE_2, self.NODE_1, self.START, self.END, self.NODE_2, self.START+3600, self.END+3600, self.SLICE_HRN, )
        records = self._run(Q)

    def test_clear_slice(self):
        Q = 'UPDATE slice SET resource = [], lease = [] where slice_hrn == "%s"' % (self.SLICE_HRN, )
        records = self._run(Q)

        assert len(records) == 1
        first_record = records[0]

        # XXX What shall we expect
        EXPECTED_FIELDS = {'slice_hrn', 'slice_urn', 'resource', 'lease'}
        assert set(first_record.keys()) == EXPECTED_FIELDS, "Record has keys: %r. Expected: %r" % (first_record.keys(), EXPECTED_FIELDS)

        assert len(first_record['resource']) == 0
        assert len(first_record['lease']) == 0

class NITOSTests(TestbedWithReservationTestCase):
    NAMESPACE = 'nitosb'
    SLICE_HRN = 'ple.upmc.myslicedemo'
    SLICE_URN = 'urn:publicid:IDN+ple:upmc+slice+myslicedemo'

    GRAIN = 1800
    NODE_1 = 'urn:publicid:IDN+omf:nitos+node+omf.nitos.node037'
    NODE_2 = 'urn:publicid:IDN+omf:nitos+node+omf.nitos.node036'

    t = time.time()
    START = int(t - (t % GRAIN) + GRAIN) 
    END   = START + 1800 # 30 min lease

    def test_list_resources(self):
        """
        List all the resources from the testbed
        """
        records = super(NITOSTests).test_list_resources()

        # Assert we have nodes and channels
        has_nodes    = False
        has_channels = False
        for record in records:
            if record['type'] == 'node':
                has_nodes = True
            elif record['type'] == 'channel':
                has_channels = True
        assert has_nodes and has_channels

class IoTLABTests(TestbedWithReservationTestCase):
    NAMESPACE = 'iotlab'
    SLICE_HRN = 'ple.upmc.myslicedemo'
    SLICE_URN = 'urn:publicid:IDN+ple:upmc+slice+myslicedemo'

    GRAIN = 10 * 60
    NODE_1 = 'urn:publicid:IDN+iotlab+node+a8-6.rocquencourt.iot-lab.info'
    NODE_2 = 'urn:publicid:IDN+iotlab+node+m3-379.devgrenoble.iot-lab.info'

    t = time.time()
    START = int(t - (t % GRAIN) + GRAIN) 
    END   = START + 1800 # 30 min lease

if __name__ == '__main__':
    import sys, inspect

    if sys.argv[1] == 'list':

        for class_name, class_obj in inspect.getmembers(sys.modules[__name__], inspect.isclass):
            if not class_name.endswith('Tests'):
                continue
            print "%s %s" % (sys.argv[0], class_name,)
        
            for method_name, _ in inspect.getmembers(class_obj, predicate=inspect.ismethod):
                if not method_name.startswith('test_'):
                    continue
                print "%s %s.%s" % (sys.argv[0], class_name, method_name)
        sys.exit(0)

    Options().log_level = test_options.pop('log_level')
    unittest.main(**test_options)
