#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# ResultValue transports Records, code error, and eventual 
# during a QueryPlan execution.
#
# Copyright (C) UPMC Paris Universitas
# Authors:
#   Jordan Augé       <jordan.auge@lip6.fr>
#   Marc-Olivier Buob <marc-olivier.buob@lip6.fr>

# Inspired from GENI error codes

import time
import pprint
from types                      import StringTypes

from manifold.util.log          import Log
from manifold.util.type         import accepts, returns

class ResultValue(dict):

    # type
    SUCCESS     = 0
    WARNING     = 1
    ERROR       = 2

    # origin
    CORE        = 0
    GATEWAY     = 1

    # code
    SUCCESS     = 0
    SERVERBUSY  = 32001
    BADARGS     = 1
    ERROR       = 2
    FORBIDDEN   = 3
    BADVERSION  = 4
    SERVERERROR = 5
    TOOBIG      = 6
    REFUSED     = 7
    TIMEDOUT    = 8
    DBERROR     = 9
    RPCERROR    = 10

    # description
    ERRSTR = {
        SUCCESS     : 'Success',
        SERVERBUSY  : 'Server is (temporarily) too busy; try again later',
        BADARGS     : 'Bad Arguments: malformed',
        ERROR       : 'Error (other)',
        FORBIDDEN   : 'Operation Forbidden: eg supplied credentials do not provide sufficient privileges (on the given slice)',
        BADVERSION  : 'Bad Version (eg of RSpec)',
        SERVERERROR : 'Server Error',
        TOOBIG      : 'Too Big (eg request RSpec)',
        REFUSED     : 'Operation Refused',
        TIMEDOUT    : 'Operation Timed Out',
        DBERROR     : 'Database Error',
        RPCERROR    : ''
    }

    ALLOWED_FIELDS = set(['origin', 'type', 'code', 'value', 'description', 'traceback', 'ts'])

    def __init__(self, *args, **kwargs):
        """
        Constructor.
        Args:
            origin: A value among:
                ResultValue.CORE    : iif the error is raised by manifold/core/*
                ResultValue.GATEWAY : iif the error is raised by manifold/gateways/*
            type: An integer describing the status of the query. 
                ResultValue.SUCCESS : the Query has succeeded.
                ResultValue.WARNING : the Query results are not complete.
                ResultValue.ERROR   : the Query has failed.
            code: An integer value.
            value: A list of ResultValue in case of failure or A list of Records in case of success.
            description:
            traceback: A String containing the traceback.
            ts: A String containing the date when this Query has been issued. By default, this
                value is set to the current date.
        """ 
        
        # Checks
        if args:
            if kwargs:
                raise Exception, "Bad initialization for ResultValue"

            if len(args) == 1 and isinstance(args[0], dict):
                kwargs = args[0]
            
        given = set(kwargs.keys())
        cstr_success = set(['code', 'origin', 'value']) <= given
        cstr_error   = set(['code', 'type', 'origin', 'description']) <= given
        assert given <= self.ALLOWED_FIELDS, "Wrong fields in ResultValue constructor: %r" % (given - self.ALLOWED_FIELDS)
        assert cstr_success or cstr_error, 'Incomplete set of fields in ResultValue constructor: %r' % given
        
        dict.__init__(self, **kwargs)

        # Set missing fields to None
        for field in self.ALLOWED_FIELDS - given:
            self[field] = None
        if not 'ts' in self:
            self['ts'] = time.time()
            

    # Internal MySlice errors   : return ERROR
    # Internal MySlice warnings : return RESULT WITH WARNINGS
    # Debug                     : add DEBUG INFORMATION
    # Gateway errors            : return RESULT WITH WARNING
    # all Gateways errors       : return ERROR
    
    @classmethod
    #@returns(ResultValue)
    def get_result_value(self, results, result_values):
        """
        Craft a ResultValue instance according to a list of Records and
        an optionnal list of ResultValues retrieved during the QueryPlan
        execution.
        Args:
            results: A list of Records 
            result_values: A list of ResultValue instances.
        """
        # let's analyze the results of the query plan
        # XXX we should inspect all errors to determine whether to return a
        # result or not
        
        if not result_values:
            # No error
            return ResultValue(code = self.SUCCESS, origin = [self.CORE, 0], value = results)
        else:
            # Handle errors
            return ResultValue(code = self.WARNING, origin = [self.CORE, 0], value = results, description = result_values)

    @classmethod
    #@returns(ResultValue)
    def get_error(self, error, description=None):
        if not description:
            description = self.ERRSTR[error]
        return ResultValue(code=self.ERROR, type=error, origin=[self.CORE, 0], description=description)

    @returns(bool)
    def is_success(self):
        return self["code"] == self.SUCCESS

    @classmethod
    #@returns(ResultValue)
    def get_success(self, result):
        return ResultValue(
            code      = self.SUCCESS,
            origin    = [self.CORE, 0],
            value     = result
        )

    def ok_value(self):
        return self['value']

    def get_all(self):
        if not self.is_success():
            raise Exception, "Error executing query: %s" % e
        return self.ok_value()

    def get_one(self):
        records = self.get_all()
        if len(records) > 1:
            raise Exception, "More than 1 record"
        return records[0]

    @returns(StringTypes)
    def get_error_message(self):
        return "%r" % self["description"]

    @staticmethod
    def to_html(raw_dict):
        return pprint.pformat(raw_dict).replace("\\n","<br/>")

    def to_dict(self):
        return dict(self)

# 67    <code>
# 68      <value>9</value>
# 69      <label>DBERROR</label>
# 70      <description>Database Error</description>
# 71    </code>
# 72    <code>
# 73      <value>10</value>
# 74      <label>RPCERROR</label>
# 75      <description>RPC Error</description>
# 76    </code>
# 77    <code>
# 78      <value>11</value>
# 79      <label>UNAVAILABLE</label>
# 80      <description>Unavailable (eg server in lockdown)</description>
# 81    </code>
# 82    <code>
# 83      <value>12</value>
# 84      <label>SEARCHFAILED</label>
# 85      <description>Search Failed (eg for slice)</description>
# 86    </code>
# 87    <code>
# 88      <value>13</value>
# 89      <label>UNSUPPORTED</label>
# 90      <description>Operation Unsupported</description>
# 91    </code>
# 92    <code>
# 93      <value>14</value>
# 94      <label>BUSY</label>
# 95      <description>Busy (resource, slice, or server); try again
# later</description>
# 96    </code>
# 97    <code>
# 98      <value>15</value>
# 99      <label>EXPIRED</label>
# 100     <description>Expired (eg slice)</description>
# 101   </code>
# 102   <code>
# 103     <value>16</value>
# 104     <label>INPROGRESS</label>
# 105     <description>In Progress</description>
# 106   </code>
# 107   <code>
# 108     <value>17</value>
# 109     <label>ALREADYEXISTS</label>
# 110     <description>Already Exists (eg slice)</description>
# 111   </code>
# 112 <!-- 18+ not in original ProtoGENI implementation or Error Code --
# 113   -- proposal. -->
# 114   <code>
# 115     <value>18</value>
# 116     <label>MISSINGARGS</label>
# 117     <description>Required argument(s) missing</description>
# 118   </code>
# 119   <code>
# 120     <value>19</value>
# 121     <label>OUTOFRANGE</label>
# 122     <description>Input Argument outside of legal range</description>
# 123   </code>
# 124   <code>
# 125     <value>20</value>
# 126     <label>CREDENTIAL_INVALID</label>
# 127     <description>Not authorized: Supplied credential is
# invalid</description>
# 128   </code>
# 129   <code>
# 130     <value>21</value>
# 131     <label>CREDENTIAL_EXPIRED</label>
# 132     <description>Not authorized: Supplied credential expired</description>
# 133   </code>
# 134   <code>
# 135     <value>22</value>
# 136     <label>CREDENTIAL_MISMATCH</label>
# 137     <description>Not authorized: Supplied credential does not match client
# certificate or does not match the given slice URN</description>
# 138   </code>
# 139   <code>
# 140     <value>23</value>
# 141     <label>CREDENTIAL_SIGNER_UNTRUSTED</label>
# 142     <description>Not authorized: Supplied credential not signed by a trusted
# authority</description>
# 143   </code>
# 144   <code>
# 145     <value>24</value>
# 146     <label>VLAN_UNAVAILABLE</label>
# 147     <description>VLAN tag(s) requested not available (likely stitching
# failure)</description>
# 148   </code>
# 149 </geni-error-codes>
# 150 
# <!--
# || 0    || SUCCESS      || "Success" ||
# || 1    || BADARGS      || "Bad Arguments: malformed arguments" ||
# || 2    || ERROR        || "Error (other)" ||
# || 3    || FORBIDDEN    || "Operation Forbidden: eg supplied credentials do # not provide sufficient privileges (on given slice)" ||
# || 4    || BADVERSION   || "Bad Version (eg of RSpec)" ||
# || 5    || SERVERERROR  || "Server Error" ||
# || 6    || TOOBIG       || "Too Big (eg request RSpec)" ||
# || 7    || REFUSED      || "Operation Refused" ||
# || 8    || TIMEDOUT     || "Operation Timed Out" ||
# || 9    || DBERROR      || "Database Error" ||
# || 10   || RPCERROR     || "RPC Error" ||
# || 11   || UNAVAILABLE  || "Unavailable (eg server in lockdown)" ||
# || 12   || SEARCHFAILED         || "Search Failed (eg for slice)" ||
# || 13   || UNSUPPORTED  || "Operation Unsupported" ||
# || 14   || BUSY         || "Busy (resource, slice, or server); try again # later" ||
# || 15   || EXPIRED      || "Expired (eg slice)" ||
# || 16   || INPROGRESS   || "In Progress" ||
# || 17   || ALREADYEXISTS        || "Already Exists (eg the slice}" ||
# || 18   || MISSINGARGS  || "Required argument(s) missing" ||
# || 19   || OUTOFRANGE   || "Requested expiration time or other argument not # valid" ||
# || 20   || CREDENTIAL_INVALID   || "Not authorized: Supplied credential is # invalid" ||
# || 21   || CREDENTIAL_EXPIRED   || "Not authorized: Supplied credential # expired" ||
# || 22   || CREDENTIAL_MISMATCH   || "Not authorized: Supplied credential # does not match the supplied client certificate or does not match the given slice # URN" ||
# || 23   || CREDENTIAL_SIGNER_UNTRUSTED   || "Not authorized: Supplied # credential not signed by trusted authority" ||
# || 24   || VLAN_UNAVAILABLE     || "VLAN tag(s) requested not available # (likely stitching failure)" ||
# 
# 18+ not in original ProtoGENI implementation or Error Code proposal.
# 
# Maping to SFA Faults:
# SfaAuthenticationFailure: FORBIDDEN
# SfaDBErrr: DBERROR
# SfaFault: ERROR
# SfaPermissionDenied: FORBIDDEN
# SfaNotImplemented: UNSUPPORTED
# SfaAPIError: SERVERERROR
# MalformedHrnException: BADARGS
# NonExistingRecord: SEARCHFAILED
# ExistingRecord: ALREADYEXISTS
# NonexistingCredType: SEARCHFAILED? FORBIDDEN? CREDENTIAL_INVALID?
# NonexitingFile: SEARCHFAILED
# InvalidRPCParams: RPCERROR
# ConnectionKeyGIDMismatch: FORBIDDEN? CREDENTIAL_MISMATCH?
# MissingCallerGID: SEARCHFAILED? CREDENTIAL_MISMATCH?
# RecordNotFound: SEARCHFAILED
# PlanetLAbRecordDoesNotExist: SEARCHFAILED
# PermissionError: FORBIDDEN
# InsufficientRights: FORBIDDEN
# MissingDelegateBit: CREDENTIAL_INVALID? FORBIDDEN?
# ChildRightsNotSubsetOfParent: CREDENTIAL_INVALID
# CertMissingParent: FORBIDDEN? CREDENTIAL_INVALID?
# CertNotSignedByParent: FORBIDDEN
# GidParentHrn: FORBIDDEN? CREDENTIAL_INVALID?
# GidInvalidParentHrn: FORBIDDEN? CREDENTIAL_INVALID?
# SliverDoesNotExist: SEARCHFAILED
# MissingTrustedRoots: SERVERERROR
# MissingSfaInfo: SERVERERROR
# InvalidRSpec: BADARGS
# InvalidRSpecElement: BADARGS
# AccountNotEnabled: REFUSED? FORBIDDEN?
# CredentialNotVerifiable: CREDENTIAL_INVALID
# CertExpired: EXPIRED? FORBIDDEN?
# -->
