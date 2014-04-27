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

import pprint, time, traceback
from types                      import StringTypes

from manifold.core.code         import CORE, GATEWAY, SUCCESS, ERROR, WARNING
from manifold.core.packet       import ErrorPacket
from manifold.util.log          import Log
from manifold.util.type         import accepts, returns

class ResultValue(dict):

    ALLOWED_FIELDS = set(["origin", "type", "code", "value", "description", "traceback", "ts"])

    def __init__(self, *args, **kwargs):
        """
        Constructor.
        Args:
            origin: A value among:
                CORE    : iif the error is raised by manifold/core/*
                GATEWAY : iif the error is raised by manifold/gateways/*
            type: An integer describing the status of the query.
                SUCCESS : the Query has succeeded.
                WARNING : the Query results are not complete.
                ERROR   : the Query has failed.
            code: An integer value.
            value: - A list of ErrorPacket in case of failure
                   - A list of Records or dicts in case of success.
            description: A list of ErrorPacket instances.
            traceback: A non empty String containing the traceback or None.
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
        cstr_success = set(["code", "origin", "value"]) <= given
        cstr_error   = set(["code", "type", "origin", "description"]) <= given
        assert given <= self.ALLOWED_FIELDS, "Wrong fields in ResultValue constructor: %r" % (given - self.ALLOWED_FIELDS)
        assert cstr_success or cstr_error, "Incomplete set of fields in ResultValue constructor: %r" % given

        dict.__init__(self, **kwargs)

        # Set missing fields to None
        for field in self.ALLOWED_FIELDS - given:
            self[field] = None
        if not "ts" in self:
            self["ts"] = time.time()

    # Internal MySlice errors   : return ERROR
    # Internal MySlice warnings : return RESULT WITH WARNINGS
    # Debug                     : add DEBUG INFORMATION
    # Gateway errors            : return RESULT WITH WARNING
    # all Gateways errors       : return ERROR

#DEPRECATED|    @classmethod
#DEPRECATED|    #@returns(ResultValue)
#DEPRECATED|    def get_result_value(self, results, result_values):
#DEPRECATED|        """
#DEPRECATED|        Craft a ResultValue instance according to a list of Records and
#DEPRECATED|        an optionnal list of ResultValues retrieved during the QueryPlan
#DEPRECATED|        execution.
#DEPRECATED|        Args:
#DEPRECATED|            results: A list of Records
#DEPRECATED|            result_values: A list of ResultValue instances.
#DEPRECATED|        """
#DEPRECATED|        # let's analyze the results of the query plan
#DEPRECATED|        # XXX we should inspect all errors to determine whether to return a
#DEPRECATED|        # result or not
#DEPRECATED|
#DEPRECATED|        if not result_values:
#DEPRECATED|            # No error
#DEPRECATED|            return ResultValue(code = SUCCESS, origin = [self.CORE, 0], value = results)
#DEPRECATED|        else:
#DEPRECATED|            # Handle errors
#DEPRECATED|            return ResultValue(code = WARNING, origin = [self.CORE, 0], value = results, description = result_values)
#DEPRECATED|

    @returns(int)
    def get_code(self):
        """
        Returns:
            The code transported in this ResultValue instance/
        """
        return self["code"]

    @classmethod
    def get(self, records, errors):
        num_errors = len(errors)

        if num_errors == 0:
            return ResultValue.success(records)
        elif records:
            return ResultValue.warning(records, errors)
        else:
            return ResultValue.errors(errors)

    @classmethod
    #@returns(ResultValue)
    def success(self, result):
        return ResultValue(
            code        = SUCCESS,
            type        = SUCCESS,
            origin      = [CORE, 0],
            value       = result
        )

    @staticmethod
    #@returns(ResultValue)
    def warning(result, errors):
        return ResultValue(
            code        = ERROR, # XXX this is crappy
            type        = WARNING,
            origin      = [CORE, 0],
            value       = result,
            description = errors
        )

    @staticmethod
    #@returns(ResultValue)
    def error(description, code = ERROR):
        """
        Make a ResultValue corresponding to an error.
        Args:
            description: A String instance.
            code: An integer (see codes provided by ResultValue).
        Returns:
            The corresponding ResultValue instance.
        """
        assert isinstance(description, StringTypes),\
            "Invalid description = %s (%s)" % (description, type(description))
        assert isinstance(code, int),\
            "Invalid code = %s (%s)" % (code, type(code))

        return ResultValue(
            type        = ERROR,
            code        = code,
            origin      = [CORE, 0],
            description = [ErrorPacket(type = ERROR, code = code, message = description, traceback = None)]
        )

    @staticmethod
    #@returns(ResultValue)
    def errors(errors):
        """
        Make a ResultValue corresponding to an error and
        gathering a set of ErrorPacket instances.
        Args:
            errors: A list of ErrorPacket instances.
        Returns:
            The corresponding ResultValue instance.
        """
        assert isinstance(errors, list),\
            "Invalid errors = %s (%s)" % (errors, type(errors))

        return ResultValue(
            type        = ERROR,
            code        = ERROR,
            origin      = [CORE, 0],
            description = errors
        )

    @returns(bool)
    def is_warning(self):
        return self["type"] == WARNING

    @returns(bool)
    def is_success(self):
        return self['type'] == self.SUCCESS and self['code'] == self.SUCCESS

    @returns(list)
    def ok_value(self):
        return self["value"]

    def get_all(self):
        if not self.is_success() and not self.is_warning():
            raise Exception, "Error executing query: %s" % self['description']
        return self.ok_value()

    def get_one(self):
        records = self.get_all()
        if len(records) > 1:
            raise Exception, "More than 1 record"
        return records.get_one()

    @returns(StringTypes)
    def get_error_message(self):
        return "%r" % self["description"]

    @staticmethod
    def to_html(raw_dict):
        return pprint.pformat(raw_dict).replace("\\n","<br/>")

    @returns(dict)
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
