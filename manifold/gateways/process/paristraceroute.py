#!/usr/bin/env python
# -*- coding: utf-8 -*-


import os, pyparsing as pp
from .                          import ProcessGateway, ProcessCollection, Argument, Parameter, FLAG_IN_ANNOTATION, FLAG_OUT_ANNOTATION, FLAG_ADD_FIELD

from ...core.record             import Record, Records
from ...util.log                import Log
from ...util.misc               import is_iterable
from ...util.predicate          import eq, included
from ...util.filesystem         import hostname

class ParisTracerouteParser(object):
    """
    """

    def __init__(self):
        """
        """
        LPAR        = pp.Literal('(').suppress()
        RPAR        = pp.Literal(')').suppress()
        LCUR        = pp.Literal('{').suppress()
        RCUR        = pp.Literal('}').suppress()
        BRA         = pp.Literal('[').suppress()
        KET         = pp.Literal(']').suppress()
        COMA        = pp.Literal(',').suppress()
        STAR        = pp.Literal('*').setParseAction(lambda t: [None])
        SHARP       = pp.Literal('#').suppress()
        PPLB_FLAG   = pp.Literal('=').suppress()
        PFLB_FLAG   = pp.Literal('<').suppress()
        T_FLAG      = pp.Literal('!T').suppress() # Strange TTL value
        H_FLAG      = pp.Literal('!H').suppress()
        N_FLAG      = pp.Literal('!N').suppress()
        Q_FLAG      = pp.Literal('!Q').suppress()
        A_FLAG      = pp.Literal('!A').suppress()
        X_FLAG      = pp.Literal('!').suppress()

        value = pp.Word(pp.alphanums + '_' + '-' + '.').setParseAction(lambda t: t[0])


        EOL = pp.LineEnd().suppress()

#DEPRECATED|        garbage = (
#DEPRECATED|                '[WARN]' + pp.SkipTo(pp.LineEnd()) + EOL
#DEPRECATED|        |       'header' + pp.SkipTo(pp.LineEnd()) + EOL
#DEPRECATED|        )
#DEPRECATED|

        error = (
                '[WARN]' + pp.SkipTo(pp.LineEnd()) + EOL
        |        BRA + value + KET + pp.delimitedList(value, delim=' ')
        |       SHARP + pp.delimitedList(value)
        |       pp.Literal('header') + pp.delimitedList(value, delim=' ')
        ).suppress()

        timestamp = (
                value + value + value + value + value + ':' + value + ':' + value + value
        ).setParseAction(lambda t: int(t[0]))

        icmp_err = (
                (H_FLAG).setParseAction(lambda t: ('h', None, None))
        |       (N_FLAG).setParseAction(lambda t: ('n', None, None))
        |       (Q_FLAG).setParseAction(lambda t: ('q', None, None))
        |       (A_FLAG).setParseAction(lambda t: ('a', None, None))
        |       (T_FLAG + value).setParseAction(lambda t: ('t', int(t[1]), None))
        |       (X_FLAG + value).setParseAction(lambda t: ('x', int(t[1]), None))
        )

        interface = (
	        value
            +   pp.Optional(LPAR + value + RPAR)
            +   pp.Optional(':' + pp.delimitedList(value, delim = ';'))
            +   pp.Optional(PPLB_FLAG | PFLB_FLAG)
        )

        interface2_exhaustive = (
	        value + '/' + value + '/' + value + '/' + value + 'ms'
            +   pp.Optional(icmp_err).setResultsName('icmp')
            +   pp.Optional(T_FLAG + value)
            +   pp.Optional(LCUR + value + RCUR)
            +   pp.Optional(BRA + value + KET)
        ).setParseAction(lambda t: t.asDict())

        mpls_stack = (
                value + 'TTL=' + pp.delimitedList(value, delim = '|')
        )

        mpls_ext = (
                'MPLS Label' + pp.delimitedList(mpls_stack, delim = ',')
        )
        self.a = mpls_ext


        def make_probe(t=None):
            if not t:
                return {'ip': None}
            probe = {}
            if 'ip' in t:
                probe['ip'] = t['ip']
            if 'hostname' in t:
                probe['hostname'] = t['hostname']
            if 'rtt' in t:
                probe['rtt'] = t['rtt']
            return probe
        ip_hostname = (
                value.setResultsName('hostname') + ~pp.FollowedBy('ms') # we need a lookahead
            +   LPAR + value.setResultsName('ip') + RPAR
        |       value.setResultsName('ip') + ~pp.FollowedBy('ms') # we need a lookahead
        )
        probe = (
                STAR.setResultsName('ip') # mpls after * ?
        |       pp.Optional(ip_hostname)
            +   value.setResultsName('rtt')
            +   pp.Literal('ms').suppress()
            +   pp.Optional(icmp_err.setResultsName('icmp'))
            +   pp.Optional(T_FLAG + value).suppress()
            +   pp.Optional(LCUR + value + RCUR).suppress()
            +   pp.Optional(BRA + value + KET).suppress()
#            +   pp.Optional(mpls_ext.setResultsName('mpls'))
        ).setParseAction(make_probe)#lambda t: t.asDict())

        probe_exhaustive = (
                interface
            +   interface2_exhaustive
            +   pp.Optional(mpls_ext)
        ).setParseAction(lambda t: 'exhaustive')


        def make_hop(t):
            hop = {
                'ttl': t['ttl'],
                'probes': Records(t['probes'].asList()),
            }
            return hop
        hop = (
                value.setResultsName('ttl')
            +   pp.Group(pp.OneOrMore(probe)).setResultsName('probes')
            +   pp.Optional(mpls_ext.setResultsName('mpls'))
        ).setParseAction(make_hop)

        hop_exhaustive = (
                value.setResultsName('ttl')
            +   'P(' + value + ',' + value + pp.Optional(',' + value + ',' + value) + ')'
            +   pp.delimitedList(probe_exhaustive, delim = '').setResultsName('probes') # XXX: delim = '' seems to raise warning in pyparsing
        ).setParseAction(lambda t: t.asDict())

        header_part = (
		'traceroute'
            +   BRA + LPAR
            +   value.setResultsName('src_ip')
            +   pp.Literal(':').suppress()
            +   value.setResultsName('sport')
            +   RPAR + pp.Literal('->').suppress() + LPAR
            +   value.setResultsName('dst_ip')
            +   pp.Literal(':').suppress()
            +   value.setResultsName('dport') + RPAR + KET
            +   pp.Literal(',').suppress()
            +   pp.Literal('protocol').suppress()
            +   value.setResultsName('proto')
            +   pp.Literal(',').suppress()
            +   pp.Literal('algo').suppress()
            +   value.setResultsName('algo')
        )#.setParseAction(lambda t: t.asDict())

        raw_header = (
            header_part
        )

        raw_probe = (
                value + value + 'ms'
            +   pp.Optional(T_FLAG + value)
            +   pp.Optional(LCUR + value + RCUR)
            +   pp.Optional(BRA + value + KET)
        )

        raw_probe_list = (
                pp.delimitedList(raw_probe, delim = '')
        )

        raw_hop = (
                value.setResultsName('ttl')
            +   raw_probe_list.setResultsName('probes')
        ).setParseAction(lambda t: t.asDict())

        raw_hop_list = (
                pp.delimitedList(raw_hop, delim = '')
        )

        raw_output = (
                raw_header.setResultsName('header')
            +   raw_hop_list.setResultsName('hops')
        ).setParseAction(lambda t: t.asDict())

        def parse_header(t):
            header = dict()
            header.update(t['header_part'])
            header['duration'] = t['duration']
            return header
        text_header = (
                header_part.setResultsName('header_part')
            +   COMA.suppress()
            +   pp.Literal('duration').suppress()
            +   value.setResultsName('duration')
            +   pp.Literal('s').suppress()
        ).setParseAction(parse_header)# lambda t: t.asDict())

        def make_traceroute(t):
#            traceroute = {
#                'source': {
#                  'ip':   t['header']['src_ip'],
#                  'port': t['header']['sport']
#                },
#                'destination': {
#                  'ip':   t['header']['dst_ip'],
#                  'port': t['header']['dport']
#                },
#                'protocol': t['header']['proto'],
#                'duration': t['header']['duration'],
#                'algo': t['header']['algo'],
#            }
            traceroute = {
                'source': t['header']['src_ip'],
                'source_port': t['header']['sport'],
                'destination': t['header']['dst_ip'],
                'destination_port': t['header']['dport'],
                'protocol': t['header']['proto'],
                'duration': t['header']['duration'],
                'algo': t['header']['algo'],
            }

            traceroute['hops'] = Records(t['hops'].asList())
            if 'ts' in traceroute:
                traceroute['ts'] = t['ts']
            return traceroute

        text_output = (
                text_header.setResultsName('header')
            +   pp.Optional(timestamp).setResultsName('ts')
            +   pp.Group(pp.OneOrMore(hop | hop_exhaustive | error.suppress())).setResultsName('hops')
        ).setParseAction(make_traceroute)

        self.bnf = pp.Optional(pp.delimitedList(error, delim='')) + (
                text_output
            |   raw_output
        )

    def parse(self, string):
        try:
            result = self.bnf.parseString(string, parseAll=True)

            return Records(result.asList())
        except pp.ParseException, e:
            Log.warning("Error line %s, column %s:" % (e.lineno, e.col))
            Log.warning(e.line)
            Log.warning(" " * e.col, "^--- syntax error")
        return []

class ProbeCollection(ProcessCollection):
    """
    class probe_traceroute {
        ip ip;
        string hostname;
        float rtt;
        CAPABILITY(join);
        LOCAL KEY(ip);
    };
    """

class HopCollection(ProcessCollection):
    """
    class hop {
        const unsigned ttl;
        probe_traceroute probes[];
        CAPABILITY(join);
        LOCAL KEY(ttl);
    };
    """

class TracerouteCollection(ProcessCollection):
    """
    class traceroute {
        ip source;
        ip destination;
        unsigned source_port;
        unsigned destination_port;
        hop hops[];
        CAPABILITY(join);
        KEY(source, destination);
    };
    """
    __object_name__ = 'traceroute'
    __tool__ = 'traceroute'

    parameters = [
        Parameter(
            name        = 'hostname',
            type        = 'string',  # in metadata ?
            description = '',        # in metadata ?
            short       = '-n',
            action      = '',
            # METADATA + IN_FIELDS
            flags       = FLAG_ADD_FIELD # deduced from metadata ?
        ),
    ]
    arguments = [
        Argument(
            name        = 'destination',
            type        = 'ip'
        ),
    ]

    parser = ParisTracerouteParser

    #path = '/usr/local/bin/paris-traceroute'
    if os.path.exists("/bin/paris-traceroute"):
        path = "/bin/paris-traceroute"
    else:
        path = '/usr/sbin/paris-traceroute' 

    # XXX redundant with ping !!!
    def enforce_partition(self, packet):
        Log.warning("This is approximate...")
        filter = packet.get_destination().get_filter()
        my_hostname = hostname()

        source = filter.get_op('source', (eq, included))
        if source:
            if is_iterable(source):
                source = list(source)
            else:
                source = [source]
            if not my_hostname in source:
                return None
        return packet

class ParisTracerouteGateway(ProcessGateway):
    __gateway_name__ = 'paristraceroute'

    def __init__(self, router = None, platform_name = None, **platform_config):
        ProcessGateway.__init__(self, router, platform_name, **platform_config)

        self.register_collection(ProbeCollection())
        self.register_collection(HopCollection())
        self.register_collection(TracerouteCollection())

    # Parameters and arguments are additional fields that will get added
    # We need not only static fields that are not piloted by an option, but the
    # others also
    # Same for keys, capabilities etc
    # Keys vs arguments (might not need tags if we have metadata)
# FOR FUTURE USE|    """
# FOR FUTURE USE|    class protocol {
# FOR FUTURE USE|        const inet source;
# FOR FUTURE USE|        const inet destination;
# FOR FUTURE USE|        const string protocol;
# FOR FUTURE USE|    }
# FOR FUTURE USE|
# FOR FUTURE USE|    class packet {
# FOR FUTURE USE|        protocol protocol;
# FOR FUTURE USE|    };
# FOR FUTURE USE|
# FOR FUTURE USE|    class probe {
# FOR FUTURE USE|        packet packet;
# FOR FUTURE USE|        timestamp timestamp;
# FOR FUTURE USE|    };
