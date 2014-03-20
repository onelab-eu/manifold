#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pyparsing as pp
from .                  import ProcessGateway, Argument, Parameter, Output, FLAG_IN_ANNOTATION, FLAG_OUT_ANNOTATION, FLAG_ADD_FIELD
from ...util.log        import Log

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
        STAR        = pp.Literal('*').setParseAction( lambda t: None )
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


        probe = (
                STAR.setResultsName('ip') # mpls after * ?
        |       pp.Optional(
                    value.setResultsName('ip') + ~pp.FollowedBy('ms') # we need a lookahead
                +   pp.Optional(LPAR + value.setResultsName('hostname') + RPAR)
                )
            +   value.setResultsName('rtt')
            +   pp.Literal('ms').suppress()
            +   pp.Optional(icmp_err.setResultsName('icmp'))
            +   pp.Optional(T_FLAG + value).suppress()
            +   pp.Optional(LCUR + value + RCUR).suppress()
            +   pp.Optional(BRA + value + KET).suppress()
#            +   pp.Optional(mpls_ext.setResultsName('mpls'))
        ).setParseAction(lambda t: t.asDict())

        probe_exhaustive = (
                interface
            +   interface2_exhaustive
            +   pp.Optional(mpls_ext)
        ).setParseAction(lambda t: 'exhaustive')
        

        def make_hop(t):
            hop = {
                'ttl': t['ttl'],
                'probes': t['probes'].asList(),
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
            +   pp.delimitedList(probe_exhaustive, delim = '').setResultsName('probes')
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

        text_output = (
                text_header.setResultsName('header')
            +   pp.Optional(timestamp).setResultsName('ts')
            +   pp.Group(pp.OneOrMore(hop | hop_exhaustive | error)).setResultsName('hops')
        )

        self.bnf = pp.Optional(pp.delimitedList(error, delim='')) + (
                text_output
            |   raw_output
        ).setParseAction(lambda t: t.asDict())

    def parse(self, string):
        try:
            result = self.bnf.parseString(string, parseAll=True)
            return result.asList()
        except pp.ParseException, e:
            Log.warning("Error line %s, column %s:" % (e.lineno, e.col))
            Log.warning(e.line)
            Log.warning(" " * e.col, "^--- syntax error")
        return []

class ParisTracerouteGateway(ProcessGateway):
    __gateway_name__ = 'paristraceroute_process'
    __tool__ = 'paristraceroute'

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
    # WE SHOULD EXPLICITELY SET METADATA HERE

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
    announces = """
    class probe_traceroute {
        inet ip;
        string hostname;
    };

    class hop {
        const unsigned ttl;
        probe probes[];
    };

    class traceroute {
        hop hops[];
    };

    """
    output = Output(ParisTracerouteParser, announces, 'traceroute')
    #class Traceroute(Announce):
    #    Field()
    #    Field()

    #path = '/usr/local/bin/paris-traceroute'
    path = '/bin/paris-traceroute' # fake
