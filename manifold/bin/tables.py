#!/usr/bin/env python
#! -*- coding: utf-8 -*-

import sys, json
from argparse              import ArgumentParser
from manifold.bin.shell    import Shell
from manifold.util.options import Options
from manifold.util.log     import Log
from manifold.policy.rule  import Rule

METHOD_APPEND = 1
METHOD_FLUSH  = 2
METHOD_INSERT = 3
METHOD_DELETE = 4

DEFAULT_TABLE  = '*'
DEFAULT_FIELDS = '*'
DEFAULT_ACTION = 'RW'

def init_options():
    opt = Options()

    # Method
    group = opt.add_mutually_exclusive_group(required=True)
    group.add_argument("-A", "--append", action='store_const', 
            const=METHOD_APPEND, dest='method',
            help = "TODO")
    group.add_argument("-F", "--flush", action='store_const', 
            const=METHOD_FLUSH, dest='method',
            help = "TODO")
    group.add_argument("-I", "--insert", action='store_const', 
            const=METHOD_INSERT, dest='method',
            help = "TODO")
    group.add_argument("-D", "--delete", action='store_const', 
            const=METHOD_DELETE, dest='method',
            help = "todo")

    # XXX positional argument for -I and -D ?

    # Destination
    opt.add_argument("-o", "--object", dest='object',
            default = DEFAULT_TABLE,
            help = "TODO. Default is %s" % DEFAULT_TABLE)
    opt.add_argument("-f", "--fields", dest='fields',
            default = DEFAULT_FIELDS,
            help = "TODO. Default is %s" % DEFAULT_FIELDS)
    opt.add_argument("-a", "--access", dest='access',
            default = DEFAULT_ACTION,
            help = "TODO. Default is %s" % DEFAULT_ACTION)

    # Target
    opt.add_argument("-j", "--jump", dest='target', required = True,
            help = "TODO")

def main():
    init_options()
    Shell.init_options()

    args = Options()

    if args.method == METHOD_APPEND:
        # Build rule dictionary
        rule = Rule()
        rule.object = args.object
        fields = [x.strip() for x in args.fields.split(',')]
        rule.fields = set(fields)
        rule.access = args.access
        rule.target = args.target
        rule_dict = rule.to_dict()
        
        # Add rule
        # XXX We might need a support for transactions
        shell = Shell()
        command = "insert into local:policy SET policy_json = '%s'" % json.dumps(rule_dict)
        print "command", command
        shell.evaluate(command)
        shell.terminate()
        
    else:
        raise Exception, "Not implemented"


if __name__ == '__main__':
    main()
