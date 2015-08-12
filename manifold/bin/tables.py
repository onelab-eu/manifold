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
    import argparse

    opt = Options()

    class InsertAction(argparse.Action):
        def __call__(self, parser, namespace, values, option_string=None):
            setattr(namespace, 'method', METHOD_INSERT)
            setattr(namespace, 'index', values[0])

    class DeleteAction(argparse.Action):
        def __call__(self, parser, namespace, values, option_string=None):
            setattr(namespace, 'method', METHOD_DELETE)
            setattr(namespace, 'index', values[0])

    # Method
    group = opt.add_mutually_exclusive_group(required=True)
    group.add_argument("-A", "--append", action='store_const', 
            const=METHOD_APPEND, dest='method',
            help = "TODO")
    group.add_argument("-F", "--flush", action='store_const', 
            const=METHOD_FLUSH, dest='method',
            help = "TODO")
    group.add_argument("-I", "--insert", action=InsertAction, 
            dest='method',
            help = "TODO")
    group.add_argument("-D", "--delete", action=DeleteAction,
            dest='method',
            help = "todo")

    # Address
    opt.add_argument("-o", "--object", dest='object',
            default = DEFAULT_TABLE,
            help = "TODO. Default is %s" % DEFAULT_TABLE)
    opt.add_argument("-f", "--fields", dest='fields',
            default = DEFAULT_FIELDS,
            help = "TODO. Default is %s" % DEFAULT_FIELDS)
    opt.add_argument("-a", "--access", dest='access',
            default = DEFAULT_ACTION,
            help = "TODO. Default is %s" % DEFAULT_ACTION)

    # Target (only required for append or insert
    opt.add_argument("-j", "--jump", dest='target',
            help = "TODO")

def main():
    init_options()
    Shell.init_options()

    # This should be taken care of by argparse...

    if Options().method in [METHOD_APPEND, METHOD_INSERT]:
        if not Options().target:
            print "manifold-tables: error: argument -j/--jump is required"
            sys.exit(0)

        # Build rule dictionary
        rule = Rule()
        rule.object = Options().object
        fields = [x.strip() for x in Options().fields.split(',')]
        rule.fields = set(fields)
        rule.access = Options().access
        rule.target = Options().target
        rule_dict = rule.to_dict()

    shell = Shell()

    if Options().method == METHOD_APPEND:
        shell.evaluate( "insert into local:policy SET policy_json = '%s'" % json.dumps(rule_dict))

    elif Options().method == METHOD_FLUSH:
        shell.evaluate( "delete from local:policy")
        
    else:
        raise Exception, "Not implemented"

    shell.terminate()


if __name__ == '__main__':
    main()
