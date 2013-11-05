#!/usr/bin/env python
#! -*- coding: utf-8 -*-

import sys, json
from argparse             import ArgumentParser
from manifold.bin.shell   import Shell
from manifold.policy.rule import Rule

METHOD_ADD = 1

DEFAULT_TABLE  = '*'
DEFAULT_FIELDS = '*'
DEFAULT_ACTION = 'RW'

def init_options():
    usage="""%prog [options] [METHOD] [PARAMETERS]
Issue an SFA call, using credentials from the manifold database."""

    parser = ArgumentParser()

    parser.add_argument("-A", "--add", action='store_const', 
            const=METHOD_ADD, dest='method', required = True,
            help = "TODO")
    parser.add_argument("-o", "--object", dest='object',
            default = DEFAULT_TABLE,
            help = "TODO. Default is %s" % DEFAULT_TABLE)
    parser.add_argument("-f", "--fields", dest='fields',
            default = DEFAULT_FIELDS,
            help = "TODO. Default is %s" % DEFAULT_FIELDS)
    parser.add_argument("-a", "--access", dest='access',
            default = DEFAULT_ACTION,
            help = "TODO. Default is %s" % DEFAULT_ACTION)
    parser.add_argument("-j", "--jump", dest='target', required = True,
            help = "TODO")

    return parser.parse_args()

def main():
    args = init_options()

    if args.method == METHOD_ADD:
        # Build rule dictionary
        rule = Rule()
        rule.object = args.object
        rule.fields = args.fields
        rule.access = args.access
        rule.target = args.target
        rule_dict = rule.to_dict()
        
        # Add rule
        shell = Shell()
        command = 'CREATE local:policy SET policy_json = "%s"' % json.dumps(rule_dict) 
        shell.evaluate(command % locals())
        shell.terminate()
        
    else:
        raise Exception, 'Unknown method: %s' % args.method

if __name__ == '__main__':
    main()
