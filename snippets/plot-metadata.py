#!/usr/bin/env python
#! -*- coding: utf-8 -*-

#digraph hierarchy {
#size="5,5"
#node[shape=record,style=filled,fillcolor=gray95]
#edge[dir=back, arrowtail=empty]
#
#
#2[label = "{AbstractSuffixTree|+ text\n+ root|...}"]
#3[label = "{SimpleSuffixTree|...| + constructTree()\l...}"]
#4[label = "{CompactSuffixTree|...| + compactNodes()\l...}"]
#5[label = "{SuffixTreeNode|...|+ addSuffix(...)\l...}"]
#6[label = "{SuffixTreeEdge|...|+ compactLabel(...)\l...}"]
#
#
#2->3
#2->4
#5->5[constraint=false, arrowtail=odiamond]
#4->3[constraint=false, arrowtail=odiamond]
#2->5[constraint=false, arrowtail=odiamond]
#5->6[arrowtail=odiamond]
#}

import sys

from manifold.bin.shell  import Shell

def usage():
    print "Usage: %s [NAME]" % sys.argv[0]
    print ""
    print "Enable a platform"

def gv_header():
    return """
    digraph hierarchy {
      size="5,5"
      node[shape=record,style=filled,fillcolor=gray95]
      edge[dir=back, arrowtail=empty]

    """

def gv_node(obj):
    cols = obj.get('column', None)

    name  = obj.get('table', 'N/A')
    
    if cols:
        fields = [ x.get('name', 'N/A') for x in cols ]
    else:
        fields  = []
    methods = []

    field_str  = "\\n".join(fields)
    method_str = "(no method)" #"\\n".join(methods)
    label = "{%(name)s|%(field_str)s|%(method_str)s}" % locals()
    
    key   = 'KEY'
    
    return "%(name)s[label = \"%(label)s\"]" % locals()

def gv_footer():
    return """

    }
    """

def metadata_to_graphviz(objects):
    out = []
    out.append(gv_header())
    for obj in objects:
        out.append(gv_node(obj))    
    out.append(gv_footer())
    return "\n".join(out)

def main():
    argc = len(sys.argv)
    if argc not in [1,2]:
        usage()
        sys.exit(1)

    name = sys.argv[1] if argc == 2 else 'local'
    
    Shell.init_options()
    shell = Shell(interactive=False)
    command = 'SELECT table, column.name FROM %(name)s:object' % locals()
    objects = shell.evaluate(command % locals())
    shell.terminate()

    print metadata_to_graphviz(objects['value'])


if __name__ == '__main__':
    main()
