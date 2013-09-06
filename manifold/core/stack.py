#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Stack is used to stores ExploreTask while exploring
# the DBGraph during the QueryPlan computation.
# \sa manifold.core.query_plan
# \sa manifold.core.explore_task
# 
# QueryPlan class builds, process and executes Queries
#
# Copyright (C) UPMC Paris Universitas
# Authors:
#   Jordan Augé       <jordan.auge@lip6.fr> 
#   Marc-Olivier Buob <marc-olivier.buob@lip6.fr>

from manifold.util.log             import Log
from manifold.util.type            import returns, accepts

TASK_11, TASK_1Nsq, TASK_1N = range(0, 3)

class Stack(object):
    """
    Stack is use to prior some tasks while exploring the 3nf
    graph in order to build the QueryPlan. We first visit the
    tables reachables from the root Table by only traversing
    1..1 arcs. If this is not sufficient to serve the user
    Query, we extend this 3nf-subgraph by adding a part of
    the 3nf graph requiring to traverse 1..N link. This Stack
    allows to order this exploration.

    Stack maintains a set of stacks (one per priority), each
    of these storing a set of ExploreTask. Thus, building
    a QueryPlan only requires one Stack instance.
    - Pushing an ExploreTask in the Stack dispatches it to
    the appropriate nested stack.
    - Poping an ExploreTask extracts the ExploreTask having
    the higher priority.
    """

    def __init__(self, root_task):
        """
        Constructor.
        Args:
            root_task: an ExploreTask, corresponding to the
            3nf graph exploration starting from the root Table.
        """
        self.tasks = {
            TASK_11  : [root_task],
            TASK_1Nsq: [],
            TASK_1N  : [],
        }

    def push(self, task, priority):
        """
        Push an ExploreTask in this Stack.
        Args:
            task: An ExploreTask instance.
            priority: The corresponding priority, which is a
                value among {TASK_11, TASK_1Nsq, TASK_1N}.
        """
        Log.debug("Pushing ExploreTask with priority %d : %r" % (priority, task))
        self.tasks[priority].append(task)

    def pop(self):
        """
        Pop an ExploreTask from this Stack.
        Returns:
            The ExploreTask having the higher priority (if this Stack
            contains at least one ExploreTask), None otherwise.
        """
        for priority in [TASK_11, TASK_1Nsq, TASK_1N]:
            tasks = self.tasks[priority]
            if tasks:
                task = tasks.pop(0)
                Log.debug("Poping ExploreTask with priority %d : %r" % (priority, task))
                return task
        return None

    @returns(bool)
    def is_empty(self):
        """
        Returns:
            True iif the Stack does not contains any ExploreTask instance.
        """
        return all(map(lambda x: not x, self.tasks.values()))

    def dump(self):
        """
        (Debug function). Dump the ExploreTask embeded in this Stack
        using the logger.
        """
        for priority in [TASK_11, TASK_1Nsq, TASK_1N]:
            Log.tmp("PRIO %d : %r" % (priority, self.tasks[priority]))


