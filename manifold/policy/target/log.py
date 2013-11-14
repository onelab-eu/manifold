from manifold.policy.target import Target, TargetValue
from manifold.util.log      import Log

class LogTarget(Target):
    
    def process(self, query, record, annotation):
        if not record:
            Log.tmp("LOG TARGET: %s %r" % (query, annotation))
        else:
            Log.tmp("LOG TARGET RECORDS: %s %r" % (query, annotation))
        return (TargetValue.CONTINUE, None)
