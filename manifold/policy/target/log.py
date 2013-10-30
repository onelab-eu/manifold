from manifold.policy.target import Target, TargetValue
from manifold.util.log      import Log

class LogTarget(Target):
    
    def process(self, query, annotation):
        Log.tmp("LOG TARGET: %s %r" % (query, annotation))
        return TargetValue.CONTINUE
