from manifold.policy.target import Target, TargetValue

class LogTarget(Target):
    
    def process(self, query, annotation):
        Log.tmp("LOG TARGET: %s %r" % (query, annotation))
        return TargetValue.CONTINUE
