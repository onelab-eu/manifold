from manifold.policy.target import Target, TargetValue

class DropTarget(Target):

    def process(self, query, record, annotation):
        return (TargetValue.DENIED, None)
