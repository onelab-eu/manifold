from manifold.policy.target import Target, TargetValue

class DropTarget(Target):

    def process(self, query, annotation):
        return TargetValue.DROP
