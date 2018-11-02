import abc


class BaseOperator(abc.ABC):

    def __init__(self):
        self.downstream_operators = []

    @abc.abstractmethod
    def execute(self, context: dict):
        return

    def add_downstream_operator(self, operator):
        self.downstream_operators.append(operator)

    def __rshift__(self, other):
        self.add_downstream_operator(other)
        return other

    def __lshift__(self, other):
        other.add_downstream_operator(self)
        return other