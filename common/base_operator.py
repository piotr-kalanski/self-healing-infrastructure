import abc


class BaseOperator(abc.ABC):

    def __init__(self):
        self.downstream_operators = []

    @abc.abstractmethod
    def execute(self, context: dict):
        return

    def add_downstream_operator(self, task):
        self.downstream_operators.append(task)

    def __rshift__(self, other):
        self.add_downstream_operator(other)
        return other
