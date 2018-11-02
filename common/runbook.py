from common.base_operator import BaseOperator


class RunBookStartOperator(BaseOperator):
    def execute(self, context: dict):
        pass # TODO - implementation

    def __str__(self):
        return "RunBookStartOperator()"


class RunBookEndOperator(BaseOperator):
    def execute(self, context: dict):
        pass # TODO - implementation


class RunBook:

    def __init__(self):
        self.start_operator = RunBookStartOperator()

    def print_tasks(self):
        def print_task(operator, level):
            print("*" + ('-' * level) + ' ' + str(operator))
            for t in operator.downstream_operators:
                print_task(t, level+1)
        print_task(self.start_operator, 0)

    def __rshift__(self, other):
        return self.start_operator >> other
