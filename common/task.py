from common.base_operator import BaseOperator


class Task:

    def __init__(self, operator: BaseOperator):
        self.operator = operator

    def execute(self, context):
        self.operator.execute(context)
