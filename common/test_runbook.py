from common.runbook import RunBook
from common.base_operator import BaseOperator


class DummyOperator(BaseOperator):

    def __init__(self, x):
        super(DummyOperator, self).__init__()
        self.x = x

    def execute(self, context: dict):
        print(self.x)
        return

    def __str__(self):
        return "DummyOperator({})".format(str(self.x))


rb = RunBook()

o1 = DummyOperator(1)
o2 = DummyOperator(2)
o3 = DummyOperator(3)
o4 = DummyOperator(4)

rb >> o1
o1 >> o2 >> o3
o2 >> o4

rb.print_tasks()
