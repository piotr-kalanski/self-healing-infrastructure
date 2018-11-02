from common.runbook import RunBook
from common.base_operator import BaseOperator
from common.task_executor import LocalTaskExecutor


class DummyOperator(BaseOperator):

    def __init__(self, x):
        super(DummyOperator, self).__init__()
        self.x = x

    def execute(self, context: dict):
        print("dummy: " + str(self.x))
        return

    def __str__(self):
        return "DummyOperator({})".format(str(self.x))


rb = RunBook(task_executor=LocalTaskExecutor())

o1 = DummyOperator(1)
o2 = DummyOperator(2)
o3 = DummyOperator(3)
o4 = DummyOperator(4)

rb >> o1 >> DummyOperator("1a")
o1 >> o2 >> o3
o2 >> o4 >> DummyOperator("5")

rb.print_tasks()

print("\n\nRun tasks:")
while rb.has_more_tasks_to_execute():
    rb.execute_next_task()
