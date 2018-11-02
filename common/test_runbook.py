import unittest
from common.runbook import RunBook
from common.base_operator import BaseOperator
from common.task import Task
from common.task_executor import TaskExecutor


class DummyOperator(BaseOperator):
    def __init__(self, x):
        super(DummyOperator, self).__init__()
        self.x = x

    def execute(self, context: dict):
        return


class MockTaskExecutor(TaskExecutor):
    def __init__(self):
        self.executed_tasks = []
        self.executed_operators = []

    def execute_task(self, task: Task, context: dict):
        self.executed_tasks.append(task)
        self.executed_operators.append(task.operator)


class RunBookTests(unittest.TestCase):

    def test_build_dependencies(self):
        task_executor = MockTaskExecutor()
        run_book = RunBook(task_executor=task_executor)

        o1 = DummyOperator(1)
        o2 = DummyOperator(2)

        run_book >> o1 >> o2

        self.assertEquals([o1], run_book.start_operator.downstream_operators)
        self.assertEquals([o2], o1.downstream_operators)

    def test_execute_all_tasks(self):
        task_executor = MockTaskExecutor()
        run_book = RunBook(task_executor=task_executor)

        o1 = DummyOperator(1)
        o2 = DummyOperator(2)
        o3 = DummyOperator(3)
        o4 = DummyOperator(4)
        o5 = DummyOperator(5)
        o6 = DummyOperator(6)

        run_book >> o1 >> o2
        o2 >> o3 >> o4
        o4 >> o5
        o4 >> o6

        while run_book.has_more_tasks_to_execute():
            run_book.execute_next_task()

        self.assertEquals(8, len(task_executor.executed_operators))
        self.assertEquals([o1, o2, o3, o4, o5, o6], task_executor.executed_operators[1:-1])
