from common.base_operator import BaseOperator
from common.task import Task
from common.task_executor import TaskExecutor, LambdaTaskExecutor


class RunBookStartOperator(BaseOperator):
    def execute(self, context: dict):
        print("start operator")
        pass # TODO - implementation

    def __str__(self):
        return "RunBookStartOperator()"


class RunBookEndOperator(BaseOperator):
    def execute(self, context: dict):
        print("end operator")
        pass # TODO - implementation


class RunBook:

    def __init__(
            self,
            task_executor: TaskExecutor = LambdaTaskExecutor()
    ):
        """

        :param task_executor: Executor for tasks
        """
        self.start_operator = RunBookStartOperator()
        self.tasks_queue = []
        self.next_task_to_execute = Task(self.start_operator)
        self.task_executor = task_executor

    def print_tasks(self):
        def print_task(operator, level):
            print("*" + ('-' * level) + ' ' + str(operator))
            for t in operator.downstream_operators:
                print_task(t, level+1)
        print_task(self.start_operator, 0)

    def __rshift__(self, other):
        return self.start_operator >> other

    def execute_next_task(self):
        context = {} # TODO - create context

        if self.next_task_to_execute is None:
            raise Exception("All tasks executed")

        self.task_executor.execute_task(self.next_task_to_execute, context)

        for o in self.next_task_to_execute.operator.downstream_operators:
            self.tasks_queue.append(Task(o))

        if len(self.tasks_queue) != 0:
            self.next_task_to_execute = self.tasks_queue.pop(0)
        else:
            self.next_task_to_execute = Task(RunBookEndOperator())
            self.tasks_queue.append(None)

    def has_more_tasks_to_execute(self):
        return self.next_task_to_execute is not None
