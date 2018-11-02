from common.task import Task
import abc


class TaskExecutor(abc.ABC):
    @abc.abstractmethod
    def execute_task(self, task: Task, context: dict):
        return


class LocalTaskExecutor(TaskExecutor):
    def execute_task(self, task: Task, context: dict):
        task.execute(context)


class LambdaTaskExecutor(TaskExecutor):
    def execute_task(self, task: Task, context: dict):
        # TODO - invoke Lambda function to execute task
        return
