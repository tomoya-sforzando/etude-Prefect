from abc import ABC
from dataclasses import dataclass

from prefect import Task


@dataclass
class AbstractMetaTask(ABC):

    def get_by_demand(self, task_on_demand: Task):
        if task_on_demand.name in self.__dict__.keys():
            return self.__dict__.get(task_on_demand.name)
