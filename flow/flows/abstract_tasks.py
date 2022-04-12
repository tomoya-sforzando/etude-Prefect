from dataclasses import dataclass

from prefect import Task

from flows.abstract_demands import AbstractDemands

@dataclass
class AbstractTasks:

    def get_by_solution(self, demand: AbstractDemands):
        if demand in (self.__dict__).keys():
            return self.__dict__.get(demand)
        else:
            return None
