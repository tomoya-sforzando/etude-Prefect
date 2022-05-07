from abc import ABC
from dataclasses import dataclass


@dataclass
class AbstractDemands(ABC):
    pass


@dataclass
class AbstractTasks(ABC):

    def get_all(self):
        return self.__dict__.values()

    def get_by_demand(self, demand: AbstractDemands):
        if demand in self.__dict__.keys():
            return self.__dict__.get(demand)
        else:
            return None
