from dataclasses import dataclass


@dataclass
class AbstractDemands:
    pass


@dataclass
class AbstractTasks:

    def get_by_demand(self, demand: AbstractDemands):
        if demand in (self.__dict__).keys():
            return self.__dict__.get(demand)
        else:
            return None
