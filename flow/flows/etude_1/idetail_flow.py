from prefect import flatten, unmapped

from flows.etude_1.abstract_flow import AbstractFlow


class IdetailFlow(AbstractFlow):
    flow_name = "idetail_flow"

    def extract(self):
        pass
        # master_csv_paths = self.flow.set_dependencies(
        #     self.e_tasks[0],
        #     upstream_tasks=None,
        #     downstream_tasks=None,
        #     keyword_tasks=None,
        #     mapped=False,
        #     validate=None)
        # # master_csv_paths = [Path("csv/idetail_master.csv")]

        # products = self.flow.set_dependencies(
        #     self.e_tasks[1],
        #     upstream_tasks=None,
        #     downstream_tasks=None,
        #     keyword_tasks=None,
        #     mapped=False,
        #     validate=None)

    def transform(self):
        # resource_data = self.t_tasks[0].map(product_name=products)
        self.flow.set_dependencies(
            self.t_tasks[0],
            # upstream_tasks=[self.e_tasks[1]],
            keyword_tasks={"product_name": self.e_tasks[1]},
            mapped=True)
        # master_data = self.t_tasks[1].map(resource_data=flatten(resource_data), master_csv_path=unmapped(master_csv_paths[0]))
        master_data = self.flow.set_dependencies(
            self.t_tasks[1],
            # upstream_tasks=[self.t_tasks[0], self.e_tasks[0]],
            keyword_tasks={"resource_data": flatten(self.t_tasks[0]), "master_csv_path": unmapped(self.e_tasks[0])},
            mapped=True)

    def load(self):
        # contents_deleted = self.l_tasks[0].map(resource_data=flatten(resource_data))
        self.flow.set_dependencies(
            self.l_tasks[0],
            keyword_tasks={"resource_data": flatten(self.t_tasks[0])},
            mapped=True)
        # contents_registered = self.l_tasks[1].map(
        #     master_data=master_data,
        #     resource_data=flatten(resource_data),
        #     upstream_tasks=[contents_deleted])
        contents_registered = self.flow.set_dependencies(
            self.l_tasks[1],
            upstream_tasks=[self.l_tasks[0]],
            keyword_tasks={"master_data": self.t_tasks[1], "resource_data": flatten(self.t_tasks[0])},
            mapped=True)
        # resources_updated = self.l_tasks[2].map(
        #     product_name=products,
        #     resource_data=resource_data)
        self.flow.set_dependencies(
            self.l_tasks[2],
            keyword_tasks={"product_name": self.e_tasks[1], "resource_data": self.t_tasks[0]},
            mapped=True)
        # self.l_tasks[3].map(
        #     resource_data=resource_data,
        #     upstream_tasks=[contents_registered, resources_updated])
        self.flow.set_dependencies(
            self.l_tasks[3],
            upstream_tasks=[self.l_tasks[1], self.l_tasks[2]],
            keyword_tasks={"resource_data": self.t_tasks[0]},
            mapped=True)
