from types import ModuleType
from typing import List, Union

from spetlr.etl import Transformer, dataset_group
from spetlr.spark import Spark
from spetlr.sql import SqlExecutor


class SimpleSqlTransformer(Transformer):
    """This transformer takes as the input the module object
    or import path to where the sql files are that execute
    the work of this transformer.

    All dataset_input_keys will be created as spark temporary views.
    The sql must create a temporary view that has the name in dataset_output_key.
    All sql code int the module will be executed.
    """

    def __init__(
        self,
        *,
        sql_modue: Union[str, ModuleType],
        sql_file_pattern: str = "*",
        dataset_input_keys: List[str] = None,
        dataset_output_key: str = None,
        consume_inputs: bool = True
    ):
        super().__init__(
            dataset_input_keys=dataset_input_keys,
            dataset_output_key=dataset_output_key,
            consume_inputs=consume_inputs,
        )
        self.executor = SqlExecutor(base_module=sql_modue)
        self.sql_file_pattern = sql_file_pattern

    def etl(self, inputs: dataset_group) -> dataset_group:
        # If dataset_input_keys is None, it will be set to all keys in inputs
        dataset_input_keys = self.dataset_input_keys or list(inputs.keys())

        for key in dataset_input_keys:
            inputs[key].createOrReplaceTempView(key)

        self.executor.execute_sql_file(self.sql_file_pattern)

        if self.consume_inputs:
            for key in dataset_input_keys:
                inputs.pop(key)

        # dataset_output_key is added to inputs after pop is called
        # This ensures that dataset_output_key is not popped.
        inputs[self.dataset_output_key] = Spark.get().table(self.dataset_output_key)

        return inputs
