from .types import MLBase, pipeline_stage_type


class Piper(MLBase):
    """ """

    def __init__(self):
        super().__init__()

    def pipe(self, input_pipe: Pipeline) -> Pipeline:
        stage = self.add_to_pipe()
        return input_pipe.add(stage)

    @abstractmethod
    def add_to_pipe(self) -> pipeline_stage_type:
        raise NotImplementedError()
