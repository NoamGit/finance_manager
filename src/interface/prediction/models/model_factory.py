import sys
from typing import Dict, Any
from src.interface.prediction.models import *

class SupervisedModelFactory():
    def get_model(self, name: str, param: Dict[str, Any])->Any:
        try:
            m = getattr(sys.modules[__name__], name)
        except AttributeError as ex:
            raise ValueError(f'Model {name} is not supported')
        return m(**param)

