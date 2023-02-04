from datetime import datetime
from pydoc import locate
from typing import Dict, Any, Optional, List, Tuple, Union

import pandas as pd
import logging
import os
from abc import abstractmethod
import operator

from pydantic import BaseModel, validator, Json


class FeatureExtractor():
    def __init__(self, run_columns: List[str]
                 , replace_columns: List[str]):
        self.run_columns = run_columns
        self.replace_columns = replace_columns

    @abstractmethod
    def extract(self, values: pd.Series):
        pass


class DataProcessor():

    def __init__(self, logger: Optional[logging.Logger], *args, **kwargs):
        self.logger = logger if logger else logging.getLogger(__name__)
        self.version = os.getenv('GIT_COMMIT_SHA')

    @abstractmethod
    def preprocess_data(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Preprocess data
        return: A dataframe with preprocessed data
        """
        pass

    @abstractmethod
    def postprocess_data(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Postprocess data
        return: A dataframe with postprocessed data
        """
        pass


class FeatureStore():

    @abstractmethod
    def store_features(self, query: str, result: Dict[str, Any]):
        """
        Store feature
        """
        pass

    def read_features(self, query: str) -> Optional[Dict[str, Any]]:
        return


class AbstractEnricher():
    __table_name__ = None
    __mongo_param__ = None
    feature_store: FeatureStore()

    @abstractmethod
    def extract(self, query: pd.Series):
        pass

    @abstractmethod
    def _fetch_from_data_provider(self, query: str) -> Dict[str, Any]:
        pass

    def _fetch(self, query: str):
        if cached_res := self.feature_store.read_features(query):
            return cached_res

        res = self._fetch_from_data_provider(query)
        self.store_features(query, res)
        return res

    def store_features(self, query: str, result: Dict[str, Any]):
        return self.feature_store.store_features(query, result)

    def read_features(self, query: str) -> Optional[Dict[str, Any]]:
        return self.feature_store.read_features(query)

    def process_result(self, response: Dict[str, Any]):
        return response


class DataSetManager():
    __dataset_id__: Optional[str] = None

    @property
    def dataset_id(self):
        return self.__dataset_id__

    @dataset_id.setter
    def dataset_id(self, value):
        self.__dataset_id__ = value

    def has_initialized_dataset(self):
        if self.__dataset_id__:
            return True
        return False

    @abstractmethod
    def read_dataset(self, url: str) -> pd.DataFrame:
        pass

    @abstractmethod
    def write_dataset(self, data: Union[pd.DataFrame, Json], output_url: str):
        pass


class ModelRegistry():
    __model_id__: Optional[str] = None

    @property
    def model_id(self):
        return self.__model_id__

    @model_id.setter
    def model_id(self, value):
        self.__model_id__ = value

    @abstractmethod
    def read_model(self, uri: str):
        pass

    @abstractmethod
    def register_model(self, model, output_url: str):
        pass


class CategoryModelOutput(BaseModel):
    id: str
    features: str
    predicted_category: Optional[int]
    proba: Optional[float]
    overruled: bool = False

    def __str__(self):
        return f"pred:{self.predicted_category} | f:{self.features}"

    @validator('proba')
    def proba_must_be_positive(cls, v):
        if v < 0:
            raise ValueError(f'{v} is negative')
        return v


class BaseDomainRule():
    def __init__(self, input_operator: str, mapping: Dict[str, str], logger: Optional[logging.Logger] = None):
        operator_map = operator.__dict__
        assert input_operator in list(operator_map), f"Operator {input_operator} not found in operator module"
        self.operator = operator_map[input_operator]
        self.mapping = mapping
        self.logger = logger if logger else logging.getLogger(__name__)

    @abstractmethod
    def apply(self, value: Union[float, str, int]) -> Optional[Union[float, str, int, bool]]:
        if self.operator == operator.contains:
            return self._check_contain_rule(value)
        elif self.operator == operator.eq:
            return self.mapping.get(value, -1)

    def _check_contain_rule(self, value: Union[float, str, int]) -> Optional[Union[float, str, int]]:
        for k, v in self.mapping.items():
            if value in k:
                return v
        return -1
