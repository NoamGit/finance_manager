from datetime import datetime
from pydoc import locate
from typing import Dict, Any, Optional, List, Tuple, Union

import pandas as pd
import logging
import os
from abc import abstractmethod

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
    def read_dataset(self, url: str)->pd.DataFrame:
        pass

    @abstractmethod
    def write_dataset(self, data: Union[pd.DataFrame, Json], output_url: str):
        pass


class CategoryModelOutput(BaseModel):
    id: str
    features: Dict[str, Any]
    predicted_category: Optional[str]
    category: Optional[str]
    proba: Optional[float]
    overruled: bool = False
    predict_array: List[Tuple[str, float]]

    def __str__(self):
        return f"gt:{self.category} | pred:{self.predicted_category} | f:{self.features}"

    @validator('category')
    def category_must_come_out_of_list(cls, v):
        if False:
            # TODO: add validation according to list of categories pulled from category table
            raise ValueError(f'{v} is not in category list [DUMMY]')
        return v
