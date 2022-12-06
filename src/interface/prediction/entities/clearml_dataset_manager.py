import os
from typing import Union, Optional

from clearml import Dataset, StorageManager
import pandas as pd
from pydantic import Json
import tempfile

from src.core.common import hash_dataframe
from src.core.prediction.model import DataSetManager
from src.interface.common.utils import get_logger
from src.interface.prediction.constants import BASE_TRAINING_DATA_BUCKET_PATH


class ClearMLDataSetManager(DataSetManager):

    def __init__(self, project_name: str, dataset_name: Optional[str]):
        self.__project_name__ = project_name
        self._output_path = f"{BASE_TRAINING_DATA_BUCKET_PATH}/{project_name}"
        self.__dataset_name__ = dataset_name
        self.logger = get_logger()

    def read_dataset(self, dataset_id: Optional[str] = None) -> Optional[pd.DataFrame]:
        assert self.has_initialized_dataset() or dataset_id, "dataset_id is not provided"
        dataset_id = dataset_id if dataset_id else self.__dataset_id__
        dataset = self._get_or_create_dataset_obj("get")
        try:
            dataset_path = dataset.get_local_copy()
            if self._is_data_version_already_stored(dataset_id, dataset_path):
                return pd.read_csv(f"{dataset_path}/{dataset_id}.csv")
            else:
                raise ValueError(f"dataset_id {dataset_id} is not found")
        except ValueError as e:
            self.logger.error(f"dataset {dataset_id} is not found")
            return pd.DataFrame()

    def write_dataset(self, data: Union[pd.DataFrame, Json], output_url: Optional[str] = None):
        dataset = self._get_or_create_dataset_obj("get")
        data_version = hash_dataframe(data)
        output_url = output_url if output_url else self._output_path

        if self._is_data_version_already_stored(data_version, dataset):
            self.logger.info(f"dataset {data_version} is already stored")
            return

        dataset = self._get_or_create_dataset_obj("create")
        # Create a temporary directory
        with tempfile.TemporaryDirectory() as tmp_dir:
            file_path = f"{tmp_dir}/{hash_dataframe(data)}.csv"
            data.to_csv(file_path)
            dataset.add_files(file_path)
            # TODO: fix credentials issue with gs for different output location
            dataset.upload()

        # write to output_url using the stroage manager
        dataset.finalize()

    def _is_data_version_already_stored(self, data_version: str, dataset: Union[Dataset, str]) -> bool:
        if isinstance(dataset, str):
            for f in os.listdir(dataset):
                if data_version in f:
                    return True
        elif isinstance(dataset, Dataset):
            for f in dataset.list_files():
                if data_version in f:
                    return True
        return False

    def _get_or_create_dataset_obj(self, work_mode: str = "get") -> Dataset:
        if work_mode == "create":
            return Dataset.create(
                dataset_project=self.__project_name__
                , dataset_name=self.__dataset_name__
            )
        return Dataset.get(
            dataset_project=self.__project_name__
            , dataset_name=self.__dataset_name__
            , auto_create=True
        )
