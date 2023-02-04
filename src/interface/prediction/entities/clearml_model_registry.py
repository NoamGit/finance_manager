import datetime
import json
import os.path
import pickle
import tempfile
from hashlib import sha256
from typing import Optional, Dict, Any, List

from os.path import join as pjoin
from clearml import OutputModel, StorageManager, Model
from clearml.task import TaskInstance
from catboost import CatBoostClassifier

from src.core.prediction.model import ModelRegistry
from src.interface.prediction.models import CategoryCatboost


class ClearMLModelRegistry(ModelRegistry):
    def __init__(self, task: TaskInstance):
        self.task = task

    def read_model(self, uri: Optional[str] = None, project_name: Optional[str] = None) -> Any:
        if uri is None and project_name is None:
            raise ValueError("uri or project_name must be provided")

        model_url = uri
        if project_name and not uri:
            task = self.task.get_task(project_name=project_name) if not self.task.project else self.task
            model_artifacts = task.models.get('output').data
            model_url = self._extract_remote_storage_path(model_artifacts)

        if not model_url:
            raise ValueError("No model found")
        sm = StorageManager()
        local_path = sm.get_local_copy(model_url)
        return self._load_model(local_path)

    def _extract_remote_storage_path(self, file_list: List[Model])->Optional[str]:
        for f in file_list:
            url = f.url
            if "gs://" in url or "//s3." in url or "blob.core.windows" in url:
                return url
        return None

    def register_model(self, model, output_url: Optional[str] = None, config: Dict[str, Any] = None):
        # fixme: add model context - label_enumeration etc.
        name = f"{datetime.datetime.now().strftime('%Y%m%d%H%M')}-{model.__class__.__name__}"
        output_model = OutputModel(task=self.task, config_dict=dict(config), name=name)
        hash_string = sha256(json.dumps(model.get_all_params()).encode('utf-8')).hexdigest()
        sm = StorageManager()

        with tempfile.TemporaryDirectory() as tmp_dir:
            file_path = f"{tmp_dir}/{hash_string}"
            remote_file_url = pjoin(output_url, f"{hash_string}")
            file_path = self._save_model(model, file_path)
            remote_file_url += "." + file_path.split(".")[-1]

            sm.upload_file(local_file=file_path, remote_url=remote_file_url)
            output_model.update_weights(register_uri=remote_file_url)

    def _save_model(self, model, file_path: Optional[str] = None, **kwargs)->str:
        if isinstance(model, CatBoostClassifier):
            if not self._is_file(file_path):
                file_path += ".cbm"
            model.save_model(file_path, **kwargs)
            return file_path
        else:
            raise ValueError("This model type is not supported")

    def _is_file(self, path:str) -> bool:
        return True if '.' in path else False

    def _load_model(self, output_url: Optional[str] = None, **kwargs):
        if not output_url:
            raise FileNotFoundError("filepath is None")

        file_type = output_url.split('.')[-1]
        if file_type == "cbm":
            model = CategoryCatboost()
            model.load_model(output_url, file_type)
            return model
        else:
            raise ValueError("This model type is not supported")


if __name__ == '__main__':
    import clearml

    BASE_MODEL_STORAGE_BUCKET_PATH = "gs://house-finance/clearml/models"
    sm = clearml.StorageManager()
    model_path = BASE_MODEL_STORAGE_BUCKET_PATH + "/category_classification"
    sm.upload_file(local_file="/Users/noam.cohen/projects/finance/flows/common/dataplatform/README.md"
                   , remote_url=model_path + "/README.md")
    sm.download_file(remote_url="gs://house-finance/clearml/models/category_classification/README.md",
                     local_folder="/Users/noam.cohen/projects/finance/src")
