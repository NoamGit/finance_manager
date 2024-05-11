import json
from typing import Optional, Tuple, Dict, Any, Callable, List

import pandas as pd
from clearml import Task
from omegaconf import OmegaConf
from prefect import flow, task

from flows.common.tasks.mysql_task import load_to_mysql
from flows.common.tasks.transform_task import preprocess_task
from src.core.common import get_db_secrets, get_date_range
from src.core.prediction.domain_rules import CLASS_2_CLASS_MAP
from src.core.prediction.model import CategoryModelOutput, BaseDomainRule
from src.interface.common.utils import get_mysql_client
from src.interface.prediction.constants import CATEGORY_MODEL_CONFIG_PATH
from src.interface.prediction.entities.clearml_model_registry import ClearMLModelRegistry
from src.interface.prediction.models import CategoryCatboost
from src.interface.prediction.process import load_preprocess_config

PROJECT_NAME = "finance"
TASK_NAME = "category_classification"
MYSQL_TABLE_NAME = "credit_transaction"
PREDICTION_MYSQL_TABLE = "prediction_category"
DEFAULT_MODEL_TYPE = "CategoryCatboost"


@task()
def load_data_to_predict(cred: Dict[str, Any], date_range: Tuple[str, str], table_name: str) -> pd.DataFrame:
    """
    Load data to predict.
    return: A dataframe with data to predict
    """
    start_date, end_date = date_range
    table_name = table_name
    db = get_mysql_client(cred)
    query = f"""
        select t.id
        , t.processed_date
        , t.charged_amount
        , t.description
        , t.category_raw
        , t.account_number
            from {table_name} t 
        WHERE
            date BETWEEN '{start_date}' AND '{end_date}'
    """
    df = pd.read_sql(query, db)
    return df


@task()
def load_latest_model() -> CategoryCatboost:
    """
    Load latest model and transformation pipeline
    return: A tuple with model and transformation pipeline
    """
    task = Task.get_task(project_name=f"{PROJECT_NAME}/{TASK_NAME}")
    model_registry = ClearMLModelRegistry(task)
    return model_registry.read_model(project_name=f"{PROJECT_NAME}/{TASK_NAME}")


def load_latest_pipeline() -> Callable:
    preprocess_config = load_preprocess_config(run_mode="test")
    conf = OmegaConf.load(CATEGORY_MODEL_CONFIG_PATH)
    conf_dict = OmegaConf.to_container(conf)["train_param"]
    feature_col = conf_dict["feature_columns"]

    # TODO: store transformantion pipe as a subflow
    def process_pipe(data, model):
        processed_data = preprocess_task(data, preprocess_config)
        processed_data = model.model_specific_preprocess(processed_data, **conf_dict)
        return processed_data[feature_col]

    return process_pipe


@task()
def predict(data: pd.DataFrame, model):
    prediction = model.predict_proba(data)
    return prediction


@task()
def post_process(prediction: pd.DataFrame, features: pd.DataFrame, data: pd.DataFrame) -> List[CategoryModelOutput]:
    assert prediction.shape[0] == features.shape[0] == data.shape[
        0], "the data, prediction and features are missaligned"
    override_function = BaseDomainRule(input_operator=CLASS_2_CLASS_MAP["operator"], mapping=CLASS_2_CLASS_MAP["rules"])
    full_data = pd.concat((prediction, features, data), axis=1)
    res = []
    for data_slice in full_data.iterrows():
        ind, _data = data_slice
        _data_dict = _data.to_dict()
        proba = _data_dict["proba"]
        overruled = override_function.apply(_data_dict["pred"])
        is_overruled = False if (overruled == -1 or proba == 1.) else True
        pred = overruled if is_overruled else _data_dict["pred"]
        pred = None if pd.isnull(pred) else pred
        f = json.dumps({k: _data_dict[k] for k in features.columns})
        model_output_itr = CategoryModelOutput(
            id=_data_dict["id"],
            overruled=is_overruled,
            features=f,
            predicted_category=pred,
            proba=proba,
        )
        res.append(model_output_itr)
    return res


@flow()
def run_category_classification(start_date: str = None, moth_in_future_to_predict: int = 1):
    db_secrets = get_db_secrets()
    start_to_end_date = get_date_range(start_date=start_date, month_future_to_predict=moth_in_future_to_predict)
    df = load_data_to_predict(cred=db_secrets, date_range=start_to_end_date, table_name=MYSQL_TABLE_NAME)
    model = load_latest_model()
    pipe = load_latest_pipeline()
    preprocess_data = pipe(df, model)
    prediction = predict(preprocess_data, model)
    model_output = post_process(prediction, preprocess_data, df)
    load_to_mysql(model_output, db_secrets, PREDICTION_MYSQL_TABLE)


if __name__ == '__main__':
    run_category_classification(start_date='2023-08-01',moth_in_future_to_predict=5)
