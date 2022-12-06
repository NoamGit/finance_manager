from typing import Dict, Any, Tuple, Optional, List

from os.path import join as pjoin

import pandas as pd
from clearml import Task, TaskTypes
from omegaconf import OmegaConf
from prefect import flow, task
from sklearn.model_selection import train_test_split

from src.core.common import get_db_secrets, get_date_range, set_db_secrets_as_env_variables, hash_dataframe
from src.core.prediction.feature_extractor import FeatureExtractorFactory
from src.core.prediction.model import DataSetManager
from src.interface import INTERFACE_WORKING_DIR
from src.interface.common.utils import get_mysql_client, get_logger
from src.interface.prediction.constants import CATEGORY_PREPROCESS_CONFIG_PATH, CATEGORY_MODEL_CONFIG_PATH
from src.interface.prediction.enrichment_services import DataEnricherFactory
from flows.common.tasks.transform_task import preprocess_task
from src.interface.prediction.entities.clearml_dataset_manager import ClearMLDataSetManager
from src.interface.prediction.models.catboost_model import CategoryCatboost
from src.interface.prediction.models.model_factory import SupervisedModelFactory

# region flow CONSTANTS
PROJECT_NAME = "finance"
TASK_NAME = "category_classification"
DATASET_NAME = f"training_{TASK_NAME}"
MYSQL_TABLE_NAME = "credit_transaction"
DEFAULT_MODEL_TYPE = "CategoryCatboost"


@task()
def init_clearml() -> DataSetManager:
    experiment_name = f"{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}_training"
    Task.init(project_name=f"{PROJECT_NAME}/{TASK_NAME}"
              , task_name=experiment_name
              , task_type=TaskTypes.training
              , auto_resource_monitoring=False
              )
    data_manager = ClearMLDataSetManager(project_name=PROJECT_NAME, dataset_name=DATASET_NAME)
    return data_manager


@task()
def get_db_settings():
    db_secrets = get_db_secrets()
    db_secrets.update({'mysql_table_name': MYSQL_TABLE_NAME})
    set_db_secrets_as_env_variables(db_secrets)
    return db_secrets


@task()
def load_preprocess_config() -> Dict[str, Any]:
    conf = OmegaConf.load(CATEGORY_PREPROCESS_CONFIG_PATH)
    conf_dict = OmegaConf.to_container(conf)
    res = conf_dict.copy()
    feature_extractors = []
    for fe_name, p in conf["feature_extractors"].items():
        extractor = FeatureExtractorFactory().get_feature_extractor(fe_name, param=p)
        feature_extractors.append(extractor)
    for de_name, p in conf["data_enrichers"].items():
        enricher = DataEnricherFactory().get_data_enricher(name=de_name, param=p)
        feature_extractors.append(enricher)
    res["feature_extractors"] = feature_extractors
    res['run_mode'] = 'train'
    return res


@task()
def load_model_from_config() -> Any:
    # TODO move to pydantic baseclass
    conf = OmegaConf.load(CATEGORY_MODEL_CONFIG_PATH)
    conf_dict = OmegaConf.to_container(conf).copy()
    assert 'model' in conf_dict
    model_name = conf_dict['model'].pop('model_type', DEFAULT_MODEL_TYPE)
    return SupervisedModelFactory().get_model(name=model_name, param=conf_dict['model'])


@task()
def load_train_config() -> Dict[str, str]:
    # TODO move to pydantic baseclass
    conf = OmegaConf.load(CATEGORY_MODEL_CONFIG_PATH)
    conf_dict = OmegaConf.to_container(conf)
    assert 'train_param' in conf_dict
    assert 'feature_columns' in conf_dict["train_param"]
    assert 'train_size' in conf_dict["train_param"]
    return conf_dict['train_param']


@task()
def model_specific_preprocess_task(data: pd.DataFrame, model: CategoryCatboost,
                                   model_config: Dict[str, str]) -> pd.DataFrame:
    return model.model_specific_preprocess(data, **model_config)


@task()
def load_data_to_predict(cred: Optional[Dict[str, str]] = None
                         , date_range: Optional[Tuple[str, str]] = None
                         , datamanager: Optional[ClearMLDataSetManager] = None
                         , limit: Optional[bool] = None
                         , **kwargs) -> pd.DataFrame:
    logger = get_logger()
    if dataset_id := kwargs.get('dataset_id'):
        logger.info(f"dataset id is identified and retrieved from remote storage")
        df = datamanager.read_dataset(dataset_id)
        table_name = "clearml_dataset"
    else:
        db = get_mysql_client(cred)
        table_name = cred.get('mysql_table_name')
        query = f"""
            select cred.id
                , cred.processed_date
                , cred.charged_amount
                , cred.description
                , cat.category as category_name
                , cred.category
                , cred.account_number
            from {table_name} cred 
                join category cat
                    on cred.category = cat.id 
                where 
                    cred.category is not null
                    and cred.processed_date between '{date_range[0]}' and '{date_range[1]}'
                order by 
                    cred.processed_date desc
        """
        if limit:
            query += f" limit {limit}"
        df = pd.read_sql(query, db)
        datamanager.dataset_id = hash_dataframe(df)
        datamanager.write_dataset(df)
    logger.info(f"Loaded {len(df)} rows from {table_name}")
    assert df.shape[0] > 200, "Not enough data to train"
    return df


@task()
def split_data(data: pd.DataFrame, label: str, train_size: float = 0.75) -> Tuple[Dict[str, pd.DataFrame], ...]:
    X_train, X_test, y_train, y_test = train_test_split(data, data[label], train_size=train_size, random_state=42,
                                                        )
    train_data = {'data': X_train, 'label': y_train}
    test_data = {'data': X_test, 'label': y_test}
    return train_data, test_data


@task()
def train_model(model: CategoryCatboost, train_data: Dict[str, pd.DataFrame], test_data: Dict[str, pd.DataFrame],
                feature_columns: Optional[List[str]] = None, **kwargs) -> CategoryCatboost:
    feature_columns = feature_columns if feature_columns else train_data['data'].columns
    X_train, X_test = map(lambda x: x['data'][feature_columns], [train_data, test_data])
    model.fit(X_train, train_data['label'], eval_set=(X_test, test_data['label']),
              **kwargs)
    return model


@flow()
def train_transaction_category_classifier(start_date: str = None, moth_in_future_to_predict: int = 1,
                                          dataset_id: str = None):
    datamanager = init_clearml()
    db_secrets = get_db_settings()
    preprocess_config = load_preprocess_config()
    train_config = load_train_config()
    catboost_model = load_model_from_config()
    start_to_end_date = get_date_range(start_date=start_date
                                       , month_future_to_predict=moth_in_future_to_predict
                                       , dataset_id=dataset_id)

    df = load_data_to_predict(cred=db_secrets, date_range=start_to_end_date, datamanager = datamanager, dataset_id=dataset_id)
    df = preprocess_task(df, preprocess_config)
    df = model_specific_preprocess_task(df, catboost_model, train_config)
    train_data, validation_data = split_data(df, label=preprocess_config['clean_param']['label_col'],
                                             train_size=train_config.get('train_size', 0.75))
    catboost_model = train_model(catboost_model, train_data, validation_data,
                                 feature_columns=train_config['feature_columns'])


if __name__ == '__main__':
    train_transaction_category_classifier(start_date='2020-01-01'
                                          , moth_in_future_to_predict=30
                                          , dataset_id='55462f864f3c4b03382972f5b1775af2'
                                          )
