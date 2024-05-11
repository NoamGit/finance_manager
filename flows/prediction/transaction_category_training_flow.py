from typing import Dict, Any, Tuple, Optional

import numpy as np
import pandas as pd
from clearml import Task, TaskTypes, Logger
from clearml.task import TaskInstance
from omegaconf import OmegaConf
from prefect import flow, task
from sklearn.model_selection import train_test_split
from sklearn.metrics import confusion_matrix, classification_report

from src.core.common import get_db_secrets, get_date_range, set_db_secrets_as_env_variables, hash_dataframe
from src.core.prediction.model import DataSetManager
from src.interface.common.utils import get_mysql_client, get_logger
from src.interface.prediction.constants import CATEGORY_MODEL_CONFIG_PATH, \
    BASE_MODEL_STORAGE_BUCKET_PATH
from flows.common.tasks.transform_task import preprocess_task
from src.interface.prediction.entities.clearml_dataset_manager import ClearMLDataSetManager
from src.interface.prediction.entities.clearml_model_registry import ClearMLModelRegistry
from src.interface.prediction.models.catboost_model import CategoryCatboost
from src.interface.prediction.models.model_factory import SupervisedModelFactory
from src.interface.prediction.process import load_preprocess_config

# region flow CONSTANTS
PROJECT_NAME = "finance"
TASK_NAME = "category_classification"
DATASET_NAME = f"training_{TASK_NAME}"
MYSQL_TABLE_NAME = "credit_transaction"
DEFAULT_MODEL_TYPE = "CategoryCatboost"


@task()
def init_clearml() -> Tuple[TaskInstance, DataSetManager]:
    experiment_name = f"{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}_training"
    task = Task.init(project_name=f"{PROJECT_NAME}/{TASK_NAME}"
                     , task_name=experiment_name
                     , task_type=TaskTypes.training
                     , auto_resource_monitoring=False
                     )
    data_manager = ClearMLDataSetManager(project_name=PROJECT_NAME, dataset_name=DATASET_NAME)
    return task, data_manager


@task()
def get_db_settings(env:Optional[str] = None):
    db_secrets = get_db_secrets(env)
    db_secrets.update({'mysql_table_name': MYSQL_TABLE_NAME})
    set_db_secrets_as_env_variables(db_secrets)
    return db_secrets


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
                , cred.category_raw
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
def split_data(data: pd.DataFrame, label: str, **train_config) -> Tuple[Dict[str, pd.DataFrame], ...]:
    X_train, X_test, y_train, y_test = train_test_split(data[train_config["feature_columns"]], data[label]
                                                        , train_size=train_config.get("train_size", 0.75),
                                                        random_state=42,
                                                        )
    train_data = {'data': X_train, 'label': y_train}
    test_data = {'data': X_test, 'label': y_test}
    return train_data, test_data


@task()
def train_model(model: CategoryCatboost
                , train_data: Dict[str, pd.DataFrame]
                , test_data: Optional[Dict[str, pd.DataFrame]] = None
                , **train_args) -> CategoryCatboost:
    X_train, X_test = map(lambda x: x['data'], [train_data, test_data])
    model.fit(X_train, train_data['label'], eval_set=(X_test, test_data['label']),
              **train_args)
    return model


@task()
def train_all_model(model: CategoryCatboost
                    , ref_model: CategoryCatboost
                    , data: Dict[str, pd.DataFrame]
                    , label_col: str
                    , train_config: Optional[Dict[str, Any]] = None
                    , experiment_task: Optional[TaskInstance] = None
                    , **train_args):
    X = data[train_config["train_param"]["feature_columns"]]
    y = data[label_col]
    model.set_params(**ref_model.get_params())
    model.set_params(n_estimators=ref_model.get_best_iteration(),
                     use_best_model=False)
    model.fit(X, y, **train_args)
    if experiment_task:
        registry = ClearMLModelRegistry(task=experiment_task)
        registry.register_model(model=model, output_url=BASE_MODEL_STORAGE_BUCKET_PATH + "/category_classification",
                                config=train_config)


@task()
def evaluate_model(model: CategoryCatboost, test_data: Dict[str, pd.DataFrame], cred: Optional[Dict[str, str]] = None):
    db = get_mysql_client(cred)
    query = f"select c.id, c.category from category c"
    cat_table = pd.read_sql(query, db)
    cat_table.set_index("id", inplace=True)

    X_test = test_data['data']
    y = test_data['label'].__array__(float)[:, np.newaxis]
    y_hat = model.predict(X_test)
    not_na_index = pd.notnull(y_hat)
    not_null_coverage = 100 * (not_na_index.sum() / len(y_hat))

    scalar_metrics = {"not null coverage": not_null_coverage}
    feature_imp_artifact = pd.DataFrame(model.get_feature_importance(), index=X_test.columns)
    y_cat_notnull = cat_table.loc[y[not_na_index]].values
    y_hat_cat_notnull = cat_table.loc[y_hat[not_na_index]].values
    classification_report_artifact = pd.DataFrame(
        classification_report(y_cat_notnull, y_hat_cat_notnull, output_dict=True)).T
    confusion_matrix_artifact = confusion_matrix(y_cat_notnull, y_hat_cat_notnull)

    error_index = y[not_na_index] != y_hat[not_na_index]
    X_error = X_test[not_na_index][error_index].copy()
    X_error['proba'] = model.predict_proba(X_error[X_test.columns])['proba']
    X_error['y_hat'] = cat_table.loc[y_hat[not_na_index][error_index]].values
    X_error['y'] = cat_table.loc[y[not_na_index][error_index]].values
    X_error.sort_values(by='proba', ascending=False, inplace=True)

    clearml_logger = Logger.current_logger()
    clearml_logger.report_table(
        title="feature importance",
        series="features",
        table_plot=feature_imp_artifact
    )
    clearml_logger.report_table(
        title="Error table",
        series="errors",
        table_plot=X_error
    )
    clearml_logger.report_table(title="classification report"
                                , series="classification_report"
                                , table_plot=classification_report_artifact)
    clearml_logger.report_confusion_matrix(title="confusion matrix"
                                           , series="confusion_matrix"
                                           , matrix=confusion_matrix_artifact)
    clearml_logger.report_table(title="other metrics", series="other_metrics",
                                table_plot=pd.DataFrame(scalar_metrics, index=["metric"]).T)


@flow()
def train_transaction_category_classifier(start_date: str = None, moth_in_future_to_predict: int = 1,
                                          dataset_id: str = None, env: Optional[str]=None):
    experimentation_task, datamanager = init_clearml()
    db_secrets = get_db_settings(env)
    preprocess_config = load_preprocess_config()
    train_config = load_train_config()
    catboost_model = load_model_from_config()
    start_to_end_date = get_date_range(start_date=start_date
                                       , month_future_to_predict=moth_in_future_to_predict
                                       , dataset_id=dataset_id)

    df = load_data_to_predict(cred=db_secrets, date_range=start_to_end_date, datamanager=datamanager,
                              dataset_id=dataset_id)
    df = preprocess_task(df, preprocess_config)
    df = model_specific_preprocess_task(df, catboost_model, train_config)
    train_data, validation_data = split_data(df, label=preprocess_config['clean_param']['label_col'],
                                             **train_config)
    evaluated_catboost_model = train_model(catboost_model, train_data, validation_data)
    evaluate_model(evaluated_catboost_model, validation_data, cred=db_secrets)
    final_catboost_model_final = load_model_from_config()
    train_all_model(final_catboost_model_final
                    , ref_model=evaluated_catboost_model
                    , data=df
                    , label_col=preprocess_config['clean_param']['label_col']
                    , train_config=OmegaConf.load(CATEGORY_MODEL_CONFIG_PATH)
                    , experiment_task=experimentation_task)


if __name__ == '__main__':
    train_transaction_category_classifier(start_date='2020-01-01'
                                          , moth_in_future_to_predict=43
                                          , env = "PROD"
                                          # , dataset_id='db82adc0c22e922b4f7adb1a69e97f2e'
                                          )
