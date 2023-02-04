import pandas as pd
import pytest

from src.interface.prediction.process import load_preprocess_config
from src.core.prediction.feature_extractor import WeekDayExtractor, NameExtractor
from src.interface.prediction.enrichment_services import SERPEnricher
from flows.common.tasks.transform_task import preprocess_task


@pytest.fixture
def mock_train_data():
    df=  pd.read_csv("../data/mock_raw_train_data.csv")
    df["processed_date"] = pd.to_datetime(df["processed_date"])
    return df


@pytest.fixture
def mock_config():
    return {
        "feature_extractors": [
            SERPEnricher(query_column='description')
            , WeekDayExtractor(run_columns=['processed_date'], replace_columns=[])
            , NameExtractor(run_columns=['description'], replace_columns=[])
        ]
        , "clean_param": {"exclude_features": {'processed_date', 'description',
                                               'category_raw', 'name', 'id'},
                          "inject_out_of_sample_categories": []
            , "label_col": "category"
            , "run_mode": "train"
            , "categorical_features": ['category', 'weekday', 'name']
                          }
    }


@pytest.mark.skip(reason="TODO: fix this test")
def test_config_creation():
    expected = {
        "feature_extractors": [
            SERPEnricher(query_column='description')
            , WeekDayExtractor(run_columns=['processed_date'], replace_columns=[])
            , NameExtractor(run_columns=['description'], replace_columns=[])
        ]
        , "clean_param": {"exclude_features": {'processed_date', 'description',
                                               'category_raw', 'name', 'id'},
                          "inject_out_of_sample_categories": []
            , "label_col": "category"
            , "run_mode": "train"
            , "categorical_features": ['category', 'weekday', 'name']
                          }
    }
    input_config = {
        "feature_extractors": {
            "SERPEnricher": {"query_column": 'description'}
            , "WeekDayExtractor": {"run_columns": ['processed_date'], "replace_columns": []}
            , "NameExtractor": {"run_columns": ['description'], "replace_columns": []}
        }
        , "clean_param": {"exclude_features": {'processed_date', 'description',
                                               'category_raw', 'name', 'id'},
                          "inject_out_of_sample_categories": []
            , "label_col": "category"
            , "run_mode": "train"
            , "categorical_features": ['category', 'weekday', 'name']
                          }
    }
    load_preprocess_config()


def test_preprocess_task(mock_train_data, mock_config):
    res = preprocess_task.fn(mock_train_data, mock_config)
    expected = pd.read_csv("../data/mock_preprocessed_train_data.csv")
    expected = expected.loc[res.index][res.columns]
    assert expected.compare(res).empty
