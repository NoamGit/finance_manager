from typing import List, Union, Optional, Set, Dict, Any

import pandas as pd
from catboost import CatBoostClassifier

from src.core.prediction.model import DataProcessor, AbstractEnricher, FeatureExtractor


class CategoryModelDataProcessor(DataProcessor):

    def __init__(self, conf: Dict[str, Any], *args, **kwargs):
        self.config = conf
        super().__init__(*args, **kwargs)

    def _validate_config(self):
        assert "clean_param" in self.config
        assert "feature_extractors" in self.config
        for obj in self.config["feature_extractors"]:
            assert isinstance(obj,(FeatureExtractor, AbstractEnricher))
        if "train" == self.config["clean_param"].get("run_mode"):
            assert "label_col" in self.config["clean_param"]

    def preprocess_data(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Preprocess data
        return: A dataframe with preprocessed data
        """
        self._validate_config()
        self.logger.info("Preprocessing data...")
        fe_data = self._extract_features(data, feature_extractors=self.config["feature_extractors"])
        self.logger.info("step 1 - finished feature extraction and enrichments")
        cleaned_data = self._clean_data(fe_data, exclude_features=self.config.get("exclude_features"))
        self.logger.info("step 2 - finished cleaning data")
        return cleaned_data

    def _extract_features(self, data: pd.DataFrame,
                          feature_extractors: List[Union[FeatureExtractor, AbstractEnricher]]) -> pd.DataFrame:
        extracted_data = [data.apply(fe.extract, axis=1, result_type='expand') for fe in feature_extractors]
        features = pd.concat([data, *extracted_data], axis=1)
        return features

    def _clean_data(self, data: pd.DataFrame, exclude_features: Optional[Set[str]] = None) -> pd.DataFrame:
        features_to_exclude = set(exclude_features) if exclude_features else set()
        label_col = self.config["clean_param"].get("label_col")
        include_features = list(set(data.columns).difference(features_to_exclude))
        res = data[include_features]
        res = res.where(pd.notnull(res), None)
        # TODO: add out_of_sample categories by removing category in order to allow extrapolation for model
        if self.config.get("inject_out_of_sample_categories"):
            raise NotImplementedError("inject_out_of_sample_categories is not implemented yet")
        if self.config.get("run_mode") == 'train':
            res = self._remove_infrequent_categories(res, min_freq=5, label_col=label_col)
            res = res[~res[label_col].isin({'IGNORE','Investment'})]  # don't train on IGNORE
            res = res.drop_duplicates()
        return res

    def _remove_infrequent_categories(self, data: pd.DataFrame, label_col: str,
                                      min_freq: Optional[int] = 1) -> pd.DataFrame:
        value_counts = data[label_col].value_counts()
        valid_label = list(value_counts[value_counts >= min_freq].index)
        return data[data[label_col].isin(valid_label)]
