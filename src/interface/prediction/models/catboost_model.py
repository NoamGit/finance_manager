from typing import Dict, Any, Optional, Union

from catboost import CatBoostClassifier
import pandas as pd
import numpy as np

from src.core.prediction.domain_rules import TYPE_2_CLASS_MAP, NAME_2_CLASS_MAP, CATEGORY_RAW_2_CLASS_MAP
from src.core.prediction.model import BaseDomainRule
from src.interface.prediction.constants import NAME_COLUMN, TYPE_COLUMN, CATEGORY_RAW_COLUMN


class CategoryCatboost(CatBoostClassifier):
    def __init__(self, **params: Dict[str, Any]):
        use_domain_rules = params.pop("use_domain_rules", True)
        super().__init__(**params)
        self._load_domain_rules()
        self.use_domain_rules = use_domain_rules

    def model_specific_preprocess(self, df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        categorical_features = [self.feature_names_[x] for x in self.get_cat_feature_indices()] if self.is_fitted() else self.get_param("cat_features")
        if categorical_features:
            for c in categorical_features:
                df[c] = df[c].fillna("MISSING")
        return df

    def _load_domain_rules(self):
        self.name_rules = BaseDomainRule(input_operator=NAME_2_CLASS_MAP["operator"], mapping=NAME_2_CLASS_MAP["rules"])
        self.type_rules = BaseDomainRule(input_operator=TYPE_2_CLASS_MAP["operator"], mapping=TYPE_2_CLASS_MAP["rules"])
        self.category_raw_rules = BaseDomainRule(input_operator=CATEGORY_RAW_2_CLASS_MAP["operator"],
                                                 mapping=CATEGORY_RAW_2_CLASS_MAP["rules"])

    def predict(self, data, prediction_type='Class', ntree_start=0, ntree_end=0, thread_count=-1, verbose=None,
                task_type="CPU"):
        """
        - 'RawFormulaVal' : return raw formula value.
        - 'Class' : return class label.
        - 'Probability' : return one-dimensional numpy.ndarray with probability for every class.
        - 'LogProbability' : return one-dimensional numpy.ndarray with
          log probability for every class.
                  """

        # Use the loaded domain rules and fallbacks to modify the model's predictions
        predictions = super().predict(data, prediction_type, ntree_start, ntree_end, thread_count, verbose, task_type)

        if self.use_domain_rules:
            predictions = self._update_prediction_with_domain_rules(predictions, data)
        return predictions

    def predict_proba(self, data, ntree_start=0, ntree_end=0, thread_count=-1, verbose=None, task_type="CPU"):
        probas = super().predict_proba(data, ntree_start, ntree_end, thread_count, verbose, task_type)
        predictions = super().predict(data, 'Class', ntree_start, ntree_end, thread_count, verbose, task_type)

        max_probas = np.max(probas, axis=1)
        if self.use_domain_rules:
            rule_predictions = self._update_prediction_with_domain_rules(predictions.squeeze(), data)
            overruled_predictions_index = (rule_predictions != predictions.squeeze())
            max_probas[overruled_predictions_index] = 1.

        return pd.DataFrame(np.stack((rule_predictions, max_probas, overruled_predictions_index)).T,
                            columns=["pred", "proba", "overruled"])

    def _update_prediction_with_domain_rules(self, predictions: np.ndarray, data: pd.DataFrame) -> np.ndarray:
        predictions = predictions.astype(float)
        for k, data_pred in enumerate(zip(data.iterrows(), predictions)):
            _data, _pred = data_pred
            _index, _data = _data
            _data_dict = _data.to_dict()
            label_pred = self.category_raw_rules.apply(_data_dict[CATEGORY_RAW_COLUMN])
            label_pred = self.type_rules.apply(_data_dict[TYPE_COLUMN]) \
                if label_pred == -1 else label_pred
            label_pred = self.name_rules.apply(_data_dict[NAME_COLUMN]) \
                if label_pred == -1 else label_pred
            predictions[k] = label_pred if label_pred != -1 else _pred
        return predictions
