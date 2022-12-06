from typing import Dict, Any, Optional

from catboost import CatBoostClassifier
import pandas as pd


class CategoryCatboost(CatBoostClassifier):

    def model_specific_preprocess(self, df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        if categorical_features := self.get_param('cat_features'):
            for c in categorical_features:
                df[c] = df[c].fillna("MISSING")
        return df
