import re
import sys
from collections import OrderedDict, defaultdict
from typing import Dict, Any

import pandas as pd

from src.core.common import DATE_FORMAT
from src.core.prediction.model import FeatureExtractor
from src.core.prediction.domain_rules import SWITCH_TERMS, BRANDS, STOPWORDS


class WeekDayExtractor(FeatureExtractor):

    def extract(self, values: pd.Series):
        res = {'month': None, 'weekday': None}
        date_value = pd.to_datetime(values[self.run_columns].values[0], format=DATE_FORMAT)
        res.update({'weekday': date_value.weekday()
                       , 'month': date_value.month
                       , 'day': date_value.day
                    })
        return res


class NameExtractor(FeatureExtractor):
    remove = '|'.join(STOPWORDS)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.stop_regex = re.compile(r'\b(' + self.remove + r')\b')

    def extract(self, values: pd.Series):
        return {"normalized": self.remove_stopwords(
            self.normalize_to_brand(self.switch_terms(values[self.run_columns][0])))}

    def remove_stopwords(self, value: str) -> str:
        return self.stop_regex.sub("", value).strip()

    def switch_terms(self, value: str) -> str:
        switch_terms = defaultdict(str, SWITCH_TERMS)
        terms = '|'.join(switch_terms.keys())
        pattern = re.compile(r'\b(' + terms + r')\b')
        return pattern.sub(lambda x: switch_terms[x.group()], value)

    def normalize_to_brand(self, value: str) -> str:
        for b in BRANDS:
            if b.lower() in value.lower():
                return b.lower()
        return value.lower()


class SemanticTypeCategorySimilarity(FeatureExtractor):
    """
    returns the most similar category by comparing embbeddings of preprocessed entity name and type
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        raise NotImplementedError

    def extract(self, values: pd.Series):
        return


class PurchaseCadenceByName(FeatureExtractor):
    """
    returns a couple of features to quantify purchase cadence of the recent 60 days.
     - count
     - median interval bin (0-7, 7-14, 14-21, 21-28, >28)
     - binned sum amount
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        raise NotImplementedError

    def extract(self, values: pd.Series):
        return


class FeatureExtractorFactory():
    @staticmethod
    def get_feature_extractor(name: str, param: Dict[str, Any]) -> FeatureExtractor:
        try:
            fe = getattr(sys.modules[__name__], name)
            return fe(**param)
        except AttributeError:
            raise ValueError(f'FeatureExtractor {name} is not supported')
