import os
import re
import sys
from collections import OrderedDict
from typing import Dict, Any

import pandas as pd
from serpapi import GoogleSearch

from src.core.prediction.domain_rules import TYPE_NORMALIZATIONS, NAME_2_TYPE_RULES
from src.core.prediction.model import AbstractEnricher
from src.interface.prediction.entities.mongo_feature_store import MongoFeatureStore


class DunsEnricher(AbstractEnricher):
    # https: // www.dunsguide.co.il /
    def extract(self, query: pd.Series) -> Dict[str, Any]:
        raise NotImplementedError


class SERPEnricher(AbstractEnricher):
    __table_name__ = 'features_serp'

    def __init__(self, query_column: str):
        super().__init__()
        self.preprocess_regx = re.compile('[^A-Za-zא-ת &0-1]')
        self.api_key = os.getenv('SERP_API')
        self.query_column = query_column
        self.__mongo_param__ = {p: os.getenv(str(p).upper()) for p in
                                ['mongo_host', 'mongo_port', 'mongo_password', 'mongo_username']}
        self.__mongo_param__['table_name'] = self.__table_name__
        self.feature_store = MongoFeatureStore(mongo_param=self.__mongo_param__)

    def extract(self, query: pd.Series):
        query_str = self._preprocess(query[self.query_column])
        response = self._fetch(query_str)
        return self._process_result(response)

    def _preprocess(self, query: str) -> str:
        query = query.replace('.', ' ')
        return self.preprocess_regx.sub("", query)

    def _fetch_from_data_provider(self, query: str) -> Dict[str, Any]:
        params = {
            "q": query,
            "hl": "en",
            "gl": "il",
            "api_key": self.api_key
        }

        search = GoogleSearch(params)
        return search.get_dict()

    def _process_result(self, response: Dict[str, Any]):
        res = {'type': None, 'name': None}
        if "knowledge_graph" in response and 'title' in response.get('knowledge_graph', {}):
            res['name'] = response['knowledge_graph'].get('title')
            res['type'] = response['knowledge_graph'].get('type')
        if 'organic_results' in response:
            res['name'] = response.get('organic_results')[0]['title']
        old_type = res['type']
        res['type'] = self._preprocess_type(res['type'])
        res['type'] = self._fix_type_by_rules(response['search_parameters']['q'], old_type)
        # TODO add location lon,lat
        if res['type']:
            res['type'] = self._post_process_type(res['type'])
        return res

    def _preprocess_type(self, entity_type: str) -> str:
        if entity_type:
            for k, v in TYPE_NORMALIZATIONS.items():
                if k in entity_type:
                    return v
        return entity_type

    def _post_process_type(self, business_type: str):
        if ' in ' in business_type:
            return business_type.split(' in ')[0]
        return business_type

    def _fix_type_by_rules(self, name: str, current_type: str) -> str:
        for k, v in OrderedDict(NAME_2_TYPE_RULES).items():
            if k in name:
                return v
        return current_type


class GmapsEnricher(SERPEnricher):
    __table_name__ = 'features_gmaps'

    def extract(self, query: pd.Series) -> Dict[str, Any]:
        if isinstance(query, pd.Series):
            query = query[self.query_column]
        res = self._fetch(query)
        return self._process_result(res)

    def _fetch_from_data_provider(self, query: str) -> Dict[str, Any]:
        params = {
            "engine": "google_maps",
            "q": query,
            "hl": "en",
            "ll": "@32.182785,34.865594, 12z",
            "type": "search",
            "api_key": self.api_key
        }
        search = GoogleSearch(params)
        return search.get_dict()

    def _process_result(self, response: Dict[str, Any]):
        res = {'gmap_type': None, 'gmap_name': None}
        if "place_results" in response:
            res['gmap_name'] = response['place_results']['title']
            res['gmap_type'] = response['place_results']['type']
        return res


class DataEnricherFactory():
    @staticmethod
    def get_data_enricher(name: str, param: Dict[str, Any]) -> AbstractEnricher:
        try:
            de = getattr(sys.modules[__name__], name)
            return de(**param)
        except AttributeError:
            raise ValueError(f'Enricher {name} is not supported')
