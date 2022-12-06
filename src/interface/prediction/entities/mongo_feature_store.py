from datetime import datetime
from typing import Optional, Dict, Any

from src.core.prediction.model import FeatureStore
from src.interface.common.utils import load_to_mongo, read_from_mongo


class MongoFeatureStore(FeatureStore):

    def __init__(self, mongo_param:Dict[str, Any]):
        assert "table_name" in mongo_param
        self.__mongo_param__ = mongo_param
        self.__table_name__ = mongo_param["table_name"]

    def store_features(self, query: str, result: Dict[str, Any]):
        if self.__table_name__:
            doc = result.copy()
            doc['mongo_key'] = query
            doc['update_date'] = datetime.now()
            return load_to_mongo(mongo_doc=doc, mongo_param=self.__mongo_param__, table_name=self.__table_name__)
        raise ValueError(f'__table_name__ is not set')

    def read_features(self, query: str) -> Optional[Dict[str, Any]]:
        if self.__table_name__:
            mongo_query = {"mongo_key": query}
            return read_from_mongo(mongo_query, mongo_param=self.__mongo_param__)
        raise ValueError(f'__table_name__ is not set')