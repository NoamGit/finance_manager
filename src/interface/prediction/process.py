from typing import Dict, Any

from omegaconf import OmegaConf
from prefect import task

from src.core.collection.model import MongoCredentials, SERPCredentials
from src.core.prediction.feature_extractor import FeatureExtractorFactory
from src.interface.prediction.constants import CATEGORY_PREPROCESS_CONFIG_PATH
from src.interface.prediction.enrichment_services import DataEnricherFactory


@task()
def load_preprocess_config(run_mode: str = 'train') -> Dict[str, Any]:
    conf = OmegaConf.load(CATEGORY_PREPROCESS_CONFIG_PATH)
    conf_dict = OmegaConf.to_container(conf)
    res = conf_dict.copy()
    serp_block = SERPCredentials.load("serp-cred")
    mongocredentials_block = MongoCredentials.load("mongo-cred")

    feature_extractors = []
    for fe_name, p in conf["feature_extractors"].items():
        extractor = FeatureExtractorFactory().get_feature_extractor(fe_name, param=p)
        feature_extractors.append(extractor)
    for de_name, p in conf["data_enrichers"].items():
        enricher = DataEnricherFactory().get_data_enricher(name=de_name, param=p)
        if de_name == "SERPEnricher":
            enricher.api_key = serp_block.token.get_secret_value()
            enricher.__mongo_param__.update({
                "mongo_host": mongocredentials_block.mongo_host
                , "mongo_port": mongocredentials_block.mongo_port
                , "mongo_password": mongocredentials_block.mongo_password.get_secret_value()
                , "mongo_username": mongocredentials_block.mongo_username.get_secret_value()
            })
        feature_extractors.append(enricher)
    res["feature_extractors"] = feature_extractors
    res['run_mode'] = run_mode
    return res
