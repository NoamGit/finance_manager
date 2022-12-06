import os
from typing import Dict, Any

import pandas as pd
import prefect
from prefect import task, get_run_logger

from src.core.common import get_git_commit_sha
from src.core.prediction.process import CategoryModelDataProcessor
from src.interface.common.utils import get_logger


@task(version=get_git_commit_sha())
def preprocess_task(data: pd.DataFrame, config:Dict[str,Any]) -> pd.DataFrame:
    """
    Preprocess data
    return: A dataframe with preprocessed data
    """
    logger = get_logger()
    processor = CategoryModelDataProcessor(conf=config, logger = logger)
    return processor.preprocess_data(data)
