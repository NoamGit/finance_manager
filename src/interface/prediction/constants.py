from src.interface import INTERFACE_WORKING_DIR
from os.path import join as pjoin

CATEGORY_PREPROCESS_CONFIG_PATH = pjoin(INTERFACE_WORKING_DIR, "prediction/preprocess_config.yaml")
CATEGORY_MODEL_CONFIG_PATH = pjoin(INTERFACE_WORKING_DIR, "prediction/train_config.yaml")
BASE_TRAINING_DATA_BUCKET_PATH = "gs://house-finance/clearml/training_data/category_classification"
BASE_MODEL_STORAGE_BUCKET_PATH = "gs://house-finance/clearml/models"
NAME_COLUMN = "normalized"
TYPE_COLUMN = "type"
CATEGORY_RAW_COLUMN = "category_raw"