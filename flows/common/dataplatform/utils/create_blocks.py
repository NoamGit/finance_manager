from dotenv import load_dotenv
from flows.common.dataplatform.blocks.workspace import Workspace
from src.core.collection.model import IsracardCredentials, BankCredentials, MongoCredentials, MysqlCredentials
from flows.common.dataplatform.environment import get_env
import os
from prefect.filesystems import GCS
from flows.common.dataplatform.deploy_utils import save_block, DEFAULT_BLOCK

load_dotenv()

if __name__ == '__main__':
    workspace = Workspace(
        name=get_env(),
        block_name=DEFAULT_BLOCK,
        settings=dict(workspace_owner="Noam", environment="Prod"),
    )
    save_block(workspace)

    gcs = GCS(
        bucket_path=os.environ.get("GCS_BUCKET_PATH", DEFAULT_BLOCK),
        service_account_info=os.environ.get("GCS_SERVICE_ACCOUNT_INFO", DEFAULT_BLOCK),
    )
    save_block(gcs, name='gcs-storage')

    noam_isracard_credentials = IsracardCredentials(
        user_name=os.environ.get("ISRACARD_USERNAME_NOAM"),
        cardnum=os.environ.get("ISRACARD_CARDNUM_NOAM"),
        password=os.environ.get("ISRACARD_PASSWORD_NOAM")
    )
    save_block(noam_isracard_credentials, name='noam-isracard-cred')

    eden_isracard_credentials = IsracardCredentials(
        user_name=os.environ.get("ISRACARD_USERNAME_EDEN"),
        cardnum=os.environ.get("ISRACARD_CARDNUM_EDEN"),
        password=os.environ.get("ISRACARD_PASSWORD_EDEN")
    )
    save_block(eden_isracard_credentials, name='eden-isracard-cred')

    otsar_credentials = BankCredentials(
        user_name=os.environ.get("OTSAR_USERNAME"),
        password=os.environ.get("OTSAR_PASSWORD"),
    )
    save_block(otsar_credentials, name='otsar-cred')

    mongo_credentials = MongoCredentials(
        mongo_host=os.environ.get("MONGO_HOST"),
        mongo_port=os.environ.get("MONGO_PORT"),
        mongo_table_name=os.environ.get("MONGO_TABLE_NAME"),
        mongo_username=os.environ.get("MONGO_USERNAME"),
        mongo_password=os.environ.get("MONGO_PASSWORD")
    )
    save_block(mongo_credentials, name='mongo-cred')

    mysql_credentials = MysqlCredentials(
        mysql_password=os.environ.get("MYSQL_PASSWORD"),
        mysql_username=os.environ.get("MYSQL_USER"),
        mysql_database=os.environ.get("MYSQL_DATABASE"),
        mysql_port=os.environ.get("MYSQL_PORT"),
        mysql_root_password=os.environ.get("MYSQL_ROOT_PASSWORD")
    )
    save_block(mysql_credentials, name='mysql-cred')

"""

slack = SlackWebhook(url=os.environ.get("SLACK_WEBHOOK_URL", DEFAULT_BLOCK))
save_block(slack)

snowflake_creds = SnowflakeCredentials(
    user=os.environ.get("SNOWFLAKE_USER", DEFAULT_BLOCK),
    password=os.environ.get("SNOWFLAKE_PASSWORD", DEFAULT_BLOCK),
    account=os.environ.get("SNOWFLAKE_ACCOUNT", DEFAULT_BLOCK),
)
save_block(snowflake_creds)


snowflake_connector = SnowflakeConnector(
    schema=os.environ.get("SNOWFLAKE_SCHEMA", DEFAULT_BLOCK),
    database=os.environ.get("SNOWFLAKE_DATABASE", DEFAULT_BLOCK),
    warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", DEFAULT_BLOCK),
    credentials=SnowflakeCredentials.load(DEFAULT_BLOCK),
)
save_block(snowflake_connector)


dbt_cli_profile = DbtCliProfile(
    name="dbt_dwh_models",
    target=DEFAULT_BLOCK,
    target_configs=SnowflakeTargetConfigs(
        connector=SnowflakeConnector.load(DEFAULT_BLOCK)
    ),
)
save_block(dbt_cli_profile)

pd = SnowflakePandas(snowflake_connector=SnowflakeConnector.load(DEFAULT_BLOCK))
save_block(pd)

dbt_cloud = DbtCloudCredentials(
    account_id=os.environ.get("DBT_CLOUD_ACCOUNT_ID", 12345),
    api_key=os.environ.get("DBT_CLOUD_API_KEY", DEFAULT_BLOCK),
)
save_block(dbt_cloud)

dbt_jaffle_shop = Dbt(
    workspace=Workspace.load(DEFAULT_BLOCK),
    # because it's 3 directories up from dbt flows to the root directory in which dbt project resides
    path_to_dbt_project="dbt_jaffle_shop",
)
save_block(dbt_jaffle_shop, "jaffle-shop")

dbt_attribution = Dbt(
    workspace=Workspace.load(DEFAULT_BLOCK),
    path_to_dbt_project="dbt_attribution",
    default_dbt_cli_emoji="🤖 ",
    default_dbt_model_emoji="💰 ",
    default_dbt_test_emoji="✅ ",
)
save_block(dbt_attribution, "attribution")

gh = GitHub(
    repository="https://github.com/anna-geller/prefect-dataplatform.git",
    reference=os.environ.get("GITHUB_DATAPLATFORM_BRANCH", "main"),
    # access_token is needed for private repositories, supported in Prefect>=2.6.2
    # access_token=os.environ.get("GITHUB_PERSONAL_ACCESS_TOKEN", DEFAULT_BLOCK),
)
save_block(gh)

s3 = S3(
    bucket_path=os.environ.get("AWS_S3_BUCKET_NAME", DEFAULT_BLOCK),
    aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID", DEFAULT_BLOCK),
    aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY", DEFAULT_BLOCK),
)
save_block(s3)

az = Azure(
    bucket_path=os.environ.get("AZURE_BUCKET_PATH", DEFAULT_BLOCK),
    azure_storage_connection_string=os.environ.get(
        "AZURE_STORAGE_CONNECTION_STRING", DEFAULT_BLOCK
    ),
)
save_block(az)
"""
