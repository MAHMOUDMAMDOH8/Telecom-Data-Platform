import os
import sys
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col




clickhouse_url = os.getenv("CLICKHOUSE_URL")
clickhouse_host = os.getenv("CLICKHOUSE_HOST", "clickhouse")
clickhouse_port = os.getenv("CLICKHOUSE_PORT", "8123")
clickhouse_db = os.getenv("CLICKHOUSE_DB", "telecom_warehouse")
clickhouse_user = os.getenv("CLICKHOUSE_USER", "default")
clickhouse_password = os.getenv("CLICKHOUSE_PASSWORD", "clickhouse")
clickhouse_ssl = os.getenv("CLICKHOUSE_SSL")
clickhouse_compress = os.getenv("CLICKHOUSE_COMPRESS")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

scripts_dir = '/opt/spark_shared/Modeling'
if scripts_dir not in sys.path:
    sys.path.append(scripts_dir)

from utils import *
from Dims.Dim_cell_site import *
from Dims.DIm_user import *
from Dims.dim_device import *
from Dims.dim_agent import *
from Dims.Dim_Date import *
from Dims.Dim_time import *


spark = get_spark_session()
clickhouse_base_options = clickhouse_conne(clickhouse_url, clickhouse_host,
                                            clickhouse_port, clickhouse_db,
                                            clickhouse_user, clickhouse_password, 
                                            clickhouse_ssl, clickhouse_compress)

if __name__ == "__main__":
    load_dim_cell_site(spark, clickhouse_base_options)
    load_dim_user(spark, clickhouse_base_options)
    load_dim_device(spark, clickhouse_base_options)
    load_dim_agent(spark, clickhouse_base_options)
    load_dim_date(spark, clickhouse_base_options)
    load_dim_time(spark, clickhouse_base_options)

