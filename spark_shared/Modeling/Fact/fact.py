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
from Fact_calls import *
from Fact_payment import *
from Fact_rechage import *
from Fact_sms import *
from Fact_support import *


spark = get_spark_session()
clickhouse_base_options = clickhouse_conne(clickhouse_url, clickhouse_host,
                                            clickhouse_port, clickhouse_db,
                                            clickhouse_user, clickhouse_password, 
                                            clickhouse_ssl, clickhouse_compress)

if __name__ == "__main__":
    import sys
    
    # Allow running individual fact tables by passing table name as argument
    if len(sys.argv) > 1:
        table_name = sys.argv[1].lower()
        logger.info(f"Loading single fact table: {table_name}")
        
        if table_name == "calls":
            load_fact_calls(spark, clickhouse_base_options)
        elif table_name == "payment":
            load_fact_payment(spark, clickhouse_base_options)
        elif table_name == "recharge":
            load_fact_recharge(spark, clickhouse_base_options)
        elif table_name == "sms":
            load_fact_sms(spark, clickhouse_base_options)
        elif table_name == "support":
            load_fact_support(spark, clickhouse_base_options)
        else:
            logger.error(f"Unknown table name: {table_name}")
            sys.exit(1)
    else:
        # Run all fact tables
        facts = [
            ("calls", load_fact_calls),
            ("payment", load_fact_payment),
            ("recharge", load_fact_recharge),
            ("sms", load_fact_sms),
            ("support", load_fact_support)
        ]
        
        for fact_name, load_func in facts:
            try:
                logger.info(f"Starting fact_{fact_name} load...")
                load_func(spark, clickhouse_base_options)
                logger.info(f"fact_{fact_name} loaded successfully")
            except Exception as e:
                logger.error(f"Error loading fact_{fact_name}: {e}", exc_info=True)
                logger.error(f"Stopping execution. Previous tables may have been loaded.")
                raise
    
    logger.info("All fact tables loaded successfully!")


