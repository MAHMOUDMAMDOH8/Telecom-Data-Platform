from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month, dayofmonth, hour, date_format, to_date
from pyspark.sql.functions import broadcast

# Note: In a real environment, you'd include the ClickHouse JDBC jar in your --jars or packages
spark = SparkSession.builder \
    .appName("SilverToGold_Calls") \
    .config("spark.jars.packages", "com.clickhouse:clickhouse-jdbc:0.4.6,org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.5.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.local.type", "hadoop") \
    .config("spark.sql.catalog.local.warehouse", "s3a://telecom_lakehouse/warehouse") \
    .getOrCreate()

# Reading the User Dim (denormalized JSON)
# For a small JSON string/file, you can use:
users_df = spark.read.json("s3a://telecom_lakehouse/source/telecom-generator/DIM_USER.json")

# Reading the Site Dim
sites_df = spark.read.json("s3a://telecom_lakehouse/source/telecom-generator/dim_cell_site.json")

# Reading the Device Dim
devices_df = spark.read.json("s3a://telecom_lakehouse/source/telecom-generator/DIM_DEVICE.json")


def enrich_fact_with_time(df, ts_col="timestamp"):
    return df.withColumn("event_date", to_date(col(ts_col))) \
             .withColumn("year", year(col(ts_col))) \
             .withColumn("month", month(col(ts_col))) \
             .withColumn("day", dayofmonth(col(ts_col))) \
             .withColumn("hour", hour(col(ts_col))) \
             .withColumn("date_key", date_format(col(ts_col), "yyyyMMdd").cast("int"))


clickhouse_options = {
    "url": "jdbc:clickhouse://localhost:8123/default",
    "dbtable": "fact_calls_gold",
    "user": "default",
    "password": "",
    "driver": "com.clickhouse.jdbc.ClickHouseDriver"
}

# Reading the Calls Fact from Iceberg table
calls_silver = spark.read.table("local.calls").filter("rejection_reason IS NULL")
calls_enriched = enrich_fact_with_time(calls_silver, ts_col="timestamp")

calls_fact_final = calls_enriched.alias("c") \
    .join(broadcast(users_df).alias("u"), col("c.from_phone_number") == col("u.msisdn"), "left") \
    .join(broadcast(devices_df).alias("d"), col("c.imei") == col("d.imei"), "left") \
    .select(
        "c.sid", "c.timestamp", "c.event_date", "c.date_key", "c.year", "c.month", "c.hour",
        "c.from_phone_number", "c.to_phone_number", "c.call_duration_seconds", "c.amount", "c.call_type",
        "u.customer_type", "u.city", "u.age_group", # User attributes
        "d.brand", "d.model", "d.is_smartphone"     # Device attributes
    )
# Writing the final Calls Fact to ClickHouse
calls_fact_final.write \
    .format("jdbc") \
    .options(**clickhouse_options) \
    .mode("append") \
    .save()


# 1. Read Silver SMS
sms_silver = spark.read.table("local.sms").filter("rejection_reason IS NULL")
sms_enriched = enrich_fact_with_time(sms_silver)

# 2. Join with Dims
sms_fact_final = sms_enriched.alias("s") \
    .join(broadcast(users_df).alias("u"), col("s.from_phone_number") == col("u.msisdn"), "left") \
    .join(broadcast(devices_df).alias("d"), col("s.imei") == col("d.imei"), "left") \
    .select(
        "s.sid", "s.timestamp", "s.event_date", "s.date_key", "s.year", "s.month",
        "s.from_phone_number", "s.to_phone_number", "s.amount", "s.status",
        "u.customer_type", "u.city", "d.brand", "d.model"
    )

# 3. Write to ClickHouse
sms_fact_final.write.format("jdbc").options(**clickhouse_options).option("dbtable", "fact_sms_gold").mode("append").save()

# --- PAYMENTS ---
pay_silver = spark.read.table("local.payment").filter("rejection_reason IS NULL")
pay_fact = enrich_fact_with_time(pay_silver).alias("p") \
    .join(broadcast(users_df).alias("u"), col("p.phone_number") == col("u.msisdn"), "left") \
    .select(
        "p.sid", "p.timestamp", "p.event_date", "p.date_key",
        "p.payment_amount", "p.payment_method", "p.status", "p.transaction_id",
        "u.customer_type", "u.city"
    )

pay_fact.write.format("jdbc").options(**clickhouse_options) \
    .option("dbtable", "fact_payments_gold").mode("append").save()

# --- RECHARGE ---
rech_silver = spark.read.table("local.recharge").filter("rejection_reason IS NULL")
rech_fact = enrich_fact_with_time(rech_silver).alias("r") \
    .join(broadcast(users_df).alias("u"), col("r.phone_number") == col("u.msisdn"), "left") \
    .select(
        "r.sid", "r.timestamp", "r.event_date", "r.date_key",
        "r.recharge_amount", "r.balance_before", "r.balance_after",
        "u.customer_type", "u.city"
    )

rech_fact.write.format("jdbc").options(**clickhouse_options) \
    .option("dbtable", "fact_recharge_gold").mode("append").save()

supp_silver = spark.read.table("local.support").filter("rejection_reason IS NULL")
supp_fact = enrich_fact_with_time(supp_silver).alias("sup") \
    .join(broadcast(users_df).alias("u"), col("sup.phone_number") == col("u.msisdn"), "left") \
    .select(
        "sup.sid", "sup.timestamp", "sup.event_date", "sup.date_key",
        "sup.wait_time_seconds", "sup.resolution_time_seconds", "sup.satisfaction_score",
        "sup.reason", "sup.channel", "sup.agent_id",
        "u.customer_type", "u.city"
    )

supp_fact.write.format("jdbc").options(**clickhouse_options) \
    .option("dbtable", "fact_support_gold").mode("append").save()