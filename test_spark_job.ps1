# PowerShell command to test Spark transformation job
# Run this command in PowerShell to test the Spark script
# Uses Java system properties to set Ivy cache directory
# Note: Make sure files are copied to the spark_shared volume first using copy_files_to_spark_volume.ps1

Write-Host "Checking if files exist in spark_shared volume..."
docker exec spark-master ls -la /opt/spark_shared/from_bronze_to_silver.py

if ($LASTEXITCODE -ne 0) {
    Write-Host "Files not found! Please run .\copy_files_to_spark_volume.ps1 first"
    exit 1
}

Write-Host "Running Spark job..."
docker exec spark-master bash -c "mkdir -p /tmp/.ivy2/cache /tmp/.ivy2/jars && /opt/spark/bin/spark-submit --master spark://spark-master:7077 --driver-java-options '-Divy.cache.dir=/tmp/.ivy2 -Divy.home=/tmp/.ivy2' --packages org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.5.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 --conf spark.hadoop.fs.s3a.endpoint=https://expert-pancake-jv9wx6vww5w25gj4-4566.app.github.dev/ --conf spark.hadoop.fs.s3a.access.key=test --conf spark.hadoop.fs.s3a.secret.key=test --conf spark.hadoop.fs.s3a.path.style.access=true --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem --conf spark.hadoop.fs.s3a.connection.ssl.enabled=true --conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog --conf spark.sql.catalog.local.type=hadoop --conf spark.sql.catalog.local.warehouse=s3a://telecom_lakehouse/silver_layer --driver-memory 1g --executor-memory 2g --executor-cores 2 --num-executors 2 --py-files /opt/spark_shared/init.py,/opt/spark_shared/transformations.py,/opt/spark_shared/scheam.py /opt/spark_shared/from_bronze_to_silver.py"

