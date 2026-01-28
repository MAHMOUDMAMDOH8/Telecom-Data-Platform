# Copy spark_shared files to the Docker volume
# First, let's check what's in the volume
Write-Host "Checking files in spark_shared volume..."
docker exec spark-master ls -la /opt/spark/shared/

Write-Host "`nCopying files from ./spark_shared to spark-master container..."
# Copy files to the container
docker cp ./spark_shared/. spark-master:/opt/spark/shared/

Write-Host "`nVerifying files were copied..."
docker exec spark-master ls -la /opt/spark/shared/





