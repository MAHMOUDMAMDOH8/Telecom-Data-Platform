param(
    [string]$ClickhouseUrl = "jdbc:clickhouse:https://expert-pancake-jv9wx6vww5w25gj4-8123.app.github.dev:443/default",
    [string]$ClickhouseHost = "",
    [string]$ClickhousePort = "8123",
    [string]$ClickhouseDb = "telecom_warehouse",
    [string]$ClickhouseUser = "default",
    [string]$ClickhousePassword = "clickhouse",
    [string]$ClickhouseSsl = "true",
    [string]$ClickhouseCompress = "0"
)

Write-Host "`n=== Loading Silver Layer (Iceberg) -> ClickHouse ===" -ForegroundColor Cyan

# Check if spark-master container is running
Write-Host "`n[1/4] Checking if spark-master container is running..." -ForegroundColor Yellow
$containerStatus = docker ps --filter "name=spark-master" --format "{{.Status}}" 2>$null
if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace($containerStatus)) {
    Write-Host "ERROR: spark-master container is not running!" -ForegroundColor Red
    Write-Host "Please start Docker containers first: docker-compose up -d" -ForegroundColor Yellow
    exit 1
}
Write-Host "   [OK] spark-master is running" -ForegroundColor Green

# Prepare environment variables
Write-Host "`n[2/4] Preparing ClickHouse connection..." -ForegroundColor Yellow
$cleanHost = $ClickhouseHost -replace '^https?://', '' -replace '/$', ''
if ([string]::IsNullOrWhiteSpace($cleanHost)) {
    $cleanHost = "clickhouse"
}

$envPrefix = "CLICKHOUSE_PORT=$ClickhousePort CLICKHOUSE_DB=$ClickhouseDb CLICKHOUSE_USER=$ClickhouseUser CLICKHOUSE_PASSWORD=$ClickhousePassword CLICKHOUSE_SSL=$ClickhouseSsl CLICKHOUSE_COMPRESS=$ClickhouseCompress"
if (-not [string]::IsNullOrWhiteSpace($ClickhouseUrl)) {
    $envPrefix = "CLICKHOUSE_URL=$ClickhouseUrl $envPrefix"
    Write-Host "   [OK] Using URL: $ClickhouseUrl" -ForegroundColor Green
} else {
    $envPrefix = "CLICKHOUSE_HOST=$cleanHost $envPrefix"
    Write-Host "   [OK] Using Host: ${cleanHost}:${ClickhousePort}" -ForegroundColor Green
}

# Build spark-submit command for dimensions
Write-Host "`n[3/4] Running Spark job..." -ForegroundColor Yellow
Write-Host "   (This may take a few minutes...)`n" -ForegroundColor Gray

Write-Host "Loading dimensions to ClickHouse..." -ForegroundColor Yellow
$dimCmd = "set -e; " +
    "mkdir -p /tmp/.ivy2/cache /tmp/.ivy2/jars; " +
    "$envPrefix /opt/spark/bin/spark-submit " +
    "--master spark://spark-master:7077 " +
    "--driver-java-options '-Divy.cache.dir=/tmp/.ivy2 -Divy.home=/tmp/.ivy2' " +
    "--conf spark.jars.ivy=/tmp/.ivy2 " +
    "--packages com.clickhouse:clickhouse-jdbc:0.4.6,org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.5.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 " +
    "--conf spark.hadoop.fs.s3a.endpoint=https://expert-pancake-jv9wx6vww5w25gj4-4566.app.github.dev/ " +
    "--conf spark.hadoop.fs.s3a.access.key=test " +
    "--conf spark.hadoop.fs.s3a.secret.key=test " +
    "--conf spark.hadoop.fs.s3a.path.style.access=true " +
    "--conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem " +
    "--conf spark.hadoop.fs.s3a.connection.ssl.enabled=true " +
    "--conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog " +
    "--conf spark.sql.catalog.local.type=hadoop " +
    "--conf spark.sql.catalog.local.warehouse=s3a://telecomlakehouse/iceberg " +
    "--py-files /opt/spark_shared/Modeling/utils.py " +
    "/opt/spark_shared/Modeling/Dims/dimension.py"

$dimOutput = docker exec spark-master bash -lc "$dimCmd" 2>&1
$dimExitCode = $LASTEXITCODE

if ($dimExitCode -ne 0) {
    Write-Host "`n[ERROR] Dimension loading failed with exit code $dimExitCode`n" -ForegroundColor Red
    Write-Host "Last 50 lines of output:" -ForegroundColor Yellow
    $dimOutput | Select-Object -Last 50
    exit $dimExitCode
}

Write-Host "   [OK] Dimensions loaded successfully!" -ForegroundColor Green

# Build spark-submit command for facts
Write-Host "`nLoading facts to ClickHouse..." -ForegroundColor Yellow
$factCmd = "set -e; " +
    "mkdir -p /tmp/.ivy2/cache /tmp/.ivy2/jars; " +
    "$envPrefix /opt/spark/bin/spark-submit " +
    "--master spark://spark-master:7077 " +
    "--driver-java-options '-Divy.cache.dir=/tmp/.ivy2 -Divy.home=/tmp/.ivy2' " +
    "--conf spark.jars.ivy=/tmp/.ivy2 " +
    "--packages com.clickhouse:clickhouse-jdbc:0.4.6,org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.5.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 " +
    "--conf spark.hadoop.fs.s3a.endpoint=https://expert-pancake-jv9wx6vww5w25gj4-4566.app.github.dev/ " +
    "--conf spark.hadoop.fs.s3a.access.key=test " +
    "--conf spark.hadoop.fs.s3a.secret.key=test " +
    "--conf spark.hadoop.fs.s3a.path.style.access=true " +
    "--conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem " +
    "--conf spark.hadoop.fs.s3a.connection.ssl.enabled=true " +
    "--conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog " +
    "--conf spark.sql.catalog.local.type=hadoop " +
    "--conf spark.sql.catalog.local.warehouse=s3a://telecomlakehouse/iceberg " +
    "--py-files /opt/spark_shared/Modeling/utils.py " +
    "/opt/spark_shared/Modeling/Fact/fact.py"

$output = docker exec spark-master bash -lc "$factCmd" 2>&1
$exitCode = $LASTEXITCODE

if ($exitCode -eq 0) {
    Write-Host "   [OK] Spark job completed successfully!" -ForegroundColor Green
    
    # Verify data loaded
    Write-Host "`n[4/4] Verifying data in ClickHouse..." -ForegroundColor Yellow
    
    try {
        $verifyQuery = "SELECT count(*) as calls, (SELECT count(*) FROM fact_sms) as sms, (SELECT count(*) FROM fact_payment) as payment, (SELECT count(*) FROM fact_recharge) as recharge, (SELECT count(*) FROM fact_support) as support FROM fact_calls"
        $counts = docker exec spark-master bash -lc "curl -sS --max-time 10 'https://expert-pancake-jv9wx6vww5w25gj4-8123.app.github.dev/?query=SELECT+count(*)+as+calls,+(SELECT+count(*)+FROM+fact_sms)+as+sms,+(SELECT+count(*)+FROM+fact_payment)+as+payment,+(SELECT+count(*)+FROM+fact_recharge)+as+recharge,+(SELECT+count(*)+FROM+fact_support)+as+support+FROM+fact_calls&user=$ClickhouseUser&password=$ClickhousePassword'" 2>$null
        
        if ($LASTEXITCODE -eq 0 -and $counts -and $counts -match '^\d') {
            $countArray = $counts -split '\s+'
            Write-Host "`n   Row counts in ClickHouse:" -ForegroundColor Cyan
            Write-Host "      - fact_calls:    $($countArray[0]) rows" -ForegroundColor White
            Write-Host "      - fact_sms:      $($countArray[1]) rows" -ForegroundColor White
            Write-Host "      - fact_payment:  $($countArray[2]) rows" -ForegroundColor White
            Write-Host "      - fact_recharge: $($countArray[3]) rows" -ForegroundColor White
            Write-Host "      - fact_support:  $($countArray[4]) rows" -ForegroundColor White
        } else {
            Write-Host "   [WARN] Could not verify row counts (but load completed)" -ForegroundColor Yellow
        }
    } catch {
        Write-Host "   [WARN] Could not verify row counts (but load completed)" -ForegroundColor Yellow
    }
    
    Write-Host "`n[SUCCESS] All data loaded to ClickHouse!`n" -ForegroundColor Green
} else {
    Write-Host "`n[ERROR] Spark job failed with exit code $exitCode`n" -ForegroundColor Red
    Write-Host "Last 50 lines of output:" -ForegroundColor Yellow
    $output | Select-Object -Last 50
    exit $exitCode
}
