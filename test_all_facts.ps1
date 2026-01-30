# PowerShell script to test each fact table individually
# This will help identify which fact table is causing issues

Write-Host "`n=== Testing Each Fact Table Individually ===" -ForegroundColor Cyan
Write-Host "This will run each fact table one by one to identify any issues`n" -ForegroundColor Yellow

$facts = @("calls", "payment", "recharge", "sms", "support")

foreach ($fact in $facts) {
    Write-Host "`n" + "="*80 -ForegroundColor Cyan
    Write-Host "Testing fact_$fact..." -ForegroundColor Yellow
    Write-Host "="*80 -ForegroundColor Cyan
    
    $result = & .\run_single_fact.ps1 -FactName $fact
    
    if ($LASTEXITCODE -eq 0) {
        Write-Host "`n[SUCCESS] fact_$fact completed successfully!`n" -ForegroundColor Green
    } else {
        Write-Host "`n[FAILED] fact_$fact failed!`n" -ForegroundColor Red
        Write-Host "Stopping tests. Fix the issue with fact_$fact before continuing." -ForegroundColor Yellow
        break
    }
    
    Start-Sleep -Seconds 2  # Small delay between tests
}

Write-Host "`n=== All Fact Table Tests Complete ===" -ForegroundColor Cyan

