param(
    [string]$BaseUrl = "http://127.0.0.1:8001",
    [int]$MonthlyRecordId = 3201
)

$ErrorActionPreference = "Stop"

function Invoke-StripeWebhookDemo {
    param(
        [string]$EventId,
        [string]$Outcome
    )

    $uri = "$BaseUrl/api/v1/payments/webhooks/stripe"
    $body = @{
        monthly_record_id = $MonthlyRecordId
        outcome = $Outcome
        event_id = $EventId
    } | ConvertTo-Json -Depth 4

    Write-Host "POST $uri (event_id=$EventId, outcome=$Outcome)"
    $response = Invoke-RestMethod -Uri $uri -Method Post -ContentType "application/json" -Body $body
    $response | ConvertTo-Json -Depth 8
}

$successEventId = "evt_demo_success_$(Get-Date -Format 'yyyyMMddHHmmss')"
$failedEventId = "evt_demo_failed_$(Get-Date -Format 'yyyyMMddHHmmss')"

Write-Host "=== Stripe demo webhook success ==="
Invoke-StripeWebhookDemo -EventId $successEventId -Outcome "success"

Write-Host "=== Stripe demo webhook duplicate (idempotent) ==="
Invoke-StripeWebhookDemo -EventId $successEventId -Outcome "success"

Write-Host "=== Stripe demo webhook failure ==="
Invoke-StripeWebhookDemo -EventId $failedEventId -Outcome "failed"
