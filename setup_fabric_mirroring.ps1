# ============================================================================
# Fabric Mirroring - Automated Setup Script
# Creates mirrored databases for Insurance Fabric Accelerator
# ============================================================================

param(
    [Parameter(Mandatory=$true, HelpMessage="Fabric Workspace ID (GUID)")]
    [string]$WorkspaceId,
    
    [Parameter(Mandatory=$false)]
    [string]$SourceServer = "insurance-prod-sql.database.windows.net",
    
    [Parameter(Mandatory=$false)]
    [string]$Username = "mirroring_user",
    
    [Parameter(Mandatory=$false)]
    [SecureString]$Password,
    
    [Parameter(Mandatory=$false)]
    [switch]$UseKeyVault,
    
    [Parameter(Mandatory=$false)]
    [string]$KeyVaultName = "insurance-kv"
)

Write-Host "`n╔════════════════════════════════════════════════════════════════╗" -ForegroundColor Cyan
Write-Host "║  Fabric Mirroring - Automated Setup                           ║" -ForegroundColor Cyan
Write-Host "╚════════════════════════════════════════════════════════════════╝`n" -ForegroundColor Cyan

# ==== STEP 1: Prerequisites Check ====
Write-Host "Checking prerequisites..." -ForegroundColor Yellow

# Check Azure CLI
try {
    $azVersion = (az version | ConvertFrom-Json).'azure-cli'
    Write-Host "✅ Azure CLI version: $azVersion" -ForegroundColor Green
}
catch {
    Write-Host "❌ Azure CLI not found. Install: https://aka.ms/install-azure-cli" -ForegroundColor Red
    exit 1
}

# Check if logged in
try {
    $account = az account show | ConvertFrom-Json
    Write-Host "✅ Logged in as: $($account.user.name)" -ForegroundColor Green
}
catch {
    Write-Host "❌ Not logged in to Azure. Run: az login" -ForegroundColor Red
    exit 1
}

# ==== STEP 2: Get Credentials ====
Write-Host "`nRetrieving credentials..." -ForegroundColor Yellow

if ($UseKeyVault) {
    Write-Host "   Getting password from Key Vault: $KeyVaultName" -ForegroundColor Gray
    $secretValue = az keyvault secret show --name "mirroring-user-password" --vault-name $KeyVaultName --query value -o tsv
    $Password = ConvertTo-SecureString $secretValue -AsPlainText -Force
    Write-Host "✅ Password retrieved from Key Vault" -ForegroundColor Green
}
elseif (-not $Password) {
    $Password = Read-Host "Enter password for $Username" -AsSecureString
}

$plainPassword = [Runtime.InteropServices.Marshal]::PtrToStringAuto(
    [Runtime.InteropServices.Marshal]::SecureStringToBSTR($Password)
)

# ==== STEP 3: Get Access Token ====
Write-Host "`nGetting Fabric access token..." -ForegroundColor Yellow

$tokenResponse = az account get-access-token --resource "https://api.fabric.microsoft.com" | ConvertFrom-Json
$accessToken = $tokenResponse.accessToken
Write-Host "✅ Access token obtained" -ForegroundColor Green

$headers = @{
    "Authorization" = "Bearer $accessToken"
    "Content-Type" = "application/json"
}

# ==== STEP 4: Define Mirroring Configuration ====

$mirroringConfig = @(
    @{
        Name = "PolicySystem_Mirror"
        Description = "Policy Administration System - Real-time policy data"
        SourceDatabase = "PolicyAdministration"
        Tables = @(
            "dbo.policies",
            "dbo.policy_holders",
            "dbo.coverages",
            "dbo.products",
            "dbo.endorsements",
            "dbo.riders"
        )
    },
    @{
        Name = "ClaimsSystem_Mirror"
        Description = "Claims Management System - Real-time claims and FNOL"
        SourceDatabase = "ClaimsManagement"
        Tables = @(
            "dbo.claims",
            "dbo.claim_details",
            "dbo.fnol",
            "dbo.claim_payments",
            "dbo.claim_documents",
            "dbo.adjusters"
        )
    },
    @{
        Name = "FinanceSystem_Mirror"
        Description = "Finance System - Real-time financial transactions"
        SourceDatabase = "Finance"
        Tables = @(
            "dbo.journal_entries",
            "dbo.gl_accounts",
            "dbo.invoices",
            "dbo.payments",
            "dbo.reconciliations"
        )
    },
    @{
        Name = "CRM_Mirror"
        Description = "CRM System - Customer data synchronization"
        SourceDatabase = "CRM"
        Tables = @(
            "dbo.customers",
            "dbo.contacts",
            "dbo.opportunities",
            "dbo.campaigns",
            "dbo.activities"
        )
    }
)

# ==== FUNCTION: Create Mirrored Database ====

function Create-MirroredDatabase {
    param(
        [Parameter(Mandatory=$true)]
        [hashtable]$Config
    )
    
    Write-Host "`n" + ("="*70) -ForegroundColor Cyan
    Write-Host "Creating: $($Config.Name)" -ForegroundColor Cyan
    Write-Host ("="*70) -ForegroundColor Cyan
    
    Write-Host "   Database: $($Config.SourceDatabase)" -ForegroundColor Gray
    Write-Host "   Tables: $($Config.Tables.Count)" -ForegroundColor Gray
    
    $payload = @{
        displayName = $Config.Name
        description = $Config.Description
    } | ConvertTo-Json -Depth 10
    
    $url = "https://api.fabric.microsoft.com/v1/workspaces/$WorkspaceId/mirroredDatabases"
    
    try {
        $response = Invoke-RestMethod -Method Post -Uri $url -Headers $headers -Body $payload -ErrorAction Stop
        $mirrorId = $response.id
        
        Write-Host "✅ Mirrored Database Created" -ForegroundColor Green
        Write-Host "   ID: $mirrorId" -ForegroundColor Gray
        
        # Configure connection and tables
        Write-Host "`n   Configuring connection..." -ForegroundColor Yellow
        
        $connectionPayload = @{
            source = @{
                type = "AzureSQLDatabase"
                server = $SourceServer
                database = $Config.SourceDatabase
                authentication = @{
                    type = "Basic"
                    username = $Username
                    password = $plainPassword
                }
            }
            tables = $Config.Tables
        } | ConvertTo-Json -Depth 10
        
        $configUrl = "https://api.fabric.microsoft.com/v1/workspaces/$WorkspaceId/mirroredDatabases/$mirrorId/configure"
        
        Invoke-RestMethod -Method Post -Uri $configUrl -Headers $headers -Body $connectionPayload -ErrorAction Stop
        Write-Host "   ✅ Connection configured" -ForegroundColor Green
        
        # Start mirroring
        Write-Host "`n   Starting mirroring..." -ForegroundColor Yellow
        
        $startUrl = "https://api.fabric.microsoft.com/v1/workspaces/$WorkspaceId/mirroredDatabases/$mirrorId/startMirroring"
        
        Invoke-RestMethod -Method Post -Uri $startUrl -Headers $headers -ErrorAction Stop
        Write-Host "   ✅ Mirroring started" -ForegroundColor Green
        
        Write-Host "`n   ℹ️  Initial snapshot will complete in 10-30 minutes" -ForegroundColor Cyan
        Write-Host "   ℹ️  Real-time CDC will start automatically after snapshot" -ForegroundColor Cyan
        
        return @{
            Success = $true
            Id = $mirrorId
            Name = $Config.Name
        }
    }
    catch {
        Write-Host "❌ Failed to create mirrored database" -ForegroundColor Red
        Write-Host "   Error: $($_.Exception.Message)" -ForegroundColor Red
        
        return @{
            Success = $false
            Name = $Config.Name
            Error = $_.Exception.Message
        }
    }
}

# ==== STEP 5: Create All Mirrored Databases ====

Write-Host "`n╔════════════════════════════════════════════════════════════════╗" -ForegroundColor Cyan
Write-Host "║  Creating Mirrored Databases ($($mirroringConfig.Count) total)                        ║" -ForegroundColor Cyan
Write-Host "╚════════════════════════════════════════════════════════════════╝`n" -ForegroundColor Cyan

$results = @()

foreach ($config in $mirroringConfig) {
    $result = Create-MirroredDatabase -Config $config
    $results += $result
    
    Start-Sleep -Seconds 3  # Rate limiting
}

# ==== STEP 6: Summary ====

Write-Host "`n╔════════════════════════════════════════════════════════════════╗" -ForegroundColor Cyan
Write-Host "║  Setup Complete - Summary                                      ║" -ForegroundColor Cyan
Write-Host "╚════════════════════════════════════════════════════════════════╝`n" -ForegroundColor Cyan

$successCount = ($results | Where-Object { $_.Success }).Count
$failedCount = ($results | Where-Object { -not $_.Success }).Count

Write-Host "📊 Results:" -ForegroundColor Yellow
Write-Host "   ✅ Successful: $successCount" -ForegroundColor Green
Write-Host "   ❌ Failed: $failedCount" -ForegroundColor $(if ($failedCount -gt 0) { "Red" } else { "Gray" })

if ($successCount -gt 0) {
    Write-Host "`n✅ Successfully Created:" -ForegroundColor Green
    foreach ($result in ($results | Where-Object { $_.Success })) {
        Write-Host "   • $($result.Name)" -ForegroundColor White
        Write-Host "     ID: $($result.Id)" -ForegroundColor Gray
    }
}

if ($failedCount -gt 0) {
    Write-Host "`n❌ Failed:" -ForegroundColor Red
    foreach ($result in ($results | Where-Object { -not $_.Success })) {
        Write-Host "   • $($result.Name)" -ForegroundColor White
        Write-Host "     Error: $($result.Error)" -ForegroundColor Gray
    }
}

# ==== STEP 7: Next Steps ====

Write-Host "`n📋 Next Steps:" -ForegroundColor Yellow
Write-Host "   1. Wait 10-30 minutes for initial snapshot to complete" -ForegroundColor White
Write-Host "   2. Create lakehouse shortcuts to mirrored tables" -ForegroundColor White
Write-Host "   3. Update Notebook 30 to use mirrored data" -ForegroundColor White
Write-Host "   4. Add mirroring health monitoring to Notebook 45" -ForegroundColor White
Write-Host "   5. Test real-time updates from source systems" -ForegroundColor White

Write-Host "`n🔗 Monitor in Fabric:" -ForegroundColor Yellow
Write-Host "   https://app.fabric.microsoft.com/groups/$WorkspaceId" -ForegroundColor Cyan

Write-Host "`n════════════════════════════════════════════════════════════════`n" -ForegroundColor Cyan

# ==== OPTIONAL: Export results to file ====

$resultsJson = $results | ConvertTo-Json -Depth 5
$resultsJson | Out-File "mirroring_setup_results.json"

Write-Host "💾 Results saved to: mirroring_setup_results.json`n" -ForegroundColor Gray
