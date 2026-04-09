# ============================================================================
# Import Notebooks to Fabric - PowerShell Wrapper
# ============================================================================

param(
    [Parameter(Mandatory=$true)]
    [string]$WorkspaceId,
    
    [Parameter(Mandatory=$false)]
    [string]$NotebooksDir = "notebooks",
    
    [Parameter(Mandatory=$false)]
    [switch]$Overwrite
)

Write-Host "`n========================================" -ForegroundColor Cyan
Write-Host "Fabric Notebook Import - PowerShell Wrapper" -ForegroundColor Cyan
Write-Host "========================================`n" -ForegroundColor Cyan

# Check if Python is installed
Write-Host "Checking Python installation..." -ForegroundColor Yellow
$pythonVersion = python --version 2>&1
if ($LASTEXITCODE -ne 0) {
    Write-Host "❌ Python not found. Please install Python 3.8+: https://python.org" -ForegroundColor Red
    exit 1
}
Write-Host "✅ $pythonVersion" -ForegroundColor Green

# Check if Azure CLI is installed
Write-Host "`nChecking Azure CLI installation..." -ForegroundColor Yellow
$azVersion = az version 2>&1 | ConvertFrom-Json
if ($LASTEXITCODE -ne 0) {
    Write-Host "❌ Azure CLI not found. Please install: https://aka.ms/install-azure-cli" -ForegroundColor Red
    exit 1
}
Write-Host "✅ Azure CLI version $($azVersion.'azure-cli')" -ForegroundColor Green

# Check if logged in to Azure
Write-Host "`nChecking Azure authentication..." -ForegroundColor Yellow
$azAccount = az account show 2>&1
if ($LASTEXITCODE -ne 0) {
    Write-Host "❌ Not logged in to Azure. Running 'az login'..." -ForegroundColor Yellow
    az login
    if ($LASTEXITCODE -ne 0) {
        Write-Host "❌ Azure login failed" -ForegroundColor Red
        exit 1
    }
}
$accountInfo = az account show | ConvertFrom-Json
Write-Host "✅ Logged in as: $($accountInfo.user.name)" -ForegroundColor Green
Write-Host "   Subscription: $($accountInfo.name)" -ForegroundColor White

# Install Python dependencies
Write-Host "`nChecking Python dependencies..." -ForegroundColor Yellow
$requirementsFile = Join-Path $PSScriptRoot "requirements-fabric-import.txt"

if (Test-Path $requirementsFile) {
    Write-Host "Installing dependencies from requirements file..." -ForegroundColor Yellow
    python -m pip install -q -r $requirementsFile
    if ($LASTEXITCODE -eq 0) {
        Write-Host "✅ Dependencies installed" -ForegroundColor Green
    }
} else {
    Write-Host "Installing requests library..." -ForegroundColor Yellow
    python -m pip install -q requests
    if ($LASTEXITCODE -eq 0) {
        Write-Host "✅ Requests library installed" -ForegroundColor Green
    }
}

# Run the Python script
Write-Host "`n========================================" -ForegroundColor Cyan
Write-Host "Starting Notebook Import..." -ForegroundColor Cyan
Write-Host "========================================`n" -ForegroundColor Cyan

$scriptPath = Join-Path $PSScriptRoot "import_notebooks_to_fabric.py"

$command = "python `"$scriptPath`" --workspace-id `"$WorkspaceId`" --notebooks-dir `"$NotebooksDir`""

if ($Overwrite) {
    $command += " --overwrite"
}

Write-Host "Command: $command`n" -ForegroundColor Gray

Invoke-Expression $command

if ($LASTEXITCODE -eq 0) {
    Write-Host "`n========================================" -ForegroundColor Cyan
    Write-Host "✅ IMPORT COMPLETED SUCCESSFULLY!" -ForegroundColor Green
    Write-Host "========================================" -ForegroundColor Cyan
    
    Write-Host "`n🔗 Next Steps:" -ForegroundColor Yellow
    Write-Host "   1. Open workspace: https://app.fabric.microsoft.com" -ForegroundColor White
    Write-Host "   2. Create lakehouses (if not exists):" -ForegroundColor White
    Write-Host "      - insurance_bronze" -ForegroundColor White
    Write-Host "      - insurance_silver" -ForegroundColor White
    Write-Host "      - insurance_gold" -ForegroundColor White
    Write-Host "      - insurance_metadata" -ForegroundColor White
    Write-Host "   3. Attach lakehouses to notebooks" -ForegroundColor White
    Write-Host "   4. Run 01_demo_data_generator.ipynb" -ForegroundColor White
    Write-Host "`n"
} else {
    Write-Host "`n❌ Import failed with exit code: $LASTEXITCODE" -ForegroundColor Red
    exit $LASTEXITCODE
}
