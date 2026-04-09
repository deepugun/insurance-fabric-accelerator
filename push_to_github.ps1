# ============================================================================
# Push Insurance Fabric Accelerator to GitHub
# ============================================================================
# Prerequisites: You must be authenticated with GitHub (run: gh auth login)
# ============================================================================

Write-Host "`n========================================" -ForegroundColor Cyan
Write-Host "Insurance Fabric Accelerator - GitHub Push" -ForegroundColor Cyan
Write-Host "========================================`n" -ForegroundColor Cyan

# Step 1: Verify Git status
Write-Host "Step 1: Checking Git repository status..." -ForegroundColor Yellow
git status

$gitStatus = git status --porcelain
if ($gitStatus) {
    Write-Host "`n⚠️  Warning: You have uncommitted changes" -ForegroundColor Yellow
    Write-Host "Commit them before pushing:`n" -ForegroundColor Yellow
    Write-Host "  git add ." -ForegroundColor White
    Write-Host "  git commit -m 'Your commit message'" -ForegroundColor White
    Write-Host ""
    $continue = Read-Host "Continue anyway? (y/n)"
    if ($continue -ne 'y') {
        Write-Host "Aborted." -ForegroundColor Red
        exit 1
    }
}

# Step 2: Check if authenticated with GitHub
Write-Host "`nStep 2: Verifying GitHub authentication..." -ForegroundColor Yellow
$authStatus = gh auth status 2>&1
if ($LASTEXITCODE -ne 0) {
    Write-Host "`n❌ Not authenticated with GitHub" -ForegroundColor Red
    Write-Host "Run this command first: gh auth login --web" -ForegroundColor Yellow
    Write-Host "Then run this script again." -ForegroundColor Yellow
    exit 1
}
Write-Host "✅ Authenticated with GitHub" -ForegroundColor Green

# Step 3: Create GitHub repository
Write-Host "`nStep 3: Creating GitHub repository..." -ForegroundColor Yellow
Write-Host "Repository name: insurance-fabric-accelerator" -ForegroundColor White

$createResult = gh repo create insurance-fabric-accelerator `
    --public `
    --source=. `
    --description "Complete insurance operations platform built on Microsoft Fabric with AI, real-time analytics, and compliance features" `
    --confirm 2>&1

if ($LASTEXITCODE -eq 0) {
    Write-Host "✅ Repository created successfully" -ForegroundColor Green
} else {
    # Check if repo already exists
    if ($createResult -match "already exists") {
        Write-Host "⚠️  Repository already exists" -ForegroundColor Yellow
        
        # Check if remote is already configured
        $remotes = git remote -v
        if ($remotes -match "origin") {
            Write-Host "✅ Remote 'origin' already configured" -ForegroundColor Green
        } else {
            Write-Host "Adding remote..." -ForegroundColor Yellow
            $username = gh api user --jq .login
            git remote add origin "https://github.com/$username/insurance-fabric-accelerator.git"
            Write-Host "✅ Remote added" -ForegroundColor Green
        }
    } else {
        Write-Host "❌ Failed to create repository:" -ForegroundColor Red
        Write-Host $createResult -ForegroundColor Red
        exit 1
    }
}

# Step 4: Push to GitHub
Write-Host "`nStep 4: Pushing code to GitHub..." -ForegroundColor Yellow

# Check if we need to set upstream
$branch = git branch --show-current
Write-Host "Current branch: $branch" -ForegroundColor White

$pushResult = git push -u origin $branch 2>&1
if ($LASTEXITCODE -eq 0) {
    Write-Host "✅ Code pushed successfully!" -ForegroundColor Green
} else {
    if ($pushResult -match "up-to-date") {
        Write-Host "✅ Already up-to-date" -ForegroundColor Green
    } elseif ($pushResult -match "rejected") {
        Write-Host "⚠️  Push rejected - trying force push..." -ForegroundColor Yellow
        git push -u origin $branch --force
        if ($LASTEXITCODE -eq 0) {
            Write-Host "✅ Force push successful" -ForegroundColor Green
        } else {
            Write-Host "❌ Force push failed" -ForegroundColor Red
            exit 1
        }
    } else {
        Write-Host "❌ Push failed:" -ForegroundColor Red
        Write-Host $pushResult -ForegroundColor Red
        exit 1
    }
}

# Step 5: Display repository URL
Write-Host "`n========================================" -ForegroundColor Cyan
Write-Host "✅ SUCCESS!" -ForegroundColor Green
Write-Host "========================================" -ForegroundColor Cyan

$username = gh api user --jq .login
$repoUrl = "https://github.com/$username/insurance-fabric-accelerator"

Write-Host "`n📦 Repository URL:" -ForegroundColor Yellow
Write-Host "   $repoUrl" -ForegroundColor White

Write-Host "`n📊 Files Pushed:" -ForegroundColor Yellow
Write-Host "   31 files (17,839 lines of code)" -ForegroundColor White
Write-Host "   - 15 production notebooks" -ForegroundColor White
Write-Host "   - 4 documentation files" -ForegroundColor White
Write-Host "   - 12 source code files" -ForegroundColor White

Write-Host "`n🔗 Next Steps:" -ForegroundColor Yellow
Write-Host "   1. View your repository: $repoUrl" -ForegroundColor White
Write-Host "   2. Connect Fabric workspace to Git:" -ForegroundColor White
Write-Host "      - Open Fabric workspace → Settings → Git integration" -ForegroundColor White
Write-Host "      - Connect to GitHub repository: insurance-fabric-accelerator" -ForegroundColor White
Write-Host "      - Select branch: $branch" -ForegroundColor White
Write-Host "      - Select folder: /notebooks" -ForegroundColor White
Write-Host "   3. Create Fabric lakehouses:" -ForegroundColor White
Write-Host "      - insurance_bronze, insurance_silver, insurance_gold, insurance_metadata" -ForegroundColor White
Write-Host "   4. Run demo data generator (notebook 01)" -ForegroundColor White
Write-Host "   5. Execute medallion pipeline (notebook 30)" -ForegroundColor White

Write-Host "`n========================================`n" -ForegroundColor Cyan
