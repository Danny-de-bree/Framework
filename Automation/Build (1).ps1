# Install Nuget and Pester to allow for tests to run
Install-PackageProvider -Name NuGet -Force -Scope CurrentUser
Install-Module -Name Pester -Force -Verbose -Scope CurrentUser
Import-Module Pester

# Initialize variables
[Int32]$Failedtests = 0
$path = (Join-path -Path (get-location).Path -ChildPath "Tests\ADF")
$ADFPath = (Join-path -Path (get-location).Path -ChildPath "ADF")

# Run thru all tests for ADF
Get-ChildItem  $path -Filter *.tests.ps1 -Recurse | ForEach-Object {
    
    # Deduct test name from file name
    $testName = [regex]::matches($_.Name, ".+?(?=\.tests\.ps1)", "IgnoreCase") 

    # Prepare an output file for test
    $OutputFile = join-path $_.Directory ( "TEST-" + $testName.Value + ".xml")

    #$OutputFile

    # Invoke Pester for test
    $PesterRun = Invoke-Pester -OutputFormat NUnitXml  -OutputFile $OutputFile -PassThru -Script @{ Path = $_.FullName; Parameters = @{ADFPath = $ADFPath; }}

    # Propagate errors to total counter
    $Failedtests += $PesterRun.FailedCount
}

if ($Failedtests -gt 0) {
    Write-Error ("Number of failed tests have surpassed 0")
    exit 1
}

