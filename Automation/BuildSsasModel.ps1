
param(
	$modelDirName 
)

$SSASPath = (Join-path -Path (Get-Location).Path -ChildPath 'DXC BI Cubes\')

##$tabularEditorFile = Get-ChildItem -Path 'Automation\TabularEditor.exe'
##Move-Item -Path "$tabularEditorFile" -Destination (Get-Location)

    Write-Host ">> Building " $modelDirName "..."
    $modelFileName = $modelDirName + '.bim'
	$modelDir = $SSASPath + $modelDirName
	
    $buildParameters = @(
        ('"' + $modelDir + '"'),
        "-B `"$modelFileName`"",
        "-V"
    )

    Write-Host "Build parameters:"
    Write-Host $buildParameters

    $processHandle = Start-Process -NoNewWindow -PassThru -Wait -FilePath 'Automation\TabularEditor.exe' -ArgumentList $buildParameters

    if ( $processHandle.ExitCode -ne 0) {
        throw "Module build finished with non zero code!"
    }

    Write-Host ">> Building " $modelDirName "... DONE"
    Write-Host "================================`n`n"

##Copy-Item -Path 'ODW.bim' -Destination 'model.bim' # Backward compatibility | Line should be removed after switch to new deployment pipeline supporting multiple models