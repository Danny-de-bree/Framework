$SSASPath = (Join-path -Path (Get-Location).Path -ChildPath 'DXC BI Cubes')


##$tabularEditorFile = Get-ChildItem -Path 'Automation\TabularEditor.exe'
##Move-Item -Path "$tabularEditorFile" -Destination (Get-Location)

$allModelDirs = Get-ChildItem -Directory -Path $SSASPath
foreach ($modelDir in $allModelDirs) {
    Write-Host ">> Building " $modelDir.Name "..."
    $modelFileName = $modelDir.Name + '.bim'

    $buildParameters = @(
        ('"' + $modelDir.FullName + '"'),
        "-B `"$modelFileName`"",
        "-V"
    )

    Write-Host "Build parameters:"
    Write-Host $buildParameters

    $processHandle = Start-Process -NoNewWindow -PassThru -Wait -FilePath 'Automation\TabularEditor.exe' -ArgumentList $buildParameters

    if ( $processHandle.ExitCode -ne 0) {
        throw "Module build finished with non zero code!"
    }

    Write-Host ">> Building " $modelDir.Name "... DONE"
    Write-Host "================================`n`n"
}

##Copy-Item -Path 'ODW.bim' -Destination 'model.bim' # Backward compatibility | Line should be removed after switch to new deployment pipeline supporting multiple models