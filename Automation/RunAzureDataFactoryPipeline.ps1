param (
    [string] $AzureDataFactoryName ,
    [string] $DWResourceGroupName
)

$RunId = Invoke-AzDataFactoryV2Pipeline -DataFactoryName $AzureDataFactoryName -ResourceGroupName $DWResourceGroupName -PipelineName 'ControllerMaster' 

while ($True) 
{
    $Run = Get-AzDataFactoryV2PipelineRun -DataFactoryName $AzureDataFactoryName -ResourceGroupName $DWResourceGroupName -PipelineRunId $RunId

    if ($Run) 
    {
        if ( ($Run.Status -ne "InProgress") -and ($Run.Status -ne "Queued") ) 
		{
            Write-Output ("Pipeline run finished. The status is: " +  $Run.Status)
            $Run
            break
        }
        Write-Output ("Pipeline is running...status: " + $Run.Status)
    }

    Start-Sleep -Seconds 10
}