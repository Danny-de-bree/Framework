param (
    [Parameter(Mandatory=$true)][string]$ResourceGroup,
    [Parameter(Mandatory=$true)][string]$SSASServer
)

$asSrv = Get-AzAnalysisServicesServer -Name $SSASServer -ResourceGroupName $ResourceGroup
$AASServiceStatus = $asSrv.State

Write-Output "Azure Analysis Services Original Status: $AASServiceStatus"

	IF($asSrv.State -eq "Paused")
	{
		Write-Output "Server is paused. Resuming!"
		$asSrv | Resume-AzAnalysisServicesServer -WarningVariable CapturedWarning
		Write-Output "Server Resumed."
	}
	IF($asSrv.State -ne "Paused")
	{
		Write-Output "Server is already online!"
	}
		
Write-Host "##vso[task.setvariable variable=AASServiceStatus]" $AASServiceStatus
