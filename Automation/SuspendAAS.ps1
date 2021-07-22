param (
    [Parameter(Mandatory=$true)][string]$ResourceGroup,
    [Parameter(Mandatory=$true)][string]$SSASServer,
	[Parameter(Mandatory=$true)][string]$AASServiceStatus
)
 
$asSrv = Get-AzAnalysisServicesServer -Name $SSASServer -ResourceGroupName $ResourceGroup

Write-Output "Azure Analysis Services Original Status: $AASServiceStatus"

	IF($AASServiceStatus -eq "Paused")
	{
		Write-Output "Server Original suspended. Suspending!"
		$asSrv | Suspend-AzAnalysisServicesServer -WarningVariable CapturedWarning
		Write-Output "Server Suspended."
	}
	
	IF($AASServiceStatus -ne "Paused")
	{
		Write-Output "Server Original Running. Do Nothing!"
	}