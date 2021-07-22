param (
    [Parameter(Mandatory = $true)][string]$ResourceGroupName,
    [Parameter(Mandatory = $true)][string]$ResourceName,
    [Parameter(Mandatory = $true)][string]$FirewallRuleName
)

try {
    Write-Output "Updating Azure Analysis Service firewall config"

    # Get current config
    $AServiceServer = Get-AzAnalysisServicesServer -Name $ResourceName -ResourceGroupName $ResourceGroupName
    $currentConfig = ($AServiceServer).FirewallConfig
    $newFirewallRules = $currentConfig.FirewallRules

    # Remove rile if exists
    $newFirewallRules.RemoveAll({ $args[0].FirewallRuleName -eq $FirewallRuleName })

    # Write new rules to ourput
    Write-Output $newFirewallRules         #Write all FireWall Rules to Host
    
    
    if ($currentConfig.EnablePowerBIService) {
        $firewallConfig = New-AzAnalysisServicesFirewallConfig -FirewallRule $newFirewallRules -EnablePowerBIService
    } else {
        $firewallConfig = New-AzAnalysisServicesFirewallConfig -FirewallRule $newFirewallRules
    }

    #Setting firewall config
    if ([String]::IsNullOrEmpty($AServiceServer.BackupBlobContainerUri)) {
        $AServiceServer | Set-AzAnalysisServicesServer `
            -FirewallConfig $firewallConfig `
            -DisableBackup `
            -Sku $AServiceServer.Sku.Name.TrimEnd()
    }
    else {
        $AServiceServer | Set-AzAnalysisServicesServer `
            -FirewallConfig $firewallConfig `
            -BackupBlobContainerUri $AServiceServer.BackupBlobContainerUri `
            -Sku $AServiceServer.Sku.Name.TrimEnd()
    
    }
    Write-Output "Updated firewall rule to exclude current IP: $currentIP"
} catch {
    $errMsg = $_.exception.message
    Write-Host "##vso[task.logissue type=error;]Error during removing firewall rule ($errMsg)"
    throw
}