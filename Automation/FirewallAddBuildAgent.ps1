param (
    [Parameter(Mandatory = $true)][string]$ResourceGroupName,
    [Parameter(Mandatory = $true)][string]$ResourceName,
    [Parameter(Mandatory = $true)][string]$FirewallRuleName
)

try {
    #Setting additional parameters
    $PubIPSource = "ipinfo.io/ip"

    # Get info about current setup
    $AServiceServer = Get-AzAnalysisServicesServer -Name $ResourceName -ResourceGroupName $ResourceGroupName
    $FirewallRules = ($AServiceServer).FirewallConfig.FirewallRules
    $FirewallRuleNameList = $FirewallRules.FirewallRuleName
    $powerBi = ($AServiceServer).FirewallConfig.EnablePowerBIService

    #Getting previous IP from firewall rule, and new public IP
    $PreviousRuleIndex = [Array]::IndexOf($FirewallRuleNameList, $FirewallRuleName)
    $currentIP = (Invoke-WebRequest -uri $PubIPSource -UseBasicParsing).content.TrimEnd()
    $previousIP = ($FirewallRules).RangeStart[$PreviousRuleIndex]

    #Updating rules if request is coming from new IP address.
    if (!($currentIP -eq $previousIP)) {
        Write-Output "Updating Azure Analysis Service firewall config"
        $ruleNumberIndex = 1
        $Rules = @() -as [System.Collections.Generic.List[Microsoft.Azure.Commands.AnalysisServices.Models.PsAzureAnalysisServicesFirewallRule]]

        #Storing Analysis Service firewall rules
        $FirewallRules | ForEach-Object {
            $ruleNumberVar = "rule" + "$ruleNumberIndex"
            #Exception of storage of firewall rule is made for the rule to be updated
            if (!($_.FirewallRuleName -match "$FirewallRuleName")) {

                $start = $_.RangeStart
                $end = $_.RangeEnd
                $tempRule = New-AzAnalysisServicesFirewallRule `
                    -FirewallRuleName $_.FirewallRuleName `
                    -RangeStart $start `
                    -RangeEnd $end

                Set-Variable -Name "$ruleNumberVar" -Value $tempRule
                $Rules.Add((Get-Variable $ruleNumberVar -ValueOnly))
                $ruleNumberIndex = $ruleNumberIndex + 1
            }
        }
        
        Write-Output $FirewallRules         #Write all FireWall Rules to Host

        #Add rule for new IP
        $updatedRule = New-AzAnalysisServicesFirewallRule `
            -FirewallRuleName "$FirewallRuleName" `
            -RangeStart $currentIP `
            -RangeEnd $currentIP
        
        $ruleNumberVar = "rule" + "$ruleNumberIndex"
        Set-Variable -Name "$ruleNumberVar" -Value $updatedRule
        $Rules.Add((Get-Variable $ruleNumberVar -ValueOnly))

        #Creating Firewall config object
        if ($powerBi) {
                $conf = New-AzAnalysisServicesFirewallConfig -EnablePowerBiService -FirewallRule $Rules 
            }
        else {       
                $conf = New-AzAnalysisServicesFirewallConfig -FirewallRule $Rules 
            }
        
        #Setting firewall config
        if ([String]::IsNullOrEmpty($AServiceServer.BackupBlobContainerUri)) {
            $AServiceServer | Set-AzAnalysisServicesServer `
                -FirewallConfig $conf `
                -DisableBackup `
                -Sku $AServiceServer.Sku.Name.TrimEnd()
        }
        else {
            $AServiceServer | Set-AzAnalysisServicesServer `
                -FirewallConfig $conf `
                -BackupBlobContainerUri $AServiceServer.BackupBlobContainerUri `
                -Sku $AServiceServer.Sku.Name.TrimEnd()
        
        }
        Write-Output "Updated firewall rule to include current IP: $currentIP"
        Write-Output "Enable Power Bi Service was set to: $powerBi" 
    }
} catch {
    $errMsg = $_.exception.message
    Write-Host "##vso[task.logissue type=error;]Error during add firewall rule ($errMsg)"
    throw
}