param (  
    [Parameter(Mandatory=$true)][string] $DWConnectionString,
    [Parameter(Mandatory=$true)][string] $DWResourceGroupName,
    [Parameter(Mandatory=$true)][string] $DWServiceObjectiveName
)

Import-Module SqlServer
Import-Module Az.Sql

## Get SQL Server and SQL Database from Connection string
$SqlQuery = "SELECT @@SERVERNAME AS 'DWServerName', DB_NAME() AS 'DWDatabaseName'"

$SqlOutput = Invoke-Sqlcmd -ConnectionString "$DWConnectionString" -Query $SqlQuery

## Set variables
$DWServerName = $SqlOutput.DWServerName
$DWDatabaseName = $SqlOutput.DWDatabaseName

## Scale Azure SQL Database Service Object

Write-Output "Start Scaling Azure SQL Database to DWServiceObjectiveName: $DWServiceObjectiveName" 

Set-AzSqlDatabase -ResourceGroupName $DWResourceGroupName -DatabaseName $DWDatabaseName -ServerName $DWServerName -RequestedServiceObjectiveName $DWServiceObjectiveName

Write-Output "End Scaling Azure SQL Database to DWServiceObjectiveName: $DWServiceObjectiveName"