# Example of call
# .\pbiBatchRename.ps1 -SourceFolder ".\source\" -DestinationFolder ".\dest\" -Rename ".\Rename.txt" -server "asazure://northeurope.asazure.windows.net/devpelagiadwhssas"

Param (
    [string]$SourceFolder,
    [string]$DestinationFolder,
    [string]$Rename,
    [string]$ASConnectionString
)

function FileToFolder()
{
	Param ([string]$file, [string]$folder)
	$zipfile = ([io.path]::ChangeExtension($file, ".zip"))
	Copy-Item $file -Destination $zipfile
	Expand-Archive -Path $zipfile -DestinationPath $folder -Force
	Remove-Item $zipfile

}

function FileFromFolder()
{
	Param ([string]$file, [string]$folder)
	$securityfile=$folder+"\SecurityBindings"
	if([io.file]::Exists($securityfile))
	{
		Remove-Item $securityfile
	}
	$zipfile = ([io.path]::ChangeExtension($file, ".zip"))
	if([io.file]::Exists($zipfile))
	{
		Remove-Item $zipfile
	}
	$zipfolder=$folder+"\*"
	set-alias sz "$env:ProgramFiles\7-Zip\7z.exe" 
	sz a $zipfile $zipfolder
	#Compress-Archive -Path $zipfolder -DestinationPath $zipfile -Force
	Copy-Item $zipfile -Destination $file
	Remove-Item $zipfile
}

function ChangeConnection([string]$folder, [string]$ASConnectionString)
{
	$file = $folder+"\Connections"
	$data = Get-Content -Path $file | Out-String | ConvertFrom-Json
	$params = $data.Connections[0].ConnectionString.Split(";") | ConvertFrom-String -Delimiter "=" -PropertyNames Parm, Value
    $parm = $params | Where-Object {$_.Parm -eq 'Data Source'} 
    $parm.Value = $ASConnectionString
    $connection = ""
    foreach($parm in $params)
    {
       $connection += $parm.Parm + "=" + $parm.Value + ";"
    }
    $data.Connections[0].ConnectionString = $connection
    $content = $data | ConvertTo-Json 
    Set-Content -Path $file -Value $content 

}

function RenameObjects([string]$folder, [string]$Rename)
{
    $layoutfile = $folder+"\Report\Layout"
	$data = Get-Content -Path $layoutfile -Encoding Unicode
    $items = Get-Content -Path $Rename | ConvertFrom-String -Delimiter ">" -PropertyNames from, to
    foreach($item in $items)
    {
        $data = ForEach-Object {$data -Replace $item.from, $item.to}
    }

    $encoder = new-object System.Text.UnicodeEncoding
	$bytes = $encoder.Getbytes($data)
	Set-Content -Path $layoutfile -Value $bytes -Encoding Byte -NoNewLine
}


if($SourceFolder -ne "" -and $DestinationFolder -ne "" -and $Rename -ne "")
{

    $SourceFolder += "*.pbix"
    [string[]]$list = (Get-ChildItem $SourceFolder).FullName
    ForEach($sourcefile in $list) 
    {
	
        $folder = Split-Path -Path $sourcefile
	    $repname = Split-Path -Leaf $sourcefile
	    $subfolder = $repname -replace ".pbix", ""
	    $folder = $folder + "\" + $subfolder + "\"
        $destfile = $DestinationFolder + $repname
        FileToFolder -file $sourcefile -folder $folder
        ChangeConnection -folder $folder -ASConnectionString $ASConnectionString
        RenameObjects -folder $folder -Rename $Rename
        FileFromFolder -file $destfile -folder $folder
    }
}
else
{
      ""
    , "parameters:"
    , "-------------------------------------------------------------------"
    , " -SourceFolder <folder with Power BI reports to convert>"
    , " -DestinationFolder <folder with converted Power BI reports>"
    , " -Rename <file with list of Renamed objects>" 
    , "[-ASConnectionString <path to analysis service> ]"
    , ""
    , "example of the list of Renamed objects:" 
    , "-------------------------------------------------------------------"
    , "DimItems>DimItem"
    , "FactSalesOrders>FactSalesOrder"
    , "FactSalesInvoices>FactSalesInvoice"
    , ""
    , "example of calling the utility"
    "-------------------------------------------------------------------"
    , ".\pbiBatchRename.ps1 -SourceFolder .\source\ -DestinationFolder .\dest\ -Rename .\Rename.txt -ASConnectionString asazure://northeurope.asazure.windows.net/devpelagiadwhssas"
    , "" | Write-Output

}