
/* Removing Existing Database extended properties */
IF EXISTS (SELECT 1 FROM sys.extended_properties WHERE class = 0 AND [name] = 'AzureKeyVault')					EXEC sys.sp_DropExtendedProperty 'AzureKeyVault';
IF EXISTS (SELECT 1 FROM sys.extended_properties WHERE class = 0 AND [name] = 'AzureResourceGroup')				EXEC sys.sp_DropExtendedProperty 'AzureResourceGroup';
IF EXISTS (SELECT 1 FROM sys.extended_properties WHERE class = 0 AND [name] = 'AzureSubscription')				EXEC sys.sp_DropExtendedProperty 'AzureSubscription';


IF EXISTS (SELECT 1 FROM sys.extended_properties WHERE class = 0 AND [name] = 'AASOlapRegion')					EXEC sys.sp_DropExtendedProperty 'AASOlapRegion'; 
IF EXISTS (SELECT 1 FROM sys.extended_properties WHERE class = 0 AND [name] = 'AASOlapServer')					EXEC sys.sp_DropExtendedProperty 'AASOlapServer';
IF EXISTS (SELECT 1 FROM sys.extended_properties WHERE class = 0 AND [name] = 'ADFWriteBatchSize')				EXEC sys.sp_DropExtendedProperty 'ADFWriteBatchSize';
IF EXISTS (SELECT 1 FROM sys.extended_properties WHERE class = 0 AND [name] = 'DWAzureSubscription')			EXEC sys.sp_DropExtendedProperty 'DWAzureSubscription';
IF EXISTS (SELECT 1 FROM sys.extended_properties WHERE class = 0 AND [name] = 'DWResourceGroup')				EXEC sys.sp_DropExtendedProperty 'DWResourceGroup';
IF EXISTS (SELECT 1 FROM sys.extended_properties WHERE class = 0 AND [name] = 'Environment')					EXEC sys.sp_DropExtendedProperty 'Environment';
IF EXISTS (SELECT 1 FROM sys.extended_properties WHERE class = 0 AND [name] = 'CubeLogLevel')					EXEC sys.sp_DropExtendedProperty 'CubeLogLevel'; 
IF EXISTS (SELECT 1 FROM sys.extended_properties WHERE class = 0 AND [name] = 'Customization')					EXEC sys.sp_DropExtendedProperty 'Customization'; 
IF EXISTS (SELECT 1 FROM sys.extended_properties WHERE class = 0 AND [name] = 'DefaultMaxDop')					EXEC sys.sp_DropExtendedProperty 'DefaultMaxDop';
IF EXISTS (SELECT 1 FROM sys.extended_properties WHERE class = 0 AND [name] = 'DWBusinessKeySuffix')			EXEC sys.sp_DropExtendedProperty 'DWBusinessKeySuffix';
IF EXISTS (SELECT 1 FROM sys.extended_properties WHERE class = 0 AND [name] = 'DWAppendixPrefix')				EXEC sys.sp_DropExtendedProperty 'DWAppendixPrefix';
IF EXISTS (SELECT 1 FROM sys.extended_properties WHERE class = 0 AND [name] = 'DWBridgePrefix')					EXEC sys.sp_DropExtendedProperty 'DWBridgePrefix';
IF EXISTS (SELECT 1 FROM sys.extended_properties WHERE class = 0 AND [name] = 'DWDimensionPrefix')				EXEC sys.sp_DropExtendedProperty 'DWDimensionPrefix';
IF EXISTS (SELECT 1 FROM sys.extended_properties WHERE class = 0 AND [name] = 'DWSurrogateKeySuffix')			EXEC sys.sp_DropExtendedProperty 'DWSurrogateKeySuffix';
IF EXISTS (SELECT 1 FROM sys.extended_properties WHERE class = 0 AND [name] = 'DWFactPrefix')					EXEC sys.sp_DropExtendedProperty 'DWFactPrefix';
IF EXISTS (SELECT 1 FROM sys.extended_properties WHERE class = 0 AND [name] = 'DWExtractStagingSchemaName')		EXEC sys.sp_DropExtendedProperty 'DWExtractStagingSchemaName';
IF EXISTS (SELECT 1 FROM sys.extended_properties WHERE class = 0 AND [name] = 'DWExtractDWSchemaName')			EXEC sys.sp_DropExtendedProperty 'DWExtractDWSchemaName';
IF EXISTS (SELECT 1 FROM sys.extended_properties WHERE class = 0 AND [name] = 'DWExtractHistorySchemaName')		EXEC sys.sp_DropExtendedProperty 'DWExtractHistorySchemaName';
IF EXISTS (SELECT 1 FROM sys.extended_properties WHERE class = 0 AND [name] = 'DWTransformStagingSchemaName')	EXEC sys.sp_DropExtendedProperty 'DWTransformStagingSchemaName';
IF EXISTS (SELECT 1 FROM sys.extended_properties WHERE class = 0 AND [name] = 'DWTransformDWSchemaName')		EXEC sys.sp_DropExtendedProperty 'DWTransformDWSchemaName';
IF EXISTS (SELECT 1 FROM sys.extended_properties WHERE class = 0 AND [name] = 'DWTransformDMSchemaName')		EXEC sys.sp_DropExtendedProperty 'DWTransformDMSchemaName';
IF EXISTS (SELECT 1 FROM sys.extended_properties WHERE class = 0 AND [name] = 'DWTransformSchemaName')			EXEC sys.sp_DropExtendedProperty 'DWTransformSchemaName';
IF EXISTS (SELECT 1 FROM sys.extended_properties WHERE class = 0 AND [name] = 'DWLogLevel')						EXEC sys.sp_DropExtendedProperty 'DWLogLevel'; 
IF EXISTS (SELECT 1 FROM sys.extended_properties WHERE class = 0 AND [name] = 'EnableAutoPartition')			EXEC sys.sp_DropExtendedProperty 'EnableAutoPartition'; 
IF EXISTS (SELECT 1 FROM sys.extended_properties WHERE class = 0 AND [name] = 'ETLLogLevel')					EXEC sys.sp_DropExtendedProperty 'ETLLogLevel';
IF EXISTS (SELECT 1 FROM sys.extended_properties WHERE class = 0 AND [name] = 'IndexFragmentationLimit')		EXEC sys.sp_DropExtendedProperty 'IndexFragmentationLimit';
IF EXISTS (SELECT 1 FROM sys.extended_properties WHERE class = 0 AND [name] = 'FiscalYearStartMonth')			EXEC sys.sp_DropExtendedProperty 'FiscalYearStartMonth'; 
IF EXISTS (SELECT 1 FROM sys.extended_properties WHERE class = 0 AND [name] = 'State')							EXEC sys.sp_DropExtendedProperty 'State'; 
IF EXISTS (SELECT 1 FROM sys.extended_properties WHERE class = 0 AND [name] = 'UnknownDate')					EXEC sys.sp_DropExtendedProperty 'UnknownDate'; 
IF EXISTS (SELECT 1 FROM sys.extended_properties WHERE class = 0 AND [name] = 'UnknownInt')						EXEC sys.sp_DropExtendedProperty 'UnknownInt'; 
IF EXISTS (SELECT 1 FROM sys.extended_properties WHERE class = 0 AND [name] = 'UnknownText')					EXEC sys.sp_DropExtendedProperty 'UnknownText'; 


/* Add Environment specific extended properties */

IF ('$(Environment)' = 'Dev')
BEGIN
	EXEC sys.sp_AddExtendedProperty 'AzureKeyVault'					, '<AzureKeyVaultDev>';
	EXEC sys.sp_AddExtendedProperty 'AzureResourceGroup'			, '<AzureResourceGroupDev>';
	EXEC sys.sp_AddExtendedProperty 'AzureSubscription'				, '<AzureSubscriptionDev>';

	EXEC sys.sp_AddExtendedProperty 'AASOlapRegion'					, '<AASOlapRegionDev>';
	EXEC sys.sp_AddExtendedProperty 'AASOlapServer'					, '<AASOlapServerDev>';
	EXEC sys.sp_AddExtendedProperty 'DWResourceGroup'				, '<DWResourceGroupDev>';
	EXEC sys.sp_AddExtendedProperty 'DWAzureSubscription'			, '<DWAzureSubscriptionDev>';
END;
IF ('$(Environment)' = 'Test')
BEGIN
	EXEC sys.sp_AddExtendedProperty 'AzureKeyVault'					, '<AzureKeyVaultTest>';
	EXEC sys.sp_AddExtendedProperty 'AzureResourceGroup'			, '<AzureResourceGroupTest>';
	EXEC sys.sp_AddExtendedProperty 'AzureSubscription'				, '<AzureSubscriptionTest>';

	EXEC sys.sp_AddExtendedProperty 'AASOlapRegion'					, '<AASOlapRegionTest>';
	EXEC sys.sp_AddExtendedProperty 'AASOlapServer'					, '<AASOlapServerTest>';
	EXEC sys.sp_AddExtendedProperty 'DWResourceGroup'				, '<DWResourceGroupTest>';
	EXEC sys.sp_AddExtendedProperty 'DWAzureSubscription'			, '<DWAzureSubscriptionTest>';
END;
IF ('$(Environment)' = 'PreProd')
BEGIN
	EXEC sys.sp_AddExtendedProperty 'AzureKeyVault'					, '<AzureKeyVaultPreProd>';
	EXEC sys.sp_AddExtendedProperty 'AzureResourceGroup'			, '<AzureResourceGroupPreProd>';
	EXEC sys.sp_AddExtendedProperty 'AzureSubscription'				, '<AzureSubscriptionPreProd>';

	EXEC sys.sp_AddExtendedProperty 'AASOlapRegion'					, '<AASOlapRegionPreProd>';
	EXEC sys.sp_AddExtendedProperty 'AASOlapServer'					, '<AASOlapServerPreProd>';
	EXEC sys.sp_AddExtendedProperty 'DWResourceGroup'				, '<DWResourceGroupPreProd>';
	EXEC sys.sp_AddExtendedProperty 'DWAzureSubscription'			, '<DWAzureSubscriptionPreProd>';
END;
IF ('$(Environment)' = 'Prod')
BEGIN
	EXEC sys.sp_AddExtendedProperty 'AzureKeyVault'					, '<AzureKeyVaultProd>';
	EXEC sys.sp_AddExtendedProperty 'AzureResourceGroup'			, '<AzureResourceGroupProd>';
	EXEC sys.sp_AddExtendedProperty 'AzureSubscription'				, '<AzureSubscriptionProd>';

	EXEC sys.sp_AddExtendedProperty 'AASOlapRegion'					, '<AASOlapRegionProd>';
	EXEC sys.sp_AddExtendedProperty 'AASOlapServer'					, '<AASOlapServerProd>';
	EXEC sys.sp_AddExtendedProperty 'DWResourceGroup'				, '<DWResourceGroupProd>';
	EXEC sys.sp_AddExtendedProperty 'DWAzureSubscription'			, '<DWAzureSubscriptionProd>';
END;

/* Add Database extended properties */

EXEC sys.sp_AddExtendedProperty 'ADFWriteBatchSize'					, 102400;
EXEC sys.sp_AddExtendedProperty 'CubeLogLevel'						, 'Info,Warning';
EXEC sys.sp_AddExtendedProperty 'Customization'						, 'Managed';
EXEC sys.sp_AddExtendedProperty 'DefaultMaxDop'						, 7;
EXEC sys.sp_AddExtendedProperty 'DWAppendixPrefix'					, 'Calc';
EXEC sys.sp_AddExtendedProperty 'DWBusinessKeySuffix'				, 'Key';
EXEC sys.sp_AddExtendedProperty 'DWBridgePrefix'					, 'Bridge';
EXEC sys.sp_AddExtendedProperty 'DWDimensionPrefix'					, 'Dim';
EXEC sys.sp_AddExtendedProperty 'DWSurrogateKeySuffix'				, 'ID';
EXEC sys.sp_AddExtendedProperty 'DWFactPrefix'						, 'Fact';
EXEC sys.sp_AddExtendedProperty 'DWExtractStagingSchemaName'		, 'DSA';
EXEC sys.sp_AddExtendedProperty 'DWExtractDWSchemaName'				, 'ODS';
EXEC sys.sp_AddExtendedProperty 'DWExtractHistorySchemaName'		, 'History';
EXEC sys.sp_AddExtendedProperty 'DWTransformStagingSchemaName'		, 'Stage';
EXEC sys.sp_AddExtendedProperty 'DWTransformDWSchemaName'			, 'EDW';
EXEC sys.sp_AddExtendedProperty 'DWTransformDMSchemaName'			, 'DM';
EXEC sys.sp_AddExtendedProperty 'DWTransformSchemaName'				, 'TRANSFORM';
EXEC sys.sp_AddExtendedProperty 'DWLogLevel'						, 'Info,Warning';
EXEC sys.sp_AddExtendedProperty 'EnableAutoPartition'				, 0;
EXEC sys.sp_AddExtendedProperty 'ETLLogLevel'						, 'Info,Warning';
EXEC sys.sp_AddExtendedProperty 'IndexFragmentationLimit'			, 10;
EXEC sys.sp_AddExtendedProperty 'FiscalYearStartMonth'				, 4;
EXEC sys.sp_AddExtendedProperty 'State'								, 'Setup';
EXEC sys.sp_AddExtendedProperty 'UnknownDate'						, 19000101;
EXEC sys.sp_AddExtendedProperty 'UnknownInt'						, 0;
EXEC sys.sp_AddExtendedProperty 'UnknownText'						, 'N/A';
EXEC sys.sp_AddExtendedProperty 'Environment'						, '$(Environment)';