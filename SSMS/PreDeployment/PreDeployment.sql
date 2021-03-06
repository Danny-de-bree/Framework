/*
 Pre-Deployment Script Template							
--------------------------------------------------------------------------------------
 This file contains SQL statements that will be executed before the build script.	
 Use SQLCMD syntax to include a file in the pre-deployment script.			
 Example:      :r .\myfile.sql								
 Use SQLCMD syntax to reference a variable in the pre-deployment script.		
 Example:      :setvar TableName MyTable							
               SELECT * FROM [$(TableName)]					
--------------------------------------------------------------------------------------
*/
GO

/* Add Extended Properties */
:r .\ExtendedProperties\DatabaseExtendedProperties.sql
GO

/* Create Data Warehouse schemas */
:r .\Schemas\DataWarehouseSchemas.sql
GO

/* Disable Database triggers */
:r .\Triggers\DisableTableTrigger.sql
:r .\Triggers\DisableViewTrigger.sql
GO
