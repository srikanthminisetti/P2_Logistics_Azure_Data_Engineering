/* =====================================================================================
   01_database_setup.sql  -  Logistics & Supply Chain | Azure SQL Database (T-SQL)
   -------------------------------------------------------------------------------------
   Purpose : create the database (step 1, run on the SERVER) and the schemas / security
             objects (steps 2-3, run INSIDE the database).
   Status  : deployment-ready template. NOT executed against an Azure SQL instance in the
             delivered project (no Azure subscription was available). Placeholders are in <angle brackets>.
   Run with: Azure Data Studio, SSMS or sqlcmd. "GO" is the client batch separator.
   ===================================================================================== */

/* ---- STEP 1 : run while connected to the [master] database of your logical server -----
-- In Azure SQL Database, CREATE DATABASE must be the only statement in its batch.
CREATE DATABASE logisticsdb
    ( EDITION = 'Standard', SERVICE_OBJECTIVE = 'S1', MAXSIZE = 10 GB );
GO
---------------------------------------------------------------------------------------- */

/* ---- STEP 2 : connect to [logisticsdb] and create schemas ---------------------------- */
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = N'logistics')
    EXEC (N'CREATE SCHEMA logistics AUTHORIZATION dbo');   -- dimensions, facts, gold tables, procs
GO
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = N'staging')
    EXEC (N'CREATE SCHEMA staging AUTHORIZATION dbo');     -- landing area for BULK INSERT / ADF copies
GO

/* ---- STEP 3 : least-privilege access ---------------------------------------------------
   Azure Data Factory authenticates with its managed identity (no passwords in the pipeline).
   Replace <adf-name> with the name of your data factory, <aad-group> with your analyst group.

CREATE USER [<adf-name>] FROM EXTERNAL PROVIDER;
ALTER ROLE db_datareader ADD MEMBER [<adf-name>];
ALTER ROLE db_datawriter ADD MEMBER [<adf-name>];
GRANT EXECUTE ON SCHEMA::logistics TO [<adf-name>];     -- allows the logging / reload stored procedures

CREATE USER [<aad-group>] FROM EXTERNAL PROVIDER;
ALTER ROLE db_datareader ADD MEMBER [<aad-group>];      -- analysts: read-only
---------------------------------------------------------------------------------------- */

/* Quick check */
SELECT s.name AS schema_name
FROM   sys.schemas AS s
WHERE  s.name IN (N'logistics', N'staging');
GO
