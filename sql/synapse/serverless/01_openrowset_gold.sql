-- Serverless SQL views over Gold Delta in ADLS.
-- Replace the account name. OPENROWSET Delta does not need a dedicated pool.
-- These views were not executed against a Synapse workspace in this repo.

CREATE DATABASE northline_gold;
GO

USE northline_gold;
GO

CREATE OR ALTER VIEW gold.fact_sales AS
SELECT *
FROM OPENROWSET(
    BULK 'https://stnlretaildev.dfs.core.windows.net/gold/fact_sales/',
    FORMAT = 'DELTA'
) AS rows;
GO

CREATE OR ALTER VIEW gold.fact_returns AS
SELECT *
FROM OPENROWSET(
    BULK 'https://stnlretaildev.dfs.core.windows.net/gold/fact_returns/',
    FORMAT = 'DELTA'
) AS rows;
GO

CREATE OR ALTER VIEW gold.fact_payments AS
SELECT *
FROM OPENROWSET(
    BULK 'https://stnlretaildev.dfs.core.windows.net/gold/fact_payments/',
    FORMAT = 'DELTA'
) AS rows;
GO

CREATE OR ALTER VIEW gold.fact_inventory_movement AS
SELECT *
FROM OPENROWSET(
    BULK 'https://stnlretaildev.dfs.core.windows.net/gold/fact_inventory_movement/',
    FORMAT = 'DELTA'
) AS rows;
GO

CREATE OR ALTER VIEW gold.fact_inventory_snapshot AS
SELECT *
FROM OPENROWSET(
    BULK 'https://stnlretaildev.dfs.core.windows.net/gold/fact_inventory_snapshot/',
    FORMAT = 'DELTA'
) AS rows;
GO

CREATE OR ALTER VIEW gold.dim_customer AS
SELECT *
FROM OPENROWSET(
    BULK 'https://stnlretaildev.dfs.core.windows.net/gold/dim_customer/',
    FORMAT = 'DELTA'
) AS rows;
GO

CREATE OR ALTER VIEW gold.dim_product AS
SELECT *
FROM OPENROWSET(
    BULK 'https://stnlretaildev.dfs.core.windows.net/gold/dim_product/',
    FORMAT = 'DELTA'
) AS rows;
GO

CREATE OR ALTER VIEW gold.dim_store AS
SELECT *
FROM OPENROWSET(
    BULK 'https://stnlretaildev.dfs.core.windows.net/gold/dim_store/',
    FORMAT = 'DELTA'
) AS rows;
GO

CREATE OR ALTER VIEW gold.dim_channel AS
SELECT *
FROM OPENROWSET(
    BULK 'https://stnlretaildev.dfs.core.windows.net/gold/dim_channel/',
    FORMAT = 'DELTA'
) AS rows;
GO

CREATE OR ALTER VIEW gold.inventory_variance AS
SELECT *
FROM OPENROWSET(
    BULK 'https://stnlretaildev.dfs.core.windows.net/gold/inventory_variance/',
    FORMAT = 'DELTA'
) AS rows;
GO
