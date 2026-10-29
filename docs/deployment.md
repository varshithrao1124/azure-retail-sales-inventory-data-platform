# Deployment

Nothing in this document was executed against Azure. The commands are the deploy path once the GitHub environment secrets exist. `.github/workflows/cd.yml` skips the job when they are absent.

## Secrets

| Name | Where |
| --- | --- |
| `AZURE_CLIENT_ID`, `AZURE_TENANT_ID`, `AZURE_SUBSCRIPTION_ID`, `AZURE_CLIENT_SECRET` | GitHub environment `dev` |
| `AZURE_CLIENT_ID_PROD`, `AZURE_CLIENT_SECRET_PROD` | GitHub environment `prod` |
| `DATABRICKS_HOST`, `DATABRICKS_TOKEN` | `dev` |
| `DATABRICKS_HOST_PROD`, `DATABRICKS_TOKEN_PROD` | `prod` |
| `SYNAPSE_SQL_PASSWORD`, `SYNAPSE_SQL_PASSWORD_PROD` | Synapse SQL admin password. Not stored in `*.bicepparam`. |

Create Key Vault secrets `synapse-sql-password` and, if you switch Databricks off MSI, `databricks-token`. Create a Databricks secret scope `nl-kv-<env>` backed by that vault. Local mode reads the environment instead (`secrets.mode: env` in `conf/dev.yml`).

## Bicep

`infra/main.bicep` creates:

- ADLS Gen2 account (hierarchical namespace) and containers `landing`, `bronze`, `silver`, `gold`, `audit`
- Key Vault with RBAC
- Data Factory (system-assigned identity) and Storage Blob Data Contributor on the account
- Databricks premium workspace
- Synapse workspace on the gold filesystem, plus a dedicated SQL pool (`DW100c` by default)

The dedicated pool has a real cost. Do not deploy it into a subscription you are not prepared to pause.

```bash
az group create --name rg-nl-retail-dev --location eastus
az deployment group create \
  --resource-group rg-nl-retail-dev \
  --template-file infra/main.bicep \
  --parameters infra/parameters/dev.bicepparam \
  --parameters sqlAdminPassword="$SYNAPSE_SQL_PASSWORD"
```

Storage account names are global. Change `storageAccountName` if `stnlretaildev` is taken, and use the same value in the ADF linked service URL, the bundle variable, and `NL_STORAGE_ACCOUNT`.

## Data Factory

Import in this order: linked services (`LS_KeyVault`, `LS_ADLS_Gen2`, `LS_Databricks`, `LS_Synapse_Dedicated`), datasets, pipelines, trigger. Upload `adf/control/ingest_sources.json` to `landing/control/ingest_sources.json`.

Replace the placeholder Databricks domain, workspace resource id, and cluster id. The ADLS linked service is annotated for managed identity; confirm the factory identity has Storage Blob Data Contributor (the bicep role assignment). The Synapse password reference is `AzureKeyVaultSecret`, not a literal.

`TR_Twice_Daily` is imported stopped. The alert webhook is a pipeline parameter, not a secret in git. Start the trigger after a manual orchestrator run.

## Databricks

```bash
databricks bundle validate -t dev
databricks bundle deploy -t dev
```

The job runs `src/northline/jobs/run_medallion.py` with `NL_STORAGE_MODE=adls` and `NL_CATALOG_MODE=unity`. That is the same `run_pipeline` function as `make pipeline`.

On the workspace, create catalog `northline_dev` (or `northline_prod`) and schemas `bronze`, `silver`, `gold`, `audit` if you want tables registered in Unity Catalog. The job writes Delta at the `abfss` paths either way. `catalog.mode=unity` is read by settings for the three-part name; the local runner does not execute `CREATE CATALOG`.

## Synapse

1. Run `sql/synapse/dedicated/01_watermark.sql` on the dedicated pool.
2. Run `sql/synapse/serverless/01_openrowset_gold.sql` on the serverless pool after gold exists in ADLS. The workspace managed identity needs read access to the gold container (granted in bicep).
3. Create an external data source or use the serverless views as `ext.*` and run `sql/synapse/dedicated/02_ctas_gold.sql`.
4. Import `synapse/pipeline/PL_CTAS_Refresh.json` if you want a Synapse pipeline step after the Databricks job. The procedure name in that JSON is a placeholder you replace with the CTAS script or a stored procedure you create from it.

## Power BI

See `docs/powerbi.md`. Connect to the dedicated pool or the serverless views. There is no `.pbix` in the repo.
