using '../main.bicep'

param env = 'dev'
param location = 'eastus'
param storageAccountName = 'stnlretaildev'
param keyVaultName = 'kv-nl-retail-dev'
param dataFactoryName = 'adf-nl-retail-dev'
param databricksName = 'dbw-nl-retail-dev'
param synapseName = 'syn-nl-retail-dev'
param sqlAdminLogin = 'nladmin'
param sqlPoolName = 'nlretail'
param sqlPoolSku = 'DW100c'
// sqlAdminPassword is supplied at deploy time from SYNAPSE_SQL_PASSWORD.
