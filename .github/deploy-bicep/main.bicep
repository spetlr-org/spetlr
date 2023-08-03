// Setting target scope
targetScope = 'subscription'

param permanentResourceGroup string
param location string
param keyVaultName string
param spnobjectid string
param resourceTags object
param cosmosName string
param resourceGroupName string
param databricksName string
param dataLakeName string
param datalakeContainers array
param ehNamespace string
param eventHubConfig array
param databaseServerName string
param deliveryDatabase string
param allowUserIp string
param sqlServerAdminUser string
@secure()
param sqlServerAdminPassword string
param sqlAdminSpnName string
param sqlAdminObjectId string
param logAnalyticsWsName string

// Metastore resources
param metastoreDatabricksName string
param metastoreStorageAccountName string
param metastoreContainerName string
param metastoreAccessConnectorName string

// Create permanent resource group
module rgModule 'rg-permanent.bicep' = {
  scope: subscription()
  name: '${permanentResourceGroup}-create'
  params: {
    name: permanentResourceGroup
    location: location
    tags: resourceTags
  }
}

// Deploy resources in the newly created permanent rg
module resources 'resources-permanent.bicep' = {
  name: '${permanentResourceGroup}-resources-deployment'
  scope: resourceGroup(permanentResourceGroup)
  dependsOn: [ rgModule ]
  params: {
    location: location
    keyVaultName: keyVaultName
    spnobjectid: spnobjectid
    tags: resourceTags
    dbname: cosmosName
    permanentResourceGroup: permanentResourceGroup
    metastoreDatabricksName: metastoreDatabricksName
    metastoreStorageAccountName: metastoreStorageAccountName
    metastoreContainerName: metastoreContainerName
    metastoreAccessConnectorName: metastoreAccessConnectorName
  }
}

// Create integration resource group
module rgModule2 'rg-integration.bicep' = {
  scope: subscription()
  name: '${resourceGroupName}-create'
  params: {
    name: resourceGroupName
    location: location
    tags: resourceTags
  }
}

// Create integration resources
module resources2 'resources-integration.bicep' = {
  name: '${resourceGroupName}-resources-deployment'
  scope: resourceGroup(resourceGroupName)
  dependsOn: [ rgModule2 ]
  params: {
    databricksName: databricksName
    location: location
    resourceGroupName: resourceGroupName
    resourceTags: resourceTags
    dataLakeName: dataLakeName
    dataLakeContainers: datalakeContainers
    ehNamespace: ehNamespace
    eventHubConfig: eventHubConfig
    databaseServerName: databaseServerName
    allowUserIp: allowUserIp
    deliveryDatabase: deliveryDatabase
    sqlServerAdminUser: sqlServerAdminUser
    sqlServerAdminPassword: sqlServerAdminPassword
    sqlAdminSpnName: sqlAdminSpnName
    sqlAdminObjectId: sqlAdminObjectId
    logAnalyticsWsName: logAnalyticsWsName
  }
}
