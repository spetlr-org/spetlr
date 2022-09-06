// Setting target scope
targetScope = 'subscription'

//param basename string

param permanentResourceGroup string //= '${basename}-permanent'
param location string //= 'westeurope'
param keyVaultName string //= 'atcGithubCiCd'
param devobjectid string
param spnobjectid string
param resourceTags object //= {
//  Owner: 'Auto Deployed', System: 'ATC-NET', Service: 'Data Platform'
//}
//param resourceName string //= 'github${basename}'

param cosmosName string //= resourceName

param resourceGroupName string //= '${basename}-integration'

param databricksName string// = resourceName

param dataLakeName string //= resourceName

param datalakeContainers array

param ehNamespace string //= resourceName
param eventHubConfig array

param databaseServerName string
param deliveryDatabase string
param allowUserIp string
param sqlServerAdminUser string
@secure()
param sqlServerAdminPassword string

// Creating permanent resource group
module rgModule 'rg-permanent.bicep' = {
  scope: subscription()
  name: '${permanentResourceGroup}-create'
  params: {
    name: permanentResourceGroup
    location: location
    tags: resourceTags
  }
}

// Deploying resources in the newly created permanent rg
module resources 'resources-permanent.bicep' = {
  name: '${permanentResourceGroup}-resources-deployment'
  scope: resourceGroup(permanentResourceGroup)
  dependsOn: [ rgModule ]
  params: {
    location: location
    keyVaultName: keyVaultName
    devobjectid: devobjectid
    spnobjectid: spnobjectid
    tags: resourceTags
    dbname: cosmosName
  }
}

// Creating integration resource group
module rgModule2 'rg-integration.bicep' = {
  scope: subscription()
  name: '${resourceGroupName}-create'
  params: {
    name: resourceGroupName
    location: location
    tags: resourceTags
  }
}

// Creating integration resources
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
  }
}
