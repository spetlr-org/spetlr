param location string
param keyVaultName string
param spnobjectid string
param tags object
param dbname string
param permanentResourceGroup string
param metastoreDatabricksName string
param metastoreStorageAccountName string
param metastoreContainerName string
param metastoreAccessConnectorName string

//#############################################################################################
//# Provision Keyvault
//#############################################################################################

resource kw 'Microsoft.KeyVault/vaults@2022-07-01' = {
  name: keyVaultName
  location: location
  tags: tags
  properties: {
    sku: {
      family: 'A'
      name: 'standard'
    }
    enabledForTemplateDeployment: true
    tenantId: tenant().tenantId
    accessPolicies: [
      {
        objectId: spnobjectid
        permissions: {
          secrets: [
            'list'
            'get'
            'set'
          ] }
        tenantId: tenant().tenantId
      }
    ]
  }
}

//#############################################################################################
//# Provision Cosmos database
//#############################################################################################

resource csdb 'Microsoft.DocumentDB/databaseAccounts@2022-05-15' = {
  name: dbname
  tags: tags
  location: location
  properties: {
    databaseAccountOfferType: 'Standard'
    enableFreeTier: true
    locations: [ {
        failoverPriority: 0
        isZoneRedundant: false
        locationName: location
      } ]
  }
}

//#############################################################################################
//# Provision Storage Account (data lake) and Container (for Metastore)
//#############################################################################################

resource metastoreStorageAccount 'Microsoft.Storage/storageAccounts@2023-01-01' = {
  name: metastoreStorageAccountName
  location: location
  tags: tags
  sku: {
    name: 'Standard_LRS'
  }
  kind: 'StorageV2'
  properties: {
    isHnsEnabled: true
    allowBlobPublicAccess: false
    encryption: {
      services: {
        blob: {
          enabled: true
        }
        file: {
          enabled: true
        }
      }
      keySource: 'Microsoft.Storage'
    }
    supportsHttpsTrafficOnly: true
  }

  resource blobservice 'blobServices@2021-09-01' = {
    name: 'default'

    resource metastoreContainer 'containers@2023-01-01' = {
      name: metastoreContainerName
      properties: {
        publicAccess: 'None'
      }
    }
  }
}

//#############################################################################################
//# Provision Databricks Access Connector (for Metastore)
//#############################################################################################

resource metastoreAccessConnector 'Microsoft.Databricks/accessConnectors@2023-05-01' = {
  name: metastoreAccessConnectorName
  location: location
  tags: tags
  identity: {
    type: 'SystemAssigned'
  }
}

//#############################################################################################
//# Provision Databricks Workspace (for Metastore)
//#############################################################################################

resource metastoreDatabricksWorkspace 'Microsoft.Databricks/workspaces@2022-04-01-preview' = {
  name: metastoreDatabricksName
  location: location
  properties: {
    managedResourceGroupId: subscriptionResourceId('Microsoft.Resources/resourceGroups', '${permanentResourceGroup}Cluster')
  }
  tags: tags
}
