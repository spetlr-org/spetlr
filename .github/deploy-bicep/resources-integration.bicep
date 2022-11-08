param databricksName string
param location string
param resourceGroupName string
param resourceTags object
param dataLakeName string
param dataLakeContainers array
param ehNamespace string
param eventHubConfig array
param databaseServerName string
param allowUserIp string
param deliveryDatabase string
param sqlServerAdminUser string
@secure()
param sqlServerAdminPassword string
param sqlAdminSpnName string
param sqlAdminObjectId string

//#############################################################################################
//# Provision Databricks Workspace
//#############################################################################################

resource rsdatabricks 'Microsoft.Databricks/workspaces@2022-04-01-preview' = {
  name: databricksName
  location: location
  properties: {
    managedResourceGroupId: subscriptionResourceId('Microsoft.Resources/resourceGroups', '${resourceGroupName}Cluster')
  }
  tags: resourceTags
}

//#############################################################################################
//# Provision Storage Account (data lake)
//#############################################################################################

resource staccount 'Microsoft.Storage/storageAccounts@2021-09-01' = {
  name: dataLakeName
  location: location
  tags: resourceTags
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

    resource containersVar 'containers@2021-02-01' = [for container in dataLakeContainers: {
      name: '${container.name}'
      properties: {
        publicAccess: 'None'
      }
    }]
  }
}

//#############################################################################################
//# Provision Eventhub namespace and eventhubs
//#############################################################################################

var captureFormat = '{Namespace}/{EventHub}/y={Year}/m={Month}/d={Day}/{Year}_{Month}_{Day}_{Hour}_{Minute}_{Second}_{PartitionId}'

resource eventhubs 'Microsoft.EventHub/namespaces@2021-11-01' = {
  name: ehNamespace
  location: location
  tags: resourceTags
  sku: {
    name: 'Standard'
  }

  resource ehs 'eventhubs@2021-11-01' = [for eh in eventHubConfig: {
    name: eh.name
    properties: {
      messageRetentionInDays: 7
      partitionCount: 4
      captureDescription: {
        enabled: true
        intervalInSeconds: 60
        sizeLimitInBytes: 314572800
        destination: {
          name: 'EventHubArchive.AzureBlockBlob'
          properties: {
            dataLakeAccountName: dataLakeName
            blobContainer: eh.captureLocation
            archiveNameFormat: captureFormat
            storageAccountResourceId: staccount.id
          }
        }
        skipEmptyArchives: true
        encoding: 'Avro'
      }
    }
  }]
}

//#############################################################################################
//# Provision SQL Server
//#############################################################################################

resource sqlserver 'Microsoft.Sql/servers@2022-02-01-preview' = {
  name: databaseServerName
  location: location
  tags: resourceTags
  identity: {
    type: 'SystemAssigned'
  }
  properties: {
    administratorLogin: sqlServerAdminUser
    administratorLoginPassword: sqlServerAdminPassword

    administrators: {
      administratorType: 'ActiveDirectory'
      login: sqlAdminSpnName
      principalType: 'Application'
      sid: sqlAdminObjectId
      tenantId: tenant().tenantId
    }
  }

  resource firewallazure 'firewallRules@2022-02-01-preview' = {
    name: 'AllowAllWindowsAzureIps'
    properties: {
      startIpAddress: '0.0.0.0'
      endIpAddress: '0.0.0.0'
    }
  }

  resource firewalluser 'firewallRules@2022-02-01-preview' = {
    name: 'Allow ${allowUserIp}'
    properties: {
      startIpAddress: allowUserIp
      endIpAddress: allowUserIp
    }
  }
}


//#############################################################################################
//# Provision SQL database
//#############################################################################################

resource sqlDb 'Microsoft.Sql/servers/databases@2022-02-01-preview' = {
  name: deliveryDatabase
  parent: sqlserver
  location: location
  tags: resourceTags
  sku: {
    name: 'GP_S_Gen5'
    capacity: 1
    tier: 'GeneralPurpose'
    family: 'Gen5'
  }
  properties: {
    minCapacity: any('0.5')
    zoneRedundant: false
    requestedBackupStorageRedundancy: 'Local'
  }
}
