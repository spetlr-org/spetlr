targetScope = 'resourceGroup'

param location string
param keyVaultName string
param devobjectid string
param spnobjectid string
param tags object
param dbname string

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
    tenantId: subscription().tenantId
    accessPolicies: [
      {
        // Developers
        objectId: devobjectid
        permissions: {
          secrets: [
            'list'
            'get'
            'set'
          ] }
        tenantId: subscription().tenantId
      } 
      {
        objectId: spnobjectid
        permissions: {
          secrets: [
            'list'
            'get'
            'set'
          ] }
        tenantId: subscription().tenantId
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
    locations: [{
      failoverPriority: 0
      isZoneRedundant: false
      locationName: location
    }]
  }
}
