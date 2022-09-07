targetScope = 'subscription'

param name string
param location string
param tags object


resource rgIntegration 'Microsoft.Resources/resourceGroups@2021-01-01' = {
  location: location
  name:name
  tags: tags
  
}
