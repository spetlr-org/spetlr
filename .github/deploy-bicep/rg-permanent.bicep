targetScope = 'subscription'

param name string
param location string
param tags object


resource rgPermanent 'Microsoft.Resources/resourceGroups@2021-01-01' = {
  location: location
  name:name
  tags: tags
  
}
