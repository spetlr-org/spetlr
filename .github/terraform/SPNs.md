# Service Principals
This Repo uses 3 levels of service principals
## SpetlrGithubPipe
Provisioned by the manual script at `.github/deploy/setup_deploying_SPN.ps1`

The secret of this SPN is not persisted should be copied directly to the github 
secrets area.
Created in the separate script setup_deploying_SPN.ps1, this SPN is supposed to 
act as close as possible to the human that may want to run this pipeline. 
Keeping this 1-1 correspondence take some effort because SPNs can do some things 
that humans cannot. It is up to the developer to avoid these problems. The 
benefit of this design philosophy is that the entire pipeline can be tested 
locally.

# Captain

This is a service principal created as part of the integration resource deployment, 
and has a unique name. 

This SPN has no azure rights. It is used in the following functions
- The test job is run as this identity.
- It therefore needs access to keyvault for the keyvault backed secret scope
- Through the keyvault, this SPN has access to its own azure credentials
- This SPN is also configured as Sql Server Administrator
