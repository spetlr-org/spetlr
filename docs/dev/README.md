

# Github setup

## Pipeline setup

The architectiure of the integration pipeline is described in the following diagram

![ATC architecture](atc_architecture.drawio.png)

## Secrets
You need an Azure Service Principal to run the pipeline in Github.
If you have a subscription and the necessary Owner permissions, you
can run the included script `.github/deploy/setup_deploying_SPN.ps1`
to create such a service principal. The secrets need to be saved
somewhere, they can not be retrieved later.
