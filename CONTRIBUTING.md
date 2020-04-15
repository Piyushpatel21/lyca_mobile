# CONTRIBUTING

To contributing to this project is welcome!

Please create a Pull/Merge Request, and add `WIP:` in the header while it is still in progress.
Be sure to update the description with a summary of the changes you are proposing and the reason.

When it is ready for review, remove the `WIP:` and send the link to the required people (email alerts do not always work, slack messages are a better choice).

To avoid conflict, the CloudFormation is recommended to be applied to the platforms (except dev and only if the developers have agreed), only once it is merged to master.
The reason is that if multiple people work on the same platform we will have conflicts. Also non reviewed code should not be applied to any platform (with slight exception dev).

Any deployment steps (until CI/CD is added), must be documented in [DEPLOYMENTS.md](DEPLOYMENTS.md)

Cloudformation stack-name of the deployments should start with 'platform-'.

IAM roles and policies have to start with 'mis-dl-' and the roles need the OrganizationAccountServiceBoundariesPolicy as a boundary.

Last but not least, please follow the folder structure or request guidance if you are unsure where a component should be stored.

Thank you!



