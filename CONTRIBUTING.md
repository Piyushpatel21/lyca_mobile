# CONTRIBUTING

Contributing to this project is welcome!

# Repository Management
Please create a Pull/Merge Request, and add `WIP:` in the header while it is still in progress.
Be sure to update the description with a summary of the changes you are proposing and the reason.

When it is ready for review, remove the `WIP:` and send the link to the required people (email alerts do not always work, slack messages are a better choice).

To avoid conflict, the CloudFormation is recommended to be applied to the platforms (except dev and only if the developers have agreed), only once it is merged to master.
The reason is that if multiple people work on the same platform we will have conflicts. Also non reviewed code should not be applied to any platform (with slight exception dev).

## Deployment 
Any deployment steps (until CI/CD is added), must be documented in [DEPLOYMENTS.md](DEPLOYMENTS.md)

Cloudformation stack-name of the deployments should start with 'platform-'.

## IAM
IAM roles and policies have to start with 'mis-dl-' and the roles need the OrganizationAccountServiceBoundariesPolicy as a boundary.

## Code Review Principles Checklist
### Consistency
* Does this merge request have changes that affect the consistency of overall project definition?
* Is this merge request affecting the software design?
* Do the merge request has code that has different naming conventions(camelCase vs snake_case etc) and module names from the defined ones (see above)?

### Simplicity
* Are you able to find out what code is doing on first glance?
* Is code achieving what it is promised as per the feature specification?
* Is the developer overkilling a simple change?
* Is the developer adding changes that are not part of the JIRA issue(Feature or Bug)? (Those changes should be separated as a different merge request)

### Design Patterns
* Check whether the code falls into a standard design pattern like a singleton or an abstract factory. Ex: Get a run time project configuration based on environment (Development, Staging, Testing, Production), A database connection which should be initialized only once.
* Is the code providing a sufficient abstraction for other modules/classes?
* Is the developer reinventing the wheel? Can he/she use a proper construct(Ex: LoginView in Django) instead of implementing a feature from scratch(login mechanism)
* Does the code deal with concurrent patterns like asynchronous callbacks(ex. Python aysncio) and is it straight forward to know whether code execution path is deterministic?

### Code Level Implications
* Are database connections and file streams are correctly closed? Is there any chance for memory leaks?
* Does the code use inefficient data structures?https://wiki.python.org/moin/PythonSpeed/PerformanceTips
* Are all possible errors handled in the code? Are custom errors created and used when built-in errors are too vague to identify system failure?
* Do code logs have useful messages for runtime debugging?
* Did the developer positioned their newly written code at the end of a program or inserted based on context.
* Does alphabetical order maintained while defining variables/entities?
* Are there any edge cases where this code may fail?
* Are unnecessary libraries used(Ex: requirements.txt or package.json has unnecessary dependencies)?

### Quality of test & Documentation
* Test coverage can show a wrong picture about overall test coverage(test files are mostly covered at 100%) if test coverage includes test files. So make sure the reviewing code exclude tests from the code coverage.
* Check whether tests are sticking to their responsibilities? For Ex: 1. A unit test should only test a function and mock all external entities it uses. 2. An integration test should use real entities like test database etc.
* Check the quality of tests than the number of tests written. It is to check whether tests are testing critical logic of the implemented feature.
* Check whether the code changes need an update of project’s README and developer has done it or not?
* Check whether the documentation is consistent with rest of the project.
* Is document now less readable?

Last but not least, please follow the folder structure or request guidance if you are unsure where a component should be stored.

Thank you!

## Relevant links:
* https://google.github.io/eng-practices/review/reviewer/standard.html
* https://medium.com/dev-bits/five-golden-principles-of-a-code-review-ecf7fd977dfd




