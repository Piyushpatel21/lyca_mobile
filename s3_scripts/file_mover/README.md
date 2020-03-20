# File mover

As requested by Lyca's team, this will automatically move files from the landing zone to somewhere more appropriate, including subdirectories for date.

The stack deploys and connects three main components: a Lambda function, triggered by an SNS Topic, with configuration in a DynamoDB table.

Initially, a historical load will be done by sending synthetic notifications down the SNS topic for all files currently in the landing zone, and after that hour loads will be done by the same mechanism, followed by notifying directly from the bucket and acting as a traditional cloud event-driven mechanism.