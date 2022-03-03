Email Action Setup and Usage Information

Please note the following:

1) Sender Email address(sender param in the EMAIL class) should be verified in AWS SES console.
   One needs to acknowledge the sender email once.
   Please follow the AWS documentation here:
   https://docs.aws.amazon.com/ses/latest/DeveloperGuide/verify-email-addresses.html

2) Recipient email addresses(recipient_list in the EMAIL class) do not need to be verified.
   Please be aware that some recipients might be an email-group which needs to unblock external emails coming from
   AWS SES.

3) Please make sure that you have the sender email identity verified in the same AWS account and region
    where you are creating your RheocerOS application.

Please refer [examples/email_action_example.py](./examples/email_action_example.py) example to see the usage.


