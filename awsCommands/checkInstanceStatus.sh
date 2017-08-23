command_string='aws ec2 describe-instances'
$command_string | grep Name | head -n 2 | tail -n 1 | cut -d ':' -f 2 | cut -d '"' -f 2
