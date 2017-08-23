public_address=$(aws ec2 describe-instances | grep PublicDnsName | head -n 1 | cut -d ':' -f 2 | cut -d '"' -f 2)
connect_command='ssh -i ../../crypto-intel/keys/rds-controller.pem ec2-user@'$public_address
$connect_command
