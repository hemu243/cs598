# CS 598
## Cleaning Data
* Create EC2 instance
* Attach ECB volume mentioned snap-e1608d88 https://aws.amazon.com/datasets/transportation-databases/
Note: If you can't find the volume while then check that you are in same AWS region
* Once EC2 instance is being launch, SSH to the instance
`ssh -i "id_rsa.pub.pem" ec2-user@ec2-54-82-123-23.compute-1.amazonaws.com`
* Check attached mount vols using command following command
`lsblk`
* sudo mkdir data
`sudo mkdir data`
* Mount to a folder `sudo mount /dev/xvdb data`
* Copy data from EC2 instance folder to s3
```aws s3 cp //data/aviation/airline_ontime/ s3://src-zips/ --recursive```
* Run lambda functions to extracts data from zips files
* Store unzipped and modified files at new S3 bucket

## Install Hadoop locally (Ubuntu)
### Install openjdk 8
`sudo apt install openjdk-8-jdk -y`
### Set Up a Non-Root User for Hadoop Environment and install
Reference - https://phoenixnap.com/kb/install-hadoop-ubuntu
Note
