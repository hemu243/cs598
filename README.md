# CS 598
## Cleaning Data
* Create EC2 instance
* Attach ECB volume mentioned snap-e1608d88 https://aws.amazon.com/datasets/transportation-databases/
** If you can't find the volume while then check that you are in same AWS region
* Once EC2 instance is being launch, SSH to the instance
* Check attached mount vols using command following command
`lsblk`
* sudo mkdir data
`sudo mkdir data`
* Mount to a folder `sudo mount /dev/xvdb data`
* Copy data from EC2 instance folder to s3
```aws s3 cp //data/aviation/airline_ontime/ s3://src-zips/ --recursive```
* Run lambda functions to extracts data from zips files

