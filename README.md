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
## Failed to clean one file
```bash
On_Time_On_Time_Performance_2008_10.csv from  ready in hdfs:///new_data
  End-of-central-directory signature not found.  Either this file is not
  a zipfile, or it constitutes one disk of a multi-part archive.  In the
  latter case the central directory and zipfile comment will be found on
  the last disk(s) of this archive.
note:  new_data/aviation/airline_ontime/2008/On_Time_On_Time_Performance_2008_11.zip may be a plain executable, not an archive
unzip:  cannot find zipfile directory in one of new_data/aviation/airline_ontime/2008/On_Time_On_Time_Performance_2008_11.zip or
        new_data/aviation/airline_ontime/2008/On_Time_On_Time_Performance_2008_11.zip.zip, and cannot find new_data/aviation/airline_ontime/2008/On_Time_On_Time_Performance_2008_11.zip.ZIP, period.
  End-of-central-directory signature not found.  Either this file is not
  a zipfile, or it constitutes one disk of a multi-part archive.  In the
  latter case the central directory and zipfile comment will be found on
  the last disk(s) of this archive.
note:  new_data/aviation/airline_ontime/2008/On_Time_On_Time_Performance_2008_12.zip may be a plain executable, not an archive
unzip:  cannot find zipfile directory in one of new_data/aviation/airline_ontime/2008/On_Time_On_Time_Performance_2008_12.zip or
        new_data/aviation/airline_ontime/2008/On_Time_On_Time_Performance_2008_12.zip.zip, and cannot find new_data/aviation/airline_ontime/2008/On_Time_On_Time_Performance_2008_12.zip.ZIP, period.
```

## Install Hadoop locally (Ubuntu)
### Install openjdk 8
`sudo apt install openjdk-8-jdk -y`
### Set Up a Non-Root User for Hadoop Environment and install
Reference - https://phoenixnap.com/kb/install-hadoop-ubuntu
Update the following setting from the article to make it working
```bash

.bashrc

export HADOOP_HOME=/home/hadoop/hadoop-3.2.1
export HADOOP_INSTALL=$HADOOP_HOME
export HADOOP_MAPRED_HOME=$HADOOP_HOME
export HADOOP_COMMON_HOME=$HADOOP_HOME
export HADOOP_HDFS_HOME=$HADOOP_HOME
export YARN_HOME=$HADOOP_HOME
export HADOOP_COMMON_LIB_NATIVE_DIR=$HADOOP_HOME/lib/native
export PATH=$PATH:$HADOOP_HOME/sbin:$HADOOP_HOME/bin
export HADOOP_OPTS="-Djava.library.path=$HADOOP_HOME/lib/"
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64

hadoop-env.sh

export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64

export HADOOP_CONF_DIR=${HADOOP_HOME}/etc/hadoop
```

### AWS EMR setup
```bash
aws emr create-cluster --termination-protected --applications Name=Hadoop Name=Spark --ec2-attributes '{"KeyName":"EMR Key","InstanceProfile":"EMR_EC2_DefaultRole","SubnetId":"subnet-34596850","EmrManagedSlaveSecurityGroup":"sg-0875697d751399263","EmrManagedMasterSecurityGroup":"sg-0d93523de267eca9a"}' --release-label emr-6.0.0 --log-uri 's3n://aws-logs-584285917640-us-east-1/elasticmapreduce/' --instance-groups '[{"InstanceCount":2,"EbsConfiguration":{"EbsBlockDeviceConfigs":[{"VolumeSpecification":{"SizeInGB":32,"VolumeType":"gp2"},"VolumesPerInstance":2}]},"InstanceGroupType":"CORE","InstanceType":"m5.xlarge","Name":"Core - 2"},{"InstanceCount":1,"EbsConfiguration":{"EbsBlockDeviceConfigs":[{"VolumeSpecification":{"SizeInGB":32,"VolumeType":"gp2"},"VolumesPerInstance":2}]},"InstanceGroupType":"MASTER","InstanceType":"m5.xlarge","Name":"Master - 1"}]' --configurations '[{"Classification":"spark-env","Properties":{},"Configurations":[{"Classification":"export","Properties":{"PYSPARK_PYTHON":"/usr/bin/python3"}}]}]' --auto-scaling-role EMR_AutoScaling_DefaultRole --ebs-root-volume-size 50 --service-role EMR_DefaultRole --enable-debugging --name 'My cluster' --scale-down-behavior TERMINATE_AT_TASK_COMPLETION --region us-east-1
```
#### Cluster Configuration
```json
[{"classification":"spark-env", "properties":{}, "configurations":[{"classification":"export", "properties":{"PYSPARK_PYTHON":"/usr/bin/python3"}, "configurations":[]}]}]
```
`
file = sc.textFile('hdfs:///user/hadoop/output/part1/part-r-00000')
`
