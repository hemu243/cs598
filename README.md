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
