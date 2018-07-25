aws emr create-cluster --auto-scaling-role EMR_AutoScaling_DefaultRole \
--applications Name=Hadoop Name=Spark Name=Tez Name=Ganglia Name=Hive \
--ebs-root-volume-size 10 \
--ec2-attributes '{"KeyName":"harleen-keypair-origin","InstanceProfile":"EMR_EC2_DefaultRole","ServiceAccessSecurityGroup":"sg-02086daf2b0f2cb21","SubnetId":"subnet-03c206444a3159a43","EmrManagedSlaveSecurityGroup":"sg-082e8dd6e779019d2","EmrManagedMasterSecurityGroup":"sg-08d6d02255395a972","AdditionalMasterSecurityGroups":["sg-077384ea04e626cc6"]}' \
--service-role EMR_DefaultRole \
--enable-debugging \
--release-label emr-5.13.0 \
--log-uri 's3n://aws-logs/aws-logs-319593511656-ap-southeast-2/elasticmapreduce/' \
--name 'My cluster' \
--instance-groups '[{"InstanceCount":2,"InstanceGroupType":"CORE","InstanceType":"m4.2xlarge","Name":"Core - 2"},{"InstanceCount":1,"InstanceGroupType":"MASTER","InstanceType":"m4.xlarge","Name":"Master - 1"}]' \
--configurations '[{"Classification":"hive-site","Properties":{"javax.jdo.option.ConnectionUserName":"root","javax.jdo.option.ConnectionDriverName":"org.mariadb.jdbc.Driver","javax.jdo.option.ConnectionPassword":"rooterrooterrooter","javax.jdo.option.ConnectionURL":"jdbc:mysql://devmetastore.clmis5yhnool.ap-southeast-2.rds.amazonaws.com:3306/hive?createDatabaseIfNotExist=true"},"Configurations":[]}, {"Classification": "capacity-scheduler","Properties": {"yarn.scheduler.capacity.resource-calculator":"org.apache.hadoop.yarn.util.resource.DominantResourceCalculator"}}, {"Classification": "yarn-site","Properties": { "yarn.resourcemanager.scheduler.class": "org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler"} }]' \
--scale-down-behavior TERMINATE_AT_TASK_COMPLETION \
--region ap-southeast-2 \
--tags 'Name=6-6-18cluster01'