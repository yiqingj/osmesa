include config-aws.mk # Vars related to AWS credentials and services used
include config-emr.mk # Vars related to type and size of EMR cluster
include config-run.mk # Vars related to ingest step and spark parameters

ZOOM ?= 15

INGEST_SRC_DIR := ../../../src/ingest
INGEST_JAR := osmesa-ingest.jar
INGEST_ASSEMBLY := ${INGEST_SRC_DIR}/target/scala-2.11/${INGEST_JAR}

ANALYTICS_SRC_DIR := ../../../src/analytics
ANALYTICS_JAR := osmesa-analytics.jar
ANALYTICS_ASSEMBLY := ${ANALYTICS_SRC_DIR}/target/scala-2.11/${ANALYTICS_JAR}

ifeq ($(USE_SPOT),true)
MASTER_BID_PRICE:=BidPrice=${MASTER_PRICE},
WORKER_BID_PRICE:=BidPrice=${WORKER_PRICE},
endif

ifdef COLOR
COLOR_TAG=--tags Color=${COLOR}
endif

ifndef CLUSTER_ID
CLUSTER_ID=$(shell if [ -e "cluster-id.txt" ]; then cat cluster-id.txt; fi)
endif

rwildcard=$(foreach d,$(wildcard $1*),$(call rwildcard,$d/,$2) $(filter $(subst *,%,$2),$d))

${INGEST_ASSEMBLY}: $(call rwildcard, ${INGEST_SRC_DIR}/src, *.scala) ${INGEST_SRC_DIR}/build.sbt
	cd ${INGEST_SRC_DIR}/.. && ./sbt "project ingest" assembly
	@touch -m ${INGEST_ASSEMBLY}

${ANALYTICS_ASSEMBLY}: $(call rwildcard, ${ANALYTICS_SRC_DIR}/src, *.scala) ${ANALYTICS_SRC_DIR}/build.sbt
	cd ${ANALYTICS_SRC_DIR}/.. && ./sbt "project analytics" assembly
	@touch -m ${ANALYTICS_ASSEMBLY}

create-cluster:
	aws emr create-cluster --name "${NAME}" ${COLOR_TAG} \
--release-label emr-5.19.0 \
--output text \
--use-default-roles \
--configurations "file://$(CURDIR)/scripts/configurations.json" \
--log-uri ${S3_URI}/logs \
--ec2-attributes KeyName=${EC2_KEY},SubnetId=${SUBNET_ID},EmrManagedMasterSecurityGroup=${MASTER_SECURITY_GROUP},EmrManagedSlaveSecurityGroup=${WORKER_SECURITY_GROUP},ServiceAccessSecurityGroup=${SERVICE_ACCESS_SG},AdditionalMasterSecurityGroups=${SANDBOX_SG},AdditionalSlaveSecurityGroups=${SANDBOX_SG} \
--applications Name=Ganglia Name=Hadoop Name=Hue Name=Spark Name=Zeppelin \
--instance-groups \
'Name=Master,${MASTER_BID_PRICE}InstanceCount=1,InstanceGroupType=MASTER,InstanceType=${MASTER_INSTANCE}' \
'Name=Workers,${WORKER_BID_PRICE}InstanceCount=${WORKER_COUNT},InstanceGroupType=CORE,InstanceType=${WORKER_INSTANCE}' \
| tee cluster-id.txt

upload-ingest: ${INGEST_ASSEMBLY}
	@aws s3 cp ${INGEST_ASSEMBLY} ${S3_URI}/

upload-analytics: ${ANALYTICS_ASSEMBLY}
	@aws s3 cp ${ANALYTICS_ASSEMBLY} ${S3_URI}/

planet-ingest:
	aws emr add-steps --output text --cluster-id ${CLUSTER_ID} \
--steps Type=CUSTOM_JAR,Name="Planet ingest - zoom ${ZOOM}",Jar=command-runner.jar,Args=[\
spark-submit,--master,yarn,--deploy-mode,cluster,\
--class,osmesa.Ingest,\
--driver-memory,${DRIVER_MEMORY},\
--driver-cores,${DRIVER_CORES},\
--executor-memory,${EXECUTOR_MEMORY},\
--executor-cores,${EXECUTOR_CORES},\
--conf,spark.driver.maxResultSize=3g,\
--conf,spark.dynamicAllocation.enabled=true,\
--conf,spark.yarn.executor.memoryOverhead=${YARN_OVERHEAD},\
--conf,spark.yarn.driver.memoryOverhead=${YARN_OVERHEAD},\
--conf,spark.shuffle.io.numConnectionsPerPeer=4,\
--conf,spark.shuffle.io.connectionTimeout=18000s,\
--conf,spark.shuffle.service.enabled=true,\
${S3_URI}/${INGEST_JAR},\
--orc=${PLANET_ORC},\
--bucket=${S3_BUCKET},\
--key=vectortiles,\
--layer=history,\
--cache=${ORC_CACHE_LOCATION},\
--zoom=${ZOOM}\
] | cut -f2 | tee last-step-id.txt

generate-changesets:
	aws emr add-steps --output text --cluster-id ${CLUSTER_ID} \
--steps Type=CUSTOM_JAR,Name="Generate Changeset Stats",Jar=command-runner.jar,Args=[\
spark-submit,--master,yarn,\
--class,osmesa.analytics.oneoffs.ChangesetStats,\
--driver-memory,${DRIVER_MEMORY},\
--driver-cores,${DRIVER_CORES},\
--executor-memory,${EXECUTOR_MEMORY},\
--executor-cores,${EXECUTOR_CORES},\
--conf,spark.driver.maxResultSize=3g,\
--conf,spark.dynamicAllocation.enabled=true,\
--conf,spark.executor.memoryOverhead=${YARN_OVERHEAD},\
--conf,spark.driver.memoryOverhead=${YARN_OVERHEAD},\
--conf,spark.shuffle.io.numConnectionsPerPeer=4,\
--conf,spark.shuffle.io.connectionTimeout=18000s,\
--conf,spark.shuffle.service.enabled=true,\
--conf,spark.sql.shuffle.partitions=1024,\
${S3_URI}/${ANALYTICS_JAR},\
--history=${OSM_HISTORY},\
--changesets=${CHANGESET_URI},\
--output=${OUTPUT_LOCATION}\
] | cut -f2 | tee last-step-id.txt

backfill-coastline:
	aws emr add-steps --output text --cluster-id ${CLUSTER_ID} \
--steps Type=CUSTOM_JAR,Name="Coastline backfill generation",Jar=command-runner.jar,Args=[\
spark-submit,--master,yarn,\
--class,osmesa.analytics.oneoffs.CoastlineBackfillJob,\
--driver-memory,${DRIVER_MEMORY},\
--driver-cores,${DRIVER_CORES},\
--executor-memory,${EXECUTOR_MEMORY},\
--executor-cores,${EXECUTOR_CORES},\
--conf,spark.driver.maxResultSize=3g,\
--conf,spark.dynamicAllocation.enabled=true,\
--conf,spark.executor.memoryOverhead=${YARN_OVERHEAD},\
--conf,spark.driver.memoryOverhead=${YARN_OVERHEAD},\
--conf,spark.shuffle.io.numConnectionsPerPeer=4,\
--conf,spark.shuffle.io.connectionTimeout=18000s,\
--conf,spark.shuffle.service.enabled=true,\
--conf,spark.sql.shuffle.partitions=1024,\
${S3_URI}/${ANALYTICS_JAR},\
--history=${OSM_HISTORY},\
--changesets=${CHANGESET_URI},\
--output=${OUTPUT_LOCATION}\
] | cut -f2 | tee last-step-id.txt

ssh:
	@aws emr ssh --cluster-id $(CLUSTER_ID) --key-pair-file "${PEM_FILE}"

proxy:
	aws emr socks --cluster-id ${CLUSTER_ID} --key-pair-file "${PEM_FILE}"

terminate-cluster:
	aws emr terminate-clusters --cluster-ids ${CLUSTER_ID}
	rm -f cluster-id.txt
	rm -f last-step-id.txt

get-logs:
	@aws emr ssh --cluster-id $(CLUSTER_ID) --key-pair-file "${HOME}/${EC2_KEY}.pem" \
		--command "rm -rf /tmp/spark-logs && hdfs dfs -copyToLocal /var/log/spark/apps /tmp/spark-logs"
	@mkdir -p  logs/$(CLUSTER_ID)
	@aws emr get --cluster-id $(CLUSTER_ID) --key-pair-file "${HOME}/${EC2_KEY}.pem" --src "/tmp/spark-logs/" --dest logs/$(CLUSTER_ID)

.PHONY: planet-ingest get-logs
