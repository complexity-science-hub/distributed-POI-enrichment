VERSION?=$(shell ./gradlew -q  printVersionName)

DATA_FOLDER := phd/poi
DATA_LOAD_FOLDER := tmp_phd_data_load

.PHONY: repl

all: build

v:
	./gradlew currentVersion

zipDevDb:
	zip -r sample_data.zip spark-warehouse metastore_db -x "*/.*"

clean:
	./gradlew clean

compile:
	./gradlew compileScala

build:
	./gradlew build

fat-jar:
	./gradlew shadowJar

test: 
	./gradlew test

replSparkShell:
	./gradlew :benchmark-spark:shadowJar && \
	spark-shell --master 'local[2]' \
	--driver-memory 8G \
	--conf "spark.driver.extraJavaOptions=-Dconfig.file='configuration/poi_enrichment_benchmark.conf'" \
	--jars benchmark-spark/build/libs/benchmark-spark-${VERSION}-all.jar

debug:
	./gradlew :benchmark-spark:shadowJar && \
	spark-submit --class at.csh.geoheil.poi.PoiEnrichmentBenchmark \
	--master 'local[1]' \
	--driver-memory 8G \
	--jars benchmark-spark/build/libs/benchmark-spark-${VERSION}.jar
	--conf spark.driver.extraJavaOptions="-agentlib:jdwp=transport=dt_socket,server=y,address=5005,suspend=y" \

# works only on devBox. Otherwise (REF, PROD) ip-table rules prevent accessing the spark-shell server (due to security reasons)
replSparkShellYarn:
	./gradlew :benchmark-spark:shadowJar && \
	spark-shell --master yarn \
    --deploy-mode client \
    --driver-memory 4G \
    --executor-memory 6G \
    --num-executors 10 \
	--files /usr/hdp/current/spark2-client/conf/hive-site.xml,configuration/poi_enrichment_benchmark.conf \
	--conf spark.driver.extraJavaOptions="-Dconfig.file='poi_enrichment_benchmark.conf'" \
	--jars benchmark-spark/build/libs/benchmark-spark-${VERSION}-all.jar

reformat-code:
	./gradlew spotlessApply

release:
	./gradlew release

publish:
	./gradlew publish

release-major:
	./gradlew markNextVersion -Prelease.version=${releaseVersion}

local-clean-data:
	rm -rf metastore_db && \
	rm -rf spark-warehouse

local-load-data:
	./gradlew :data-generator:shadowJar && \
	spark-submit --verbose \
	--driver-memory 10G \
	--class at.csh.geoheil.poi.OsmDataLoader \
	--master 'local[4]' \
	--conf spark.sql.parquet.binaryAsString=true \
	--conf spark.driver.extraJavaOptions="-Dlog4j.configuration=file:benchmark-spark/src/main/resources/log4j.properties -Dosm-load.output-folder=\"${DATA_FOLDER}\"" \
	data-generator/build/libs/data-generator-${VERSION}-all.jar

hdfs-put-data:
	hdfs dfs -mkdir -p ${DATA_LOAD_FOLDER} && \
	hdfs dfs -put -f sample-data/* ${DATA_LOAD_FOLDER}

hdfs-load-data: hdfs-put-data
	./gradlew :data-generator:shadowJar && \
	spark-submit --verbose \
	--class at.csh.geoheil.poi.OsmDataLoader \
	--master 'local[8]' \
	--driver-memory 20G \
	--conf spark.sql.parquet.binaryAsString=true \
	--conf spark.driver.extraJavaOptions="-Dosm-load.shared-prefix=\"${DATA_LOAD_FOLDER}\" -Dosm-load.output-folder=\"${DATA_FOLDER}\"" \
	data-generator/build/libs/data-generator-${VERSION}-all.jar


# 01 ########################## POI enrichment benchmark #####################################
# DUE TO https://stackoverflow.com/questions/57572777/spark-kryo-register-generic-classes I have disabled kryo
#
run-local-benchmark-poi:
	rm -rf results/results_for_iteration.parquet && \
	./gradlew :benchmark-spark:shadowJar && \
	spark-submit --verbose \
	--class at.csh.geoheil.poi.PoiEnrichmentBenchmark \
	--master 'local[*]' \
	--driver-memory 8G \
	--conf 'spark.serializer=org.apache.spark.serializer.JavaSerializer' \
	--conf "spark.driver.extraJavaOptions=-Dlog4j.configuration=file:benchmark-spark/src/main/resources/log4j.properties -Dconfig.file='configuration/poi_enrichment_benchmark.conf'" \
	benchmark-spark/build/libs/benchmark-spark-${VERSION}-all.jar
run-yarn-benchmark-poi:
	hdfs dfs -rm -r -f results/results_for_iteration.parquet && \
	./gradlew :benchmark-spark:shadowJar && \
	spark-submit --verbose \
	--class at.csh.geoheil.poi.PoiEnrichmentBenchmark \
	--master yarn \
	--deploy-mode cluster \
	--driver-memory 10G \
	--executor-memory 55G \
	--num-executors 80 \
	--conf spark.stage.maxConsecutiveAttempts=20 \
	--conf spark.driver.maxResultSize=9G \
	--conf spark.yarn.maxAppAttempts=1 \
	--conf 'spark.serializer=org.apache.spark.serializer.JavaSerializer' \
	--files /usr/hdp/current/spark2-client/conf/hive-site.xml,configuration/poi_enrichment_benchmark.conf \
	--conf spark.driver.extraJavaOptions="-Dconfig.file='poi_enrichment_benchmark.conf'" \
	benchmark-spark/build/libs/benchmark-spark-${VERSION}-all.jar
