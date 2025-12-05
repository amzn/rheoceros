# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from enum import Enum

from intelliflow.core.signal_processing.signal_source import SchemaDefinition


class SparkType(str, Enum):
    DataType = "DataType()"
    NullType = "NullType()"
    StringType = "StringType()"
    BinaryType = "BinaryType()"
    BooleanType = "BooleanType()"
    DateType = "DateType()"
    TimestampType = "TimestampType()"
    DecimalType = "DecimalType()"
    DoubleType = "DoubleType()"
    FloatType = "FloatType()"
    ByteType = "ByteType()"
    IntegerType = "IntegerType()"
    LongType = "LongType()"
    ShortType = "ShortType()"

    @classmethod
    def ArrayType(cls, element_type: "SparkType") -> str:
        return f"ArrayType({element_type})"

    @classmethod
    def MapType(cls, key_type: "SparkType", val_type: "SparkType") -> str:
        return f"MapType({key_type}, {val_type})"


def create_spark_schema(schema_def: SchemaDefinition) -> "StructType":
    from pyspark.sql.types import (
        ArrayType,
        BinaryType,
        BooleanType,
        ByteType,
        DataType,
        DateType,
        DecimalType,
        DoubleType,
        FloatType,
        IntegerType,
        LongType,
        MapType,
        NullType,
        ShortType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    struct_fields = []
    for schema_field in schema_def:
        name, data_type, nullable = schema_field

        struct_fields.append(
            StructField(
                name,
                # e.g
                # "StringType()" -> StringType() or recursively call to create nested StructType from a list
                eval(data_type) if isinstance(data_type, str) else create_spark_schema(data_type),
                nullable,
            )
        )

    return StructType(struct_fields)


# COMPUTE PARAMETRIZATION
def convert_to_config_list(spark_submit_args_str: str):
    # Self-explanatory. Converts "Spark submit args" string into EMR driver format for devtime & testing convenience.
    settings = [each.strip() for each in spark_submit_args_str.split("--conf")][1:]
    return ["--conf" if (i % 2 == 0) else settings[i // 2] for i in range(2 * len(settings))]


# High Throughput CONF #1
HIGH_THROUGHPUT_SPARK_NETWORK_CONFIGS = convert_to_config_list(
    """
--conf spark.sql.broadcastTimeout=900 --conf spark.files.fetchTimeout=900s --conf spark.network.timeout=900s
--conf spark.rpc.lookupTimeout=900s --conf spark.network.auth.rpcTimeout=900s --conf spark.locality.wait=15s
--conf spark.executor.heartbeatInterval=200s --conf spark.rpc.io.clientThreads=24
--conf spark.rpc.io.serverThreads=24 --conf spark.rpc.netty.dispatcher.numThreads=24
--conf spark.hadoop.fs.s3a.connection.maximum=100 --conf spark.hadoop.fs.s3.connection.maximum=100
--conf spark.hadoop.fs.s3.getObject.initialSocketTimeoutMilliseconds=2000 --conf spark.hadoop.fs.s3.maxRetries=20
--conf spark.sql.sources.parallelPartitionDiscovery.threshold=1 --conf spark.reducer.maxReqsInFlight=12
--conf spark.reducer.maxBlocksInFlightPerAddress=64
"""
)

HIGH_THROUGHPUT_SPARK_AQE_CONFIGS = convert_to_config_list(
    """
--conf spark.sql.adaptive.enabled=true --conf spark.sql.adaptive.skewJoin.enabled=true
--conf spark.sql.adaptive.skewJoin.skewedPartitionFactor=5
--conf spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes=256M
--conf spark.sql.adaptive.forceOptimizeSkewedJoin=true
--conf spark.sql.optimizer.canChangeCachedPlanOutputPartitioning=true
"""
)

HIGH_THROUGHPUT_SPARK_SHUFFLE_PUSH_CONFIGS = convert_to_config_list(
    """
--conf spark.shuffle.push.enabled=true
--conf spark.shuffle.push.server.mergedShuffleFileManagerImpl=org.apache.spark.network.shuffle.RemoteBlockPushResolver
--conf spark.shuffle.accurateBlockSkewedFactor=5.0
--conf spark.shuffle.io.maxRetries=10
"""
)

HIGH_THROUGHPUT_SPARK_SPECULATION_CONFIGS = convert_to_config_list(
    """
--conf spark.speculation=true --conf spark.speculation.multiplier=10 --conf spark.speculation.quantile=0.99
--conf spark.speculation.interval=1000ms
"""
)

HIGH_THROUGHPUT_SPARK_DYNAMIC_ALLOCATION_CONFIGS = convert_to_config_list(
    """
--conf spark.dynamicAllocation.shuffleTracking.enabled=true --conf spark.dynamicAllocation.executorIdleTimeout=600
--conf spark.dynamicAllocation.maxExecutors=187 --conf spark.yarn.heterogeneousExecutors.enabled=false
--conf spark.dynamicAllocation.enabled=true --conf spark.dynamicAllocation.preallocateExecutors=false
"""
)

HIGH_THROUGHPUT_SPARK_JAVA_OPTIONS_LIST = convert_to_config_list(
    """
--conf "spark.driver.defaultJavaOptions=-XX:OnOutOfMemoryError='kill -9 %p'"

--conf "spark.driver.extraJavaOptions=-Djava.net.preferIPv6Addresses=false
-XX:OnOutOfMemoryError='kill -9 %p' -Xms50g -XX:MetaspaceSize=512M -Xss128M -XX:G1HeapRegionSize=32M
-XX:+UseG1GC -XX:ParallelGCThreads=20 -XX:ConcGCThreads=20 -XX:+ParallelRefProcEnabled
-XX:InitiatingHeapOccupancyPercent=0 -XX:GCTimeRatio=2 -XX:MaxGCPauseMillis=20000
-Dlog4j.configurationFile=log4j2.properties -Dfile.encoding=UTF-8 -Dio.netty.tryReflectionSetAccessible=true
-Dcom.crema.UseRetransformCache=false"

--conf "spark.executor.defaultJavaOptions=-verbose:gc -XX:+PrintGCDetails -XX:OnOutOfMemoryError='kill -9 %p'"

--conf "spark.executor.extraJavaOptions=-Djava.net.preferIPv6Addresses=false -verbose:gc -XX:+PrintGCDetails
-verbose:gc -XX:+PrintGCDetails -XX:OnOutOfMemoryError='kill -9 %p' -Xms43g -XX:MetaspaceSize=512M -Xss128M
-XX:MaxDirectMemorySize=12g -XX:G1HeapRegionSize=32M -XX:+UseG1GC -XX:ParallelGCThreads=20 -XX:ConcGCThreads=20
-XX:+ParallelRefProcEnabled -XX:InitiatingHeapOccupancyPercent=0 -XX:GCTimeRatio=2 -XX:MaxGCPauseMillis=20000
-Dlog4j.configurationFile=log4j2.properties -Dfile.encoding=UTF-8 -Dio.netty.tryReflectionSetAccessible=true
-Dcom.crema.UseRetransformCache=false"
""".replace(
        "\n", ""
    )
)

HIGH_THROUGHPUT_EXTRA_SPARK_ARGS = (
    HIGH_THROUGHPUT_SPARK_NETWORK_CONFIGS
    + HIGH_THROUGHPUT_SPARK_AQE_CONFIGS
    + HIGH_THROUGHPUT_SPARK_SHUFFLE_PUSH_CONFIGS
    + HIGH_THROUGHPUT_SPARK_SPECULATION_CONFIGS
    + HIGH_THROUGHPUT_SPARK_DYNAMIC_ALLOCATION_CONFIGS
)

EMRFS_SITE_CLUSTER_CONFIG = {
    "Classification": "emrfs-site",
    "Properties": {
        "fs.s3.connection.maximum": "100",
        "fs.s3.maxRetries": "20",
        "fs.s3a.connection.maximum": "100",
        "fs.s3n.multipart.uploads.split.size": "763363328",
        "mapred.output.direct.EmrFileSystem": "true",
        "mapred.output.direct.NativeS3FileSystem": "true",
        "mapreduce.am.max-attempts": "4",
        "mapreduce.fileoutputcommitter.algorithm.version": "2",
        "mapreduce.job.split.metainfo.maxsize": "-1",
    },
}

MAPRED_SITE_CLUSTER_CONFIG = {
    "Classification": "mapred-site",
    "Properties": {"mapreduce.map.speculative": "false", "mapreduce.reduce.speculative": "false"},
}

YARN_SITE_CLUSTER_CONFIG = {
    "Classification": "yarn-site",
    "Properties": {
        "yarn.nodemanager.aux-services": "mapreduce_shuffle,spark_shuffle",
        "yarn.nodemanager.aux-services.mapreduce_shuffle.class": "org.apache.hadoop.mapred.ShuffleHandler",
        "yarn.nodemanager.aux-services.spark_shuffle.class": "org.apache.spark.network.yarn.YarnShuffleService",
        "yarn.nodemanager.pmem-check-enabled": "false",
        "yarn.nodemanager.vmem-check-enabled": "false",
    },
}

R5_8XLARGE_SPARK_CLI_ARGS = [
    "--executor-memory",
    "130g",
    "--driver-memory",
    "130g",
    "--conf",
    "spark.driver.memoryOverhead=9000",
    "--conf",
    "spark.executor.memoryOverhead=16384",
    "--conf",
    "spark.default.parallelism=1496",
    "--conf",
    "spark.executor.cores=8",
    "--conf",
    "spark.driver.cores=8",
    "--conf",
    "spark.sql.shuffle.partitions=11750",
    "--conf",
    "spark.sql.autoBroadcastJoinThreshold=-1",
    "--conf",
    "spark.driver.maxResultSize=0",
    "--conf",
    "spark.hadoop.parquet.enable.summary-metadata=false",
]

C5D_24XLARGE_SPARK_CLI_ARGS = [
    "--executor-memory",
    "130g",
    "--driver-memory",
    "130g",
    "--conf",
    "spark.driver.memoryOverhead=9000",
    "--conf",
    "spark.executor.memoryOverhead=16384",
    "--conf",
    "spark.default.parallelism=8000",
    "--conf",
    "spark.executor.cores=8",
    "--conf",
    "spark.driver.cores=8",
    "--conf",
    "spark.sql.shuffle.partitions=11750",
    "--conf",
    "spark.sql.autoBroadcastJoinThreshold=-1",
    "--conf",
    "spark.driver.maxResultSize=0",
    "--conf",
    "spark.hadoop.parquet.enable.summary-metadata=false",
]

R5_24XLARGE_SPARK_CLI_ARGS = [
    "--executor-memory",
    "390g",
    "--driver-memory",
    "390g",
    "--conf",
    "spark.driver.memoryOverhead=4096",
    "--conf",
    "spark.executor.cores=60",
    "--conf",
    "spark.driver.cores=60",
]
