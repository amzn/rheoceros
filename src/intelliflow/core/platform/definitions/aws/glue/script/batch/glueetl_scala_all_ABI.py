# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""
Module that generates Scala ETL code executor with support to all RheocerOS Compute ABIs.
    ABIs: Embedded (script), Custom (Object / execute [spark, inputs, execParams]), etc

User code gets set as "client" code.
Then Glue based drivers are supposed to get the final state of this script and send it
to Glue during job creation.

As can be observed from the import statements, RheocerOS bootstrapping is not supported with
Scala ETL since runtimes are different. Therefore;

DONOT EDIT / Introduce dependencies to this module. Host applications working set won't be available
when this script is being executed by Glue.
"""
from typing import ClassVar, Optional, Set

from intelliflow.core.platform.definitions.aws.glue.script.batch.common import (
    AWS_REGION,
    BOOTSTRAPPER_PLATFORM_KEY_PARAM,
    CLIENT_CODE_ABI,
    CLIENT_CODE_BUCKET,
    CLIENT_CODE_METADATA,
    CLIENT_CODE_PARAM,
    INPUT_MAP_PARAM,
    JOB_NAME_PARAM,
    OUTPUT_PARAM,
    USER_EXTRA_PARAMS_PARAM,
)


class GlueAllABIScala:
    NONOVERRIDABLE_PARAMS: ClassVar[Set[str]] = {
        JOB_NAME_PARAM,
        INPUT_MAP_PARAM,
        CLIENT_CODE_PARAM,
        # CLIENT_CODE_METADATA, CLIENT_CODE_ABI,
        CLIENT_CODE_BUCKET,
        OUTPUT_PARAM,
        USER_EXTRA_PARAMS_PARAM,
        AWS_REGION,
    }
    CLASS_NAME: ClassVar[str] = "IntelliFlowGlueScalaApp"

    def __init__(self, var_args: Optional[Set[str]] = None, prologue_code: Optional[str] = None, epilogue_code: Optional[str] = None):
        """
        Instantiate a different version of this script.

        Parameter
        ----------
        var_args: extra arguments on top of non-overridable GlueDefaultABI::NONOVERRIDABLE_PARAMs, they can be
        either app-developer provided or Glue construct impl specific. These are exposed to embedded client do as well.

        prologue_code: A Glue based construct can provide prologue code to support more enhanced ABIs or to evaluate
        some of the 'var_args' which are construct specific (neither user provided nor NONOVERRIDABLE).

        epilogue_code: A Glue based construct can provide epilogue code to be executed at the end of the script
        to take a set of final actions based on Contruct impl and some of the 'var_args'. Ex: Any epilogue functionality
        can be exposed to app-developers as a high-level feature (such as dumping data to different medium that
        authorized IF_EXEC_ROLE already).
        """
        self._args = self.NONOVERRIDABLE_PARAMS.update(var_args) if var_args else self.NONOVERRIDABLE_PARAMS
        self._prologue_code = prologue_code if prologue_code else ""
        self._epilogue_code = epilogue_code if epilogue_code else ""

    def generate_glue_script(self) -> str:
        return f'''import com.amazonaws.services.glue.GlueContext
import com.amazonaws.services.glue.util.GlueArgParser
import com.amazonaws.services.glue.util.Job
import com.amazonaws.services.glue.log.GlueLogger

import com.amazonaws.services.s3.{{AmazonS3, AmazonS3Client}}
import com.amazonaws.services.s3.model._
import java.nio.charset.StandardCharsets
import com.amazonaws.services.glue.{{AWSGlue, AWSGlueClientBuilder}}
import com.amazonaws.services.glue.model._

import org.apache.spark.SparkContext
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql._

import org.apache.commons.io.IOUtils

import scala.collection.JavaConverters._
import scala.collection.Map
import scala.collection.Seq
import scala.collection.Iterable
import scala.collection.mutable.ArrayBuffer
import scala.util.parsing.json._
import scala.util.control.Breaks._

import java.util.Calendar

object {self.CLASS_NAME} {{
  import scala.reflect.runtime.universe
  import scala.tools.reflect.ToolBox
  private val runtime_mirror = universe.runtimeMirror(getClass.getClassLoader)
  //private val tb = universe.runtimeMirror(getClass.getClassLoader).mkToolBox()

  def main(sysArgs: Array[String]) {{
    val sc: SparkContext = new SparkContext()
    val glueContext: GlueContext = new GlueContext(sc)
    val spark: SparkSession = glueContext.getSparkSession
    import spark.implicits._

    val logger = new GlueLogger

    val client = new AmazonS3Client()

    val if_core_args = GlueArgParser.getResolvedOptions(sysArgs, Seq({",".join(['"{0}"'.format(arg) for arg in self._args])}).toArray)
    val user_args = GlueArgParser.getResolvedOptions(sysArgs, JSON.parseFull(if_core_args("{USER_EXTRA_PARAMS_PARAM}")).get.asInstanceOf[List[String]].toArray)
    // combine both maps
    val args = if_core_args ++ user_args

    val aws_region = args("{AWS_REGION}")
    val glue_client = AWSGlueClientBuilder.standard().withRegion(aws_region).build()
    val code_bucket = args("{CLIENT_CODE_BUCKET}")
    val code_obj = client.getObject(new GetObjectRequest(code_bucket, args("{CLIENT_CODE_PARAM}")))
    val client_code = new String(IOUtils.toByteArray(code_obj.getObjectContent()), StandardCharsets.UTF_8)

    val input_map_obj = client.getObject(new GetObjectRequest(code_bucket, args("{INPUT_MAP_PARAM}")))
    val input_map_str = new String(IOUtils.toByteArray(input_map_obj.getObjectContent()), StandardCharsets.UTF_8)

    val output_obj = client.getObject(new GetObjectRequest(code_bucket, args("{OUTPUT_PARAM}")))
    val output_str = new String(IOUtils.toByteArray(output_obj.getObjectContent()), StandardCharsets.UTF_8)

    // TODO currently Scala mode only supports _SUCCESS file does not use the completion file from the output signal.
    sc.hadoopConfiguration.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "true")

    def create_view(name: String, sql_code: String): Dataset[Row] = {{
        val df = spark.sql(sql_code)
        df.registerTempTable(name)
        df
    }}

    def check_quotes(value: Any): Any = {{
       value match {{
         case s: String => s""""$value"""" 
         case _  => value
       }}
    }}
    
    def create_query(provider: String, table_name: String, partition_keys: Seq[String], all_partitions: Seq[Seq[Any]]): String = {{
        val pr_tb = f"`$provider%s`.`$table_name%s`"
        val partition_conditions = ArrayBuffer[String]()
        for ((partition_key, i) <- partition_keys.zipWithIndex) {{
            val partition_values = for (partition <- all_partitions) yield check_quotes(partition(i))
            val condition = s"$partition_key in (${{(partition_values.mkString(","))}})"
            partition_conditions += condition
        }}
        var where_clause = ""
        if (partition_conditions.size > 0) {{ 
            where_clause = s"${{partition_conditions.mkString(" AND ")}}"
        }}
        var uncompacted_query = s"SELECT * FROM ${{pr_tb}} "

        if (where_clause.length() > 0) {{
            uncompacted_query = uncompacted_query + " WHERE " + where_clause
        }}
        uncompacted_query
    }}

    Job.init(args("{JOB_NAME_PARAM}"), glueContext, args.asJava)

    val input_map = JSON.parseFull(input_map_str).get.asInstanceOf[Map[String, Any]]
    
    def get_partition(glue_client: AWSGlue, database: String, table: String, partitions: Seq[String]): GetPartitionResult = {{
        val req = new GetPartitionRequest()
          .withDatabaseName(database)
          .withTableName(table)
          .withPartitionValues(partitions.toList.asJava)

        try {{
          glue_client.getPartition(req)
        }} catch {{
          case e: EntityNotFoundException => null
          case t: Throwable               => throw t
        }}
    }}
    
    def is_partition_present(glue_client: AWSGlue, database: String, table: String, partitions: Seq[String]): Boolean = {{
        val partition = get_partition(glue_client, database, table, partitions)
        partition != null
    }}

    def load_input_df(input: Map[String, Any], sc: SparkContext, aws_region: String): Dataset[Row] = {{
        val encryption_key: String = input("encryption_key").asInstanceOf[String]
        val resource_type: String = input("resource_type").asInstanceOf[String] 
        if (encryption_key != null && !encryption_key.isEmpty)  {{
            // first set encryption (if defined)
            if (resource_type == "S3") {{
                sc.hadoopConfiguration.set("spark.hadoop.fs.s3.cse.enabled", "true")
                sc.hadoopConfiguration.set("spark.hadoop.fs.s3.cse.kms.keyId", encryption_key)
                sc.hadoopConfiguration.set("spark.hadoop.fs.s3.cse.kms.region", aws_region)
                sc.hadoopConfiguration.set("spark.hadoop.fs.s3.cse.encryptionMaterialsProvider", "com.amazon.ws.emr.hadoop.fs.cse.KMSEncryptionMaterialsProvider")
            }}  else {{
                throw new RuntimeException("Dont know how to use encryption_key for resource type: %s".format(resource_type))
            }}
        }}
        
        val proxy: Map[String, Any] = input("proxy").asInstanceOf[Map[String, Any]]
        var database: String = null
        var table_name: String = null
        if (proxy != null && proxy("resource_type").asInstanceOf[String] == "GLUE_TABLE") {{
            val proxy_attrs: Map[String, Any] = proxy("attrs").asInstanceOf[Map[String, Any]]
            database = proxy_attrs("_database").asInstanceOf[String]
            table_name = proxy_attrs("_table_name").asInstanceOf[String]
        }} else if (resource_type == "GLUE_TABLE") {{
            val first_materialized_path = input("resource_materialized_paths").asInstanceOf[List[String]](0)
            val gt_path_parts = first_materialized_path.substring("glue_table://".length, first_materialized_path.length).split("/")
            database = gt_path_parts(0)
            table_name = gt_path_parts(1)
        }}

        var input_df: Dataset[Row] = null
        var new_df: Dataset[Row] = null
        for ((resource_path, i) <- input("resource_materialized_paths").asInstanceOf[List[String]].zipWithIndex) {{
            breakable {{
                if (database != null && table_name != null) {{
                    val partition_keys = input("partition_keys").asInstanceOf[Seq[String]]
                    var next_partitions = input("partitions").asInstanceOf[Seq[Seq[Any]]]
                    if (partition_keys.size > 0) {{
                        // leave the current partition only (e.g [["US", "20220206"]]
                        next_partitions = next_partitions.zipWithIndex.filter{{case (part, index) => index == i }}.map(_._1)
                    }}
                    if (next_partitions.size == 0 || !(input("range_check_required").asInstanceOf[Boolean] || input("nearest_the_tip_in_range").asInstanceOf[Boolean]) || is_partition_present(glue_client, database, table_name, next_partitions(0).map(_.toString))) {{
                        new_df = spark.sql(create_query(database, table_name, partition_keys, next_partitions))
                    }} else if (!input("range_check_required").asInstanceOf[Boolean]) {{
                        logger.error("%s path does not exist. Since range check is not required. Continuing with next available path".format(resource_path))
                        break
                    }} else {{
                        throw new RuntimeException(s"""%s path does not exist!
                                                   Either there is a problem with range_check mechanism or the partition has been deleted 
                                                   after the execution has started.""".format(resource_path))
                    }}
                }} else {{
                    try {{
                            new_df = spark.read.format(input("data_format").asInstanceOf[String])
                                               .option("delimiter", input("delimiter").asInstanceOf[String])
                                               .option("header", "true")
                                               .option("inferSchema", "true")
                                               .load(resource_path)
                    }}
                    //Path not found exception is considered Analysis Exception within Spark code. Please check the
                    //link below for reference.
                    //https://github.com/apache/spark/blob/1b609c7dcfc3a30aefff12a71aac5c1d6273b2c0/sql/catalyst/src/main/scala/org/apache/spark/sql/errors/QueryCompilationErrors.scala#L977
                    catch {{
                        case e: AnalysisException => {{
                            if (e.message.contains("Path does not exist") && (!input("range_check_required").asInstanceOf[Boolean])) {{
                                logger.error("%s path does not exist. Since range check is not required. Continuing with next available path".format(resource_path))
                                break
                            }} else {{
                                throw e
                            }}
                        }}     
                    }}
                }}
            }}
            if (input_df == null) {{
                input_df = new_df
            }} else {{
                input_df = input_df.unionAll(new_df)
            }}
            if (input("nearest_the_tip_in_range").asInstanceOf[Boolean]) {{
                break
            }}
        }}
        if (input_df == null) {{
            logger.error("Looks like none of the input materialised path exist for this input: %s. Check input details below".format(JSONObject(input.toMap).toString()))
            logger.error("Refer input materialised paths: %s".format(input("resource_materialized_paths").asInstanceOf[List[String]].mkString(" ")))
            throw new RuntimeException("input_df is None. Couldnt find any materialised path for this input: %s".format(JSONObject(input.toMap).toString()))
        }}
        input_df
    }}

    val variables = ArrayBuffer[String]()
    val input_table = scala.collection.mutable.Map[String, Dataset[Row]]()
    val inputs = ArrayBuffer[Dataset[Row]]()
    // assign input dataframes to user provided alias'
    for ((input_raw, i) <- input_map("data_inputs").asInstanceOf[Seq[Any]].zipWithIndex) {{
        val input: Map[String, Any] = input_raw.asInstanceOf[Map[String, Any]]
        var input_df: Dataset[Row] = load_input_df(input, sc, aws_region)
            
        val alias: String = input("alias").asInstanceOf[String]
        if (alias.length() > 0) {{
            input_df.registerTempTable(alias)
            input_table(alias) = input_df
            variables += s"val ${{alias}} = ${{"inputTable(" + s""""${{alias}}"""" + ")"}}.asInstanceOf[org.apache.spark.sql.Dataset[org.apache.spark.sql.Row]]"
        }}
        input_df.registerTempTable(f"input$i%s")
        inputs += input_df
        variables += s"val ${{"input" + i.toString}} = ${{"inputs(" + i.toString + ")"}}.asInstanceOf[org.apache.spark.sql.Dataset[org.apache.spark.sql.Row]]"
    }}

    //// TODO assign timers to user provided alias'
    //for (input, i) <- input_map("timers").asInstanceOf(List[Map[String, Any]]).zipWithIndex) {{
    //    if (input("alias").length() > 0) {{
    //        exec("{{0}} = "{{1}}"".format(input('alias'), input('time')))
    //        exec('{{0}}_signal = input_signal'.format(input("alias")))
    //    }}
    //    exec('timer{{0}} = "{{1}}"'.format(i, input('time')))
    //}}

    val output_param = JSON.parseFull(output_str).get.asInstanceOf[Map[String, Any]]
    val dimensions = output_param("dimension_map").asInstanceOf[Map[String, Any]]
    val inputTable = input_table

    // TODO
    // exec("{self._prologue_code}")

    // execute client code
    // 1- Embedded Script
    var variables_plus_client_code = new String()
    for (variable <- variables) {{
        variables_plus_client_code = variables_plus_client_code + variable + scala.util.Properties.lineSeparator
    }}
    variables_plus_client_code = variables_plus_client_code + client_code
    logger.info("RheocerOS wrapped client Scala code: " + variables_plus_client_code.stripMargin)

    def compile[A](code: String): (SparkSession, ArrayBuffer[Dataset[Row]], scala.collection.mutable.Map[String, Dataset[Row]], Map[String, Any], Map[String, String]) => A = {{
      val tb = runtime_mirror.mkToolBox()
      val tree = tb.parse(
        s"""
           |def wrapper(spark: org.apache.spark.sql.SparkSession, inputs: scala.collection.mutable.ArrayBuffer[org.apache.spark.sql.Dataset[org.apache.spark.sql.Row]], inputTable: scala.collection.mutable.Map[String, org.apache.spark.sql.Dataset[org.apache.spark.sql.Row]], dimensions: Map[String, Any], args: Map[String, String]): Any = {{
           |  import org.apache.spark.sql.functions._
           |  $code
           |}}
           |wrapper _
          """.stripMargin)
      val f = tb.compile(tree)
      val wrapper = f()
      wrapper.asInstanceOf[(SparkSession, ArrayBuffer[Dataset[Row]], scala.collection.mutable.Map[String, Dataset[Row]], Map[String, Any], Map[String, String]) => A]
    }}

    val f = compile[Dataset[Row]](variables_plus_client_code.stripMargin)
    val output = f(spark, inputs, inputTable, dimensions, args)

    //// 2- ClassPath
    //// TODO postponed since this might not be necessary because embedded script can actually take care of this mode as well.
    //// 2.1- type: Classs
    ////val executableEntity = Class.forName("entity").newInstance().asInstanceOf[Object]
    ////
    //// 2.2- type: Object
    //// Alternative for Object
    ////val cons = Class.forName(objectName).getDeclaredConstructors(); 
    ////cons(0).setAccessible(true);
    ////val someObjectTrait:SomeTrait = cons(0).newInstance().asInstanceOf[Object]
    //val executableEntity = Class.forName("PATH" + "$").getField("MODULE$").get(null).asInstanceOf[Object]
    //output = executableEntity.getClass().getDeclaredMethod("execute").invoke(spark, inputs, dimensions)
    //// output = exec(client_code)

    // TODO        
    // exec("{self._epilogue_code}")

    output.write
          .format(output_param("data_format").asInstanceOf[String])
          .option("header", "true")
          .option("delimiter", output_param("delimiter").asInstanceOf[String])
          .mode("overwrite")
          .save(output_param("resource_materialized_path").asInstanceOf[String])

    Job.commit()
}}
}}
'''


class GlueDefaultABIScala:
    pass
