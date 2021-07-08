from pyspark.sql.functions import *
from pyspark.sql import types
from pyspark.sql.functions import col, expr, when,lit
from pyspark.sql.window import Window
import pyspark.sql.functions as F
from pyspark.context import SparkContext
from pyspark.sql.types import *

from pyspark.sql import SparkSession

# spark_context = SparkContext()
spark = SparkSession.builder.getOrCreate()

def get_df_from_glue_table(table_name,db_name,bucket_name):

    df=spark.read.option("mergeSchema","true") \
        .parquet("s3://{bucket_name}/{db_name}/{table_name}/".format(table_name=table_name,db_name=db_name,bucket_name=bucket_name))

    if ('Op' not in df.columns) and ('op' not in df.columns):
        df = df.withColumn('Op', F.lit(''))
    partition_over = Window.partitionBy("id").orderBy(col("updated_at").desc(), col("Op").asc_nulls_last())

    results_df = df.withColumn("row_num", F.row_number().over(partition_over)) \
        .filter(col("row_num") == 1) \
        .withColumn("updated_at",from_utc_timestamp("updated_at","Asia/Karachi")) \
        .withColumn("created_at",from_utc_timestamp("created_at","Asia/Karachi")) \
        .drop("row_num") \
        .filter(df.Op!='D')


    for dtype in results_df.dtypes:
        if dtype[1] == "tinyint":
            results_df = results_df.withColumn(
                dtype[0], col(dtype[0]).cast(types.StringType())
            )

    return results_df

def write_into_refined_stage(df,dst_table_name,path):
    hudiOptions = {
        'hoodie.table.name':dst_table_name+'_result' ,
        'hoodie.datasource.write.recordkey.field': 'id',
        'hoodie.datasource.write.partitionpath.field': 'created_at',
        'hoodie.datasource.write.precombine.field': 'updated_at',
        'hoodie.datasource.hive_sync.enable': 'true',
        'hoodie.datasource.hive_sync.table': dst_table_name+'_result' ,
        'hoodie.datasource.hive_sync.partition_fields': 'created_at',
        'hoodie.datasource.hive_sync.partition_extractor_class': 'org.apache.hudi.hive.MultiPartKeysValueExtractor'
    }
    df.write.format('org.apache.hudi') \
        .option('hoodie.datasource.write.operation', 'insert') \
        .options(**hudiOptions).mode('overwrite') \
        .save(path)

##########################


def event_schema(dst_table_name,db_name,bucket_name):

    events_log =get_df_from_glue_table("events_log",db_name,bucket_name) \
        .select('id',
                'user_id',
                'meta',
                'created_at',
                'updated_at',
                'Op'
                )

    path="s3://{bucket_name}/{db_name}/{dst_table_name}_result/".format(dst_table_name=dst_table_name,db_name=db_name,bucket_name=bucket_name)
    write_into_refined_stage(events_log,dst_table_name,path)
    return events_log

try:
    event_schema=event_schema("events_log","bazaar_events","alldatabasesdestination")

except Exception as e:
    print(e)
    raise Exception(e)
finally:
    print("finished")

