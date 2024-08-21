import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

subsDf = glueContext.create_dynamic_frame.from_catalog(
    database="telecom-glue-pro1-database",
    table_name="telecom_data_glue_pro1"
    )
# job = Job(glueContext)
# job.init(args['JOB_NAME'], args)
# job.commit()

subsDf.printSchema()
sparkSubsDf = subsDf.toDF()
sparkSubsDf.show()
print(sparkSubsDf.count())