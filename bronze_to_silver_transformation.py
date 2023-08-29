cmd1
%md
##Viewing the files in the bronze container of the SalesLT folder


cmd2
dbutils.fs.ls('/mnt/test1/SalesLT')


cmd3
%md
###There currently no files in the silver container. When the first transformation in the bronze container is done, we will move them here. 


cmd4
dbutils.fs.ls('/mnt/silver/')


cmd5
%md
##Transformation for all the tables


cmd6
table_name = []


cmd6
#for each loop to iterate through the bronze (test1) container
for i in dbutils.fs.ls('mnt/test1/SalesLT/'):
    table_name.append(i.name.split('/')[0])


cmd7
table_name


cmd8
from pyspark.sql.functions import from_utc_timestamp, date_format
from pyspark.sql.types import TimestampType

for i in table_name:
    path = '/mnt/test1/SalesLT/' + i + '/' + i + '.parquet'
    df = spark.read.format('parquet').load(path)
    column = df.columns

    for col in column:
        if "Date" in col or "date" in col:
            df = df.withColumn(col, date_format(from_utc_timestamp(df[col].cast(TimestampType()),"UTC"), "yyyy-MM-dd"))

    output_path = '/mnt/silver/SalesLT/' + i + '/'
    df.write.format('delta').mode("overwrite").save(output_path)


cmd9
display(df)