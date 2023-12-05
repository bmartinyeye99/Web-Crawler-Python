from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, udf, expr
from pyspark.sql.functions import col, regexp_replace
from bs4 import BeautifulSoup
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from pyspark.sql.types import StringType, StructType, StructField
import json
import re
import findspark

def format_date(title):
    result_string = re.sub(r'\([^)]*\)', '', title).strip()
    return result_string

def find_director(text):

    pattern = r"director\s+=\s+\[?\[?(.*)\]?\]?"
    if re.findall(pattern, text):
        result = re.findall(pattern, text)
        result[0] = result[0].replace('<br>','; ').replace(']]', '').replace('[[', '').strip()
        result[0] = re.sub(r'\([^)]*\)', '', result[0])
        result[0] = result[0].split('|')[0].strip()
        return result[0] if result else 'not found'
    else:
        return 'not found'

def extract_music(text):
    pattern_for_more = r"\|\s*music\s*=\s*{{plainlist\|([\s\S]*?)\s*}}"
    patter_for_one = 'music\s*=\s*\[\[(.*)\]\]'
    err = "not found"
    if re.findall(pattern_for_more, text):
        result = re.findall(pattern_for_more, text)
        cleaned_string = result[0].replace('*', '').replace('[[', '').replace(']]', '').replace('\n', '; ').strip()
        cleaned_string = ' '.join(cleaned_string.split()).lstrip(';').strip()
        return cleaned_string if result else ("%s" % err)
    elif re.search(patter_for_one,text):
        result = re.search(patter_for_one, text)
        return result.group(1) if result else err
    else:
        return err
def extract_release_date(text):
    pattern = r'released\s*=\s*{{[a-zA-Z|\s]*\|(\d{4})\|'
    pattern_two = r'released\s*=\s*\d [a-zA-Z]* (\d{4})'
    pattern_three = r'released\s*=\s*({{[a-zA-Z|\s]*)?\|(\d{4})'
    err = "not found"
    if re.search(pattern,text):
        match = re.search(pattern, text)
        return match.group(1) if match else ("%s" % err)

    elif re.search(pattern_two,text):
        match = re.search(pattern_two, text)
        return match.group(1) if match else err

    elif re.search(pattern_three,text):
        match = re.search(pattern_three, text)
        return match.group(2) if match else err
    else:
        return err

schema = StructType([
    StructField("title", StringType(), True),
    StructField("revision", StructType([
        StructField("text", StructType([
            StructField("_VALUE", StringType(), True)
        ]), True)
    ]), True)
])

findspark.init()
spark = SparkSession.builder.config("spark.jars.packages", "com.databricks:spark-xml_2.12:0.13.0").getOrCreate()

raw_data = spark.read.format("com.databricks.spark.xml").option("rowTag", "page").schema(schema).load(
    "enwiki-latest-pages-articles17.xml-p20570393p22070392")

filtered_film_data = raw_data.filter(
    (col("revision.text._VALUE").contains("Infobox film")) &
    (col("revision.text._VALUE").rlike(r'\{\{\s*Infobox\s*film\s*\|'))
)

#filtered_film_data.select("title", "revision.text._VALUE").show(truncate=False)

print(filtered_film_data.first())
custom_title_udf = udf(format_date, StringType())
custom_text_udf = udf(find_director, StringType())
custom_music = udf(extract_music, StringType())
custom_release = udf(extract_release_date, StringType())

extracted_data = filtered_film_data \
    .withColumn("title", custom_title_udf(col("title"))) \
    .withColumn("director", custom_text_udf(col("revision.text._VALUE"))) \
    .withColumn("music", custom_music(col("revision.text._VALUE"))) \
    .withColumn("release", custom_release(col("revision.text._VALUE"))) \
    .select("title", "director",'music','release')

print(extracted_data.columns)
extracted_data.limit(10).show(truncate=False)
