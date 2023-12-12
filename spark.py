from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, udf, expr, array_intersect, lit, array
from pyspark.sql.functions import col, regexp_replace
from bs4 import BeautifulSoup
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, ArrayType, size, BooleanType
from pyspark.sql.types import StringType, StructType, StructField
from pyspark.sql import Row
import json
import re
import findspark
import pandas as pd


def list_contains_any(element, target_list):
    return any(e in target_list for e in element)



def format_date(title):
    result_string = re.sub(r'\([^)]*\)', '', title).strip()
    return result_string

def find_director(text):
    pattern = r"director\s+=\s+\[?\[?(.*)\]?\]?"
    if re.findall(pattern, text):
        result = re.findall(pattern, text)
        result[0] = result[0].replace('<br>', '; ').replace(']]', '').replace('[[', '').strip()
        result[0] = re.sub(r'\([^)]*\)', '', result[0])
        result[0] = result[0].split('|')[0].strip()
        # Split the result on ';' and return a list
        return [director.strip() for director in result[0].split(';')] if result else ['not found']
    else:
        return ['not found']

def extract_music(text):
    pattern_for_more = r"\|\s*music\s*=\s*{{plainlist\|([\s\S]*?)\s*}}"
    patter_for_one = 'music\s*=\s*\[\[(.*)\]\]'
    err = ["not found"]
    if re.findall(pattern_for_more, text):
        result = re.findall(pattern_for_more, text)
        result[0] = result[0].replace('<br\s*/?>', '; ').replace('*', '').replace('[[', '').replace(']]', '').replace('\n', ', ').strip()
        result[0] = ' '.join(result[0].split()).lstrip(',').strip()
        return [result[0].strip() for str in result[0].split(';')] if result else ['not found']
    elif re.search(patter_for_one,text):
        result = re.search(patter_for_one, text)
        return [result.group(1)] if result else err
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

@udf(ArrayType(StringType()))
def string_to_list(director_str):
    if director_str is not None:
        # Remove brackets and quotes and split the string to get a list of strings
        return [item.strip(" '") for item in director_str.strip("[]").split(",")]
    else:
        return []


schema = StructType([
    StructField("title", StringType(), True),
    StructField("revision", StructType([
        StructField("text", StructType([
            StructField("_VALUE", StringType(), True)
        ]), True)
    ]), True)
])


def load_movies():
    schema2 = StructType([
        StructField("title", StringType(), True),
        StructField("director", StringType(), True)  # Assume "director" column in the CSV is of type StringType
    ])
    movie_tv_df = pd.read_csv("extraction_movies_modified.csv")
    movie_tv_df.drop(['url'], axis='columns', inplace=True)
    movie_tv_sparkdf = spark.createDataFrame(movie_tv_df, schema=schema2)
    movie_tv_sparkdf = movie_tv_sparkdf.withColumn("director_list", string_to_list("director"))
    movie_tv_sparkdf = movie_tv_sparkdf.drop('director')
    return movie_tv_sparkdf


@udf(BooleanType())
def check_intersection(list1, list2):
    if 'more' in list1:
        return True
    return bool(set(list1) & set(list2))

@udf(ArrayType(StringType()))
def create_array():
    return ['not found']

findspark.init()
spark = SparkSession.builder.config("spark.jars.packages", "com.databricks:spark-xml_2.12:0.13.0").getOrCreate()

raw_data = spark.read.format("com.databricks.spark.xml").option("rowTag", "page").schema(schema).load(
    "enwiki-latest-pages-articles.xml")

filtered_film_data = raw_data.filter(
    (col("revision.text._VALUE").contains("Infobox film")) &
    (col("revision.text._VALUE").rlike(r'\{\{\s*Infobox\s*film\s*\|'))
)


custom_title_udf = udf(format_date, StringType())
custom_director_udf = udf(find_director, StringType())
custom_music = udf(extract_music, StringType())
custom_release = udf(extract_release_date, StringType())
spark.udf.register("list_contains_any", list_contains_any)
custom_director_udf = udf(find_director, ArrayType(StringType()))

extracted_data = filtered_film_data \
    .withColumn("title_wiki", custom_title_udf(col("title"))) \
    .withColumn("director_wiki", custom_director_udf(col("revision.text._VALUE"))) \
    .withColumn("music", custom_music(col("revision.text._VALUE"))) \
    .withColumn("release", custom_release(col("revision.text._VALUE"))) \
    .select("title_wiki", "director_wiki", 'music', 'release')

crawled = load_movies()

initial_joined_data = crawled.join(
    extracted_data,
    crawled.title == extracted_data.title_wiki,
    "left"
)

column_name = "director_wiki"
initial_joined_data = initial_joined_data.withColumn(column_name,
                                            F.when(initial_joined_data[column_name].isNull(),
                                            create_array()).otherwise(initial_joined_data[column_name]))


final_joined_data = initial_joined_data \
    .where(
        (check_intersection(initial_joined_data.director_list, initial_joined_data.director_wiki))
)

def precision_recall():

    precia = load_movies().toPandas()
    precib = pd.read_csv("final_merged.csv.csv")

    true_positives = len(precib)
    false_positives = len(precia) - true_positives
    false_negatives = len(extracted_data.toPandas()) - true_positives

    precision = true_positives / (true_positives + false_positives)
    recall = true_positives / (true_positives + false_negatives)


final_joined_data.toPandas().to_csv('merged.csv', index=False)
