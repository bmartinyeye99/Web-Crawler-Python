from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, udf, expr, array_intersect, lit, array, array_contains
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
from sklearn.metrics import precision_score, recall_score


SMALL_XML = 'enwiki-latest-pages-articles17.xml-p20570393p22070392'
BIG_XML = 'enwiki-latest-pages-articles.xml'
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

# Remove brackets and quotes and split the string to get a list of strings
@udf(ArrayType(StringType()))
def string_to_list(director_str):
    if director_str is not None:
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
    movie_tv_sparkdf = movie_tv_sparkdf.drop_duplicates(['title','director_list'])
    return movie_tv_sparkdf


@udf(BooleanType())
def check_intersection(list1, list2):
    if 'more' in list1:
        return True
    return bool(set(list1) & set(list2))

@udf(ArrayType(StringType()))
def create_array():
    return ['not found']

# function laods whole xml file, ad filters out all pages, where movie infobox is present
def load_wiki_data():
    raw_data = spark.read.format("com.databricks.spark.xml").option("rowTag", "page").schema(schema).load(
        BIG_XML)

    filtered_film_data = raw_data.filter(
        (col("revision.text._VALUE").contains("Infobox film")) &
        (col("revision.text._VALUE").rlike(r'\{\{\s*Infobox\s*film\s*\|'))
    )
    return filtered_film_data

# function extracts searched data from the pool of wiki movie infoboxes
def extract_data_from_wiki():
    extracted_data = movie_wiki_dadta \
        .withColumn("title_wiki", custom_title_udf(col("title"))) \
        .withColumn("director_wiki", custom_director_udf(col("revision.text._VALUE"))) \
        .withColumn("music", custom_music(col("revision.text._VALUE"))) \
        .withColumn("release", custom_release(col("revision.text._VALUE"))) \
        .select("title_wiki", "director_wiki", 'music', 'release')
    return extracted_data

# joins wiki and imdb data on matching director and title columns
# def join_df(extracted_data,crawled_data):
#     initial_joined_data = (crawled_data.join(extracted_data,crawled_data.title == extracted_data.title_wiki,"leftouter"))
#     column_name = "director_wiki"
#     initial_joined_data = initial_joined_data.withColumn(column_name,
#                                                          F.when(initial_joined_data[column_name].isNull(),
#                                                                 create_array()).otherwise(
#                                                              initial_joined_data[column_name]))
#     final_joined_data = initial_joined_data \
#         .where(
#         (check_intersection(initial_joined_data.director_list, initial_joined_data.director_wiki))
#     )
#     return final_joined_data

def join_df(extracted_data, crawled_data):
    initial_joined_data = crawled_data.join(
        extracted_data,
        (
            (crawled_data.title == extracted_data.title_wiki) &
            (F.size(F.array_intersect(crawled_data.director_list, extracted_data.director_wiki)) > 0)
        ) |
        (
            (crawled_data.title == extracted_data.title_wiki) &
            (array_contains(crawled_data.director_list, 'more'))
        ),
        "leftouter"
    )

    # Replace null values in the joined columns with empty arrays
    # column_name = "director_wiki",'music','release','title_wiki'
    # for c in column_name:
    #     initial_joined_data = initial_joined_data.withColumn(
    #         c,
    #         F.when(F.col(c).isNull(), F.array()).otherwise(F.col(c))
    #     )

    # Keep all records from crawled_data
    final_joined_data = initial_joined_data

    return final_joined_data

def precision_recall():
    merged_df = pd.read_csv("small_merged.csv")
    # methond 1
    ###########
    # Step 1: Identify True Positives (TP)
    true_positives = merged_df[['director_list', 'director_wiki']].dropna().apply(
        lambda x: x[0].lower() == x[1].lower(), axis=1).sum()

    # Step 2: Identify False Positives (FP)
    false_positives = len(merged_df) - true_positives
    # Step 3: Identify False Negatives (FN)
    false_negatives = merged_df[['title_wiki', 'title']].apply(
        lambda x: pd.notna(x[0]) and pd.notna(x[1]) and not (x[0].lower() == x[1].lower()), axis=1).sum()
    # Step 4: Calculate Precision
    precision_denominator = true_positives + false_positives
    precision = true_positives / precision_denominator if precision_denominator > 0 else 0
    # Step 5: Calculate Recall
    recall_denominator = true_positives + false_negatives
    recall = true_positives / recall_denominator if recall_denominator > 0 else 0
    print(f"Precision: {precision:.2f}")
    print(f"Recall: {recall:.2f}")


    # method 2
    ###########
    for index, row in merged_df.iterrows():
        if pd.notna(row['title_wiki']) and pd.notna(row['director_wiki']):
            # Check if it's a match
            if row['title'].lower() == row['title_wiki'].lower() and row['director_list'].lower() == row['director_wiki'].lower():
                true_positives += 1
            else:
                false_positives += 1
        else:
            false_negatives += 1
    precision = true_positives / (true_positives + false_positives) if (true_positives + false_positives) > 0 else 0
    recall = true_positives / (true_positives + false_negatives) if (true_positives + false_negatives) > 0 else 0
    print(f"Precision: {precision:.2f}")
    print(f"Recall: {recall:.2f}")

findspark.init()
spark = SparkSession.builder.config("spark.jars.packages", "com.databricks:spark-xml_2.12:0.13.0").getOrCreate()

#loading data from dump
movie_wiki_dadta = load_wiki_data()

# defining custom udf functions
custom_title_udf = udf(format_date, StringType())
custom_director_udf = udf(find_director, StringType())
custom_music = udf(extract_music, StringType())
custom_release = udf(extract_release_date, StringType())
spark.udf.register("list_contains_any", list_contains_any)
custom_director_udf = udf(find_director, ArrayType(StringType()))

#dataframe holding extracted structured data from wiki
extracted_data = extract_data_from_wiki()

# movie data from imdb
crawled_data = load_movies()

final_joined_data = join_df(extracted_data,crawled_data)
final_joined_data.toPandas().to_csv('small_merged.csv', index=False)
precision_recall()
