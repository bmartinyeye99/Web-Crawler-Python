from whoosh import scoring
from whoosh.fields import TEXT, ID, Schema
from whoosh.index import create_in
import os
from whoosh.index import open_dir
from whoosh.writing import AsyncWriter
import pandas as pd
from whoosh.qparser import QueryParser, MultifieldParser
from whoosh.searching import Searcher
from whoosh import scoring


def basic_search(keyword, index_dir):
    index = open_dir(index_dir)
    searcher = index.searcher(weighting=scoring.TF_IDF())
    query_parser = MultifieldParser(["title", "title_wiki", "director_list", "director_wiki", "music", "release"],
                                    schema=schema)
    query = query_parser.parse(keyword)
    results = searcher.search(query)
    result_count = len(results)
    searcher.close()
    search_index(keyword, index_dir)
    return result_count


def case_insensitivity_search(keyword, index_dir):
    index = open_dir(index_dir)
    searcher = index.searcher(weighting=scoring.TF_IDF())
    query_parser = MultifieldParser(["title", "title_wiki", "director_list", "director_wiki", "music", "release"],
                                    schema=schema)
    query = query_parser.parse(keyword)
    results = searcher.search(query)
    result_count = len(results)
    searcher.close()
    search_index(keyword, index_dir)
    return result_count

def number_search(search_string, index_dir):
    index = open_dir(index_dir)
    searcher = index.searcher(weighting=scoring.TF_IDF())
    # Create a QueryParser
    query_parser = MultifieldParser(["title", "title_wiki"], schema=schema)

    query = query_parser.parse(f"{search_string}*")

    results = searcher.search(query)

    print(f"Search Results for '{search_string}':")

    # Print records where the search_string is part of 'title' or 'title_wiki'
    print(f"\nRecords where {str(search_string)} is part of 'title' or 'title_wiki':")
    j = 1
    for result in results:
        print(f"{j}. match:")
        print(f" Title: {result['title']}\n Title wiki: {result['title_wiki']}\n")
        j += 1

    # Construct a new query for checking if the search_string is part of 'director_list' or 'director_wiki'
    query_parser = MultifieldParser(["release"], schema=schema)
    query = query_parser.parse(f"{search_string}*")
    results = searcher.search(query)

    # Print records where the search_string is part of 'director_list' or 'director_wiki'
    print("\nRecords where search string is part of 'release':")
    i = 1
    for result in results:
        print(f"{i}. match:")
        print(f" Release: {result['release']}\n")
        i += 1

    return j,i
def missing_value_handling_search(keyword, index_dir):
    return basic_search(keyword, index_dir)


def string_in_values_test(search_string, index_dir):
    index = open_dir(index_dir)
    searcher = index.searcher(weighting=scoring.TF_IDF())
    # Create a QueryParser
    query_parser = MultifieldParser(["title", "title_wiki"], schema=schema)

    query = query_parser.parse(f"{search_string}*")

    results = searcher.search(query)

    print(f"Search Results for '{search_string}':")

    # Print records where the search_string is part of 'title' or 'title_wiki'
    print(f"\nRecords where {str(search_string)} is part of 'title' or 'title_wiki':")
    j = 1
    for result in results:
        print(f"{j}. match:")
        print(f" Title: {result['title']}\n Title wiki: {result['title_wiki']}\n")
        j += 1

    # Construct a new query for checking if the search_string is part of 'director_list' or 'director_wiki'
    query_parser = MultifieldParser(["director_list", "director_wiki"], schema=schema)
    query = query_parser.parse(f"{search_string}*")
    results = searcher.search(query)

    # Print records where the search_string is part of 'director_list' or 'director_wiki'
    print("\nRecords where search string is part of 'director_list' or 'director_wiki':")
    i = 1
    for result in results:
        print(f"{i}. match:")
        print(f" Diretor: {result['director_list']}\n Director wiki: {result['director_wiki']}\n")
        i += 1

    query_parser = MultifieldParser(["music"], schema=schema)

    query = query_parser.parse(f"{search_string}*")

    results = searcher.search(query)

    print(f"Search Results for '{search_string}':")

    # Print records where the search_string is part of 'title' or 'title_wiki'
    print(f"\nRecords where {str(search_string)} is part of 'title' or 'music':")
    k = 1
    for result in results:
        print(f"{j}. match:")
        print(f" Title: {result['title']}\n Title wiki: {result['title_wiki']}\n")
        k += 1

    return j,i,k


def search_index(keyword, index_dir):
    index = open_dir(index_dir)
    # Create a searcher object
    searcher = index.searcher(weighting=scoring.TF_IDF())
    # Create a QueryParser
    query_parser = MultifieldParser(["title", "title_wiki","director_list","director_wiki", "music", "release"], schema=schema)
    # Parse the query
    query = query_parser.parse(keyword)
    # Search the index
    results = searcher.search(query)

    print("Search Results:")
    print(f"\n Movie where searched keyword  {keyword}  is found:")
    i = 1

    for result in results:
        print(f"{i}. match :")
        print(f" Title: {result['title']}\n Title wiki: {result['title_wiki']}\n Diretor: {result['director_list']}\n "
              f"Director wiki: {result['director_wiki']}\n Music: {result['music']}\n Release: {result['release']}\n")
        i = i + 1
    i = 0
    searcher.close()

webpages = pd.read_csv("small_merged.csv")
webpages = webpages.fillna("not found")
webpages.to_csv("final_small_merged.csv",index=False)
webpages = webpages.drop_duplicates()
webpages.to_csv("final_small_merged_deduplicated.csv",index=False)

schema = Schema(

    title=TEXT(stored=True),
    title_wiki=TEXT(stored=True),
    director_list=TEXT(stored=True),
    director_wiki=TEXT(stored=True),
    music=TEXT(stored=True),
    release=TEXT(stored=True)
)

index_dir = "index_directory_merged"
if not os.path.exists(index_dir):
    os.mkdir(index_dir)

index = create_in(index_dir, schema)

with index.writer() as writer:
    for _, row in webpages.iterrows():
        writer.add_document(
            title=row['title'],
            title_wiki=row['title_wiki'],
            director_list=row['director_list'],
            director_wiki=row['director_wiki'],
            music=row['music'],
            release=row['release']

        )



search_keyword = "David Fincher"

#BASIC SEARCH WITH 1 KEYWORD
result_count = basic_search(search_keyword, index_dir)
print(f"Result Count for '{search_keyword}': {result_count}")

# # CASE INSENSIVITY TEST
result_count_case_insensitive = case_insensitivity_search("DAVID FINCHER", index_dir)
print(f"Result Count for 'DAVID FINCHER' (case insensitive): {result_count_case_insensitive}")

# # MISSING VALUES TEST
result_count_missing_value = missing_value_handling_search("not found", index_dir)
print(f"Result Count for 'not found': {result_count_missing_value}")

# # PARTIAL STRING TEST
search_string_test = "David"
count = string_in_values_test(search_string_test, index_dir)
print(f"Matches in titles: {count[0]} \n"
      f"Matches in directors: {count[1]}\n"
      f"Matches in music: {count[2]}")

# RELEASE AND TITLE
search_string_test = "300"
count = number_search(search_string_test, index_dir)
print(f"Matches in titles: {count[0]} \n"
      f"Matches in release: {count[1]}")