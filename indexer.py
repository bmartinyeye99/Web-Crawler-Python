from whoosh import scoring
from whoosh.fields import TEXT, ID, Schema
from whoosh.index import create_in
import os
from whoosh.index import open_dir
from whoosh.writing import AsyncWriter
import pandas as pd
from whoosh.qparser import QueryParser, MultifieldParser
from whoosh.searching import Searcher

def search_index(keyword, index_dir):
    # Open the index
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

webpages = pd.read_csv("final_merged.csv")
schema = Schema(
    #url=ID(stored=True),
    #type=TEXT(stored=True),
    title=TEXT(stored=True),
    title_wiki=TEXT(stored=True),
    director_list=TEXT(stored=True),
    director_wiki=TEXT(stored=True),
    music=TEXT(stored=True),
    release=TEXT(stored=True)
    #cast=TEXT(stored=True)
)

index_dir = "index_directory_merged"
if not os.path.exists(index_dir):
    os.mkdir(index_dir)

index = create_in(index_dir, schema)

with index.writer() as writer:
    for _, row in webpages.iterrows():
        writer.add_document(
            #url=row['url'],
            title=row['title'],
            title_wiki=row['title_wiki'],
            director_list=row['director_list'],
            director_wiki=row['director_wiki'],
            music=row['music'],
            release=row['release']

        )


# BASIC SEARCH WITH 1 KEYWORD
search_keyword = "David Fincher"
search_index(search_keyword, index_dir)

#KEYS INSENSITIVITY TEST
search_keyword = "DAVID FINCHER"
search_index(search_keyword, index_dir)
