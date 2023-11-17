from whoosh.fields import TEXT, ID, Schema
from whoosh.index import create_in
import os
from whoosh.index import open_dir
from whoosh.writing import AsyncWriter
import pandas as pd
from whoosh.qparser import QueryParser
from whoosh.searching import Searcher

webpages = pd.read_csv("extraction_movies.csv")
schema = Schema(
    url=ID(stored=True),
    type=TEXT(stored=True),
    title=TEXT(stored=True),
    director=TEXT(stored=True),
    cast=TEXT(stored=True)
)

index_dir = "D:\MRc\FIIT\ING\sem 1\VINF\Web Scrapper\index_directory"
if not os.path.exists(index_dir):
    os.mkdir(index_dir)

index = create_in(index_dir, schema)

with index.writer() as writer:
    for _, row in webpages.iterrows():
        writer.add_document(
            url=row['url'],
            title=row['title'],
            director=row['director'],
            cast=row['cast']
        )