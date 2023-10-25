from whoosh.fields import TEXT, ID, Schema
from whoosh.index import create_in
import os
from whoosh.index import open_dir
from whoosh.writing import AsyncWriter
import pandas as pd


webpages = pd.read_csv("web_data.csv")


schema = Schema(
    title=TEXT(stored=True),
    actor=TEXT(stored=True),
    director=TEXT(stored=True),
    genre=TEXT(stored=True),
    year=TEXT(stored=True),
)

if os.path.exists(INDEX_PATH):
    print('removing old index files')
    shutil.rmtree(INDEX_PATH)

if not os.path.exists(INDEX_PATH):
    print('creating index files')
    os.mkdir(INDEX_PATH)

    # creating index writer
    ix = index.create_in(INDEX_PATH, schema)
    writer = ix.writer()

for _, row in webpages.iterrows():
    url = row['URL']
    content = row['HTML_Content']

    # Create a document
    doc = {
        'url': url,
        'content': content
    }

    # Add the document to the index
    writer.add_document(**doc)

# Commit the changes
writer.commit()