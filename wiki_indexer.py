from whoosh.fields import TEXT, ID, Schema
from whoosh.index import create_in, open_dir
import os
import pandas as pd
from whoosh.qparser import MultifieldParser
from whoosh.searching import Searcher

def create_index(csv_path, index_dir):
    # Load the CSV file
    webpages = pd.read_csv(csv_path)

    # Define Whoosh schema
    schema = Schema(
        url=ID(stored=True),
        title=TEXT(stored=True),
        director=TEXT(stored=True),
        cast=TEXT(stored=True)
    )

    # Create Whoosh index
    if not os.path.exists(index_dir):
        os.mkdir(index_dir)

    index = create_in(index_dir, schema)

    # Index the webpages
    with index.writer() as writer:
        for _, row in webpages.iterrows():
            writer.add_document(
                url=row['url'],
                title=row['title'],
                director=row['director'],
                cast=row['cast']
            )

    return index

def update_csv_with_wiki_text(webpages, index, user_keywords):
    # Iterate through CSV records
    for _, row in webpages.iterrows():
        # Search Whoosh index for user-input keywords
        search_results = []

        with index.searcher() as searcher:
            for keyword in user_keywords:
                query_parser = MultifieldParser(["title", "director", "cast"], index.schema)
                query = query_parser.parse(keyword)
                results = searcher.search(query)
                search_results.extend([(keyword, result["title"], result.score) for result in results])

        # Sort and get the most relevant result
        most_relevant_result = max(search_results, key=lambda x: x[2], default=None)

        # If a relevant result is found, update the CSV record with Wikipedia text
        if most_relevant_result:
            wiki_text_file = f"{most_relevant_result[1]}.txt"  # Assuming you have saved Wikipedia text in a file
            with open(wiki_text_file, 'r', encoding='utf-8') as file:
                wiki_text = file.read()

            # Update CSV record with Wikipedia text
            webpages.at[_, 'wiki_text'] = wiki_text

    return webpages

if __name__ == "__main__":
    csv_file_path = "extraction_movies.csv"
    index_dir = "D:\MRc\FIIT\ING\sem 1\VINF\Web Scrapper\index_directory"

    # Create or load the Whoosh index
    index = create_index(csv_file_path, index_dir)

    # Load the CSV file
    webpages = pd.read_csv(csv_file_path)

    # Get user input for keywords
    user_keywords = input("Enter keywords separated by commas: ").split(',')

    # Update CSV with Wikipedia text based on user-input keywords
    updated_webpages = update_csv_with_wiki_text(webpages, index, user_keywords)

    # Save the updated CSV
    updated_webpages.to_csv("updated_extraction_movies.csv", index=False, encoding='utf-8')
