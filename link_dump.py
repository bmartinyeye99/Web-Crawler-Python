import re
import pandas as pd
import requests
from bs4 import BeautifulSoup
import os
from parse_wiki import open_wiki_link
def strip_xml(xml_content):
    try:
        soup = BeautifulSoup(xml_content, 'xml')
        return soup.get_text()
    except Exception as e:
        print(f"Error stripping XML: {e}")
        return ""

def strip_wiki(wlinks):
    content_txt = []
    for link in wlinks:
        try:
            if not link.startswith(('http://', 'https://')):
                link = 'https:' + link
            response = requests.get(link)
            response.raise_for_status()  # Raise an exception for bad responses (4xx and 5xx)
            html_content = response.text
            soup = BeautifulSoup(html_content, 'lxml')
            content_txt.append(soup.get_text())
        except requests.RequestException as req_ex:
            print(f"Error fetching URL {link}: {req_ex}")
    return content_txt, wlinks


def process_and_save_chunk(chunk, output_file):
    try:
        plain_text = strip_xml(chunk)
        if plain_text.strip():
            with open(output_file, 'a', encoding='utf-8') as file:
                file.write(plain_text)
    except Exception as e:
        print(f"Error processing and saving chunk: {e}")

def extract_links(txt, keywords):
    try:
        linkPattern = r'https:\/\/en(.*?)(?:\s|$|:|,)'
        matches = re.findall(linkPattern, txt)
        modified_matches = ["https://en" + match for match in matches]
        extracted = find_keywords_in_links(modified_matches, keywords)
        return extracted
    except re.error as re_ex:
        print(f"Error in regex pattern: {re_ex}")
        return []

# function findls all links in dump, that contain keywords
def find_keywords_in_links(links, keywords):
    modified_keywords = []
    for word in keywords:
        word = word.replace(' ', '_')
        modified_keywords.append(word)
    matching_links = []
    print(modified_keywords)
    for link in links:
        for modified_keyword in modified_keywords:
            if modified_keyword in link:
                matching_links.append(link)
                break
    return matching_links

def open_xml(file_path, output_text_file, keywords):
    chunk_size = 10000
    try:
        if not os.path.exists(output_text_file):
            with open(output_text_file, 'w', encoding='utf-8'):
                pass

            with open(file_path, 'r', encoding='utf-8') as file:
                while True:
                    chunk = file.read(chunk_size)
                    if not chunk:
                        break
                    else:
                        process_and_save_chunk(chunk, output_text_file)

        with open(output_text_file, 'r', encoding='utf-8') as f:
            txt = f.read()
        links_from_dump = extract_links(txt, keywords)
        return links_from_dump
    except Exception as e:
        print(f"Error in open_xml: {e}")
        return []

def strip_newlines_and_whitespace(text):
    lines = text.splitlines()
    stripped_lines = [line.rstrip() for line in lines if line.strip()]
    result = '\n'.join(stripped_lines)
    return result

def link_to_dump(keywords):
    file_path = 'enwiki-latest-abstract1.xml'
    output_text_file = file_path + '_stripped_text'
    links = open_xml(file_path, output_text_file, keywords)
    return links

def extract_keyword_from_csv():

    movies = pd.read_csv("extraction_movies_modified_with_columns.csv")

    for index, row in movies.iterrows():
        links = []
        httpslinks = []
        keywords = []

        title = str(row['title'])
        director = str(row['director'])
        keywords.append( "Star Trek")
        if director != 'more':
            keywords.append(director)
        print("Keywords : ",keywords)
        httpslinks = link_to_dump(keywords)


        print("Link for keywords : ",httpslinks)
        release_date, writer, composerMatch, director_match = open_wiki_link(keywords,httpslinks)
        if release_date != None:
            row['release date'] = release_date
        if writer != None:
            row['writer'] = release_date
        if composerMatch != None:
            row['music by'] = composerMatch
        if row['director'] == 'more' and director_match != None:
            row['director'] = director_match

    movies.to_csv("extraction_movies_modified_with_columns.csv", index=False)


extract_keyword_from_csv()