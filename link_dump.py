import re

from bs4 import BeautifulSoup
import os

def strip_tags(xml_content):
    soup = BeautifulSoup(xml_content, 'xml')
    return soup.get_text()

def process_and_save_chunk(chunk, output_file):
    plain_text = strip_tags(chunk)
    if plain_text.strip():
        with open(output_file, 'a', encoding='utf-8') as file:
            file.write(plain_text)

def extract_links(txt):
    linkPattern = r'\/\/en(.*?)(?:\s|$|:|,)'
    matches = re.findall(linkPattern, txt)
    modified_matches = ["//en" + match for match in matches]
    find_keywords(modified_matches)

def find_keywords(links):
    modified_keywords = []
    for word in keywords:
        word = word.replace(' ', '_')
        modified_keywords.append(word)

    print(modified_keywords)
    matching_links = []

    for link in links:
        for modified_keyword in modified_keywords:
            if modified_keyword in link:
                matching_links.append(link)
                break
    print(matching_links)

def open_xml():
    if not os.path.exists(output_text_file):
        with open(output_text_file, 'w', encoding='utf-8'):
            pass

        with open(file_path, 'r', encoding='utf-8') as file:
            while True:
                chunk = file.read(chunk_size)
                if not chunk:
                    break
                else: process_and_save_chunk(chunk, output_text_file)
    else:
        f = open(output_text_file, 'r', encoding='utf-8')
        txt = f.read()
        extract_links(txt)

file_path = 'enwiki-latest-abstract1.xml'
output_text_file = file_path + '_stripped_text'
chunk_size = 10000
keywords = ["Star Wars", 'Star Trek']
open_xml()



