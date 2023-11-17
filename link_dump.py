import re
import requests
from bs4 import BeautifulSoup
import os
import re

def strip_xml(xml_content):
    soup = BeautifulSoup(xml_content, 'xml')
    return soup.get_text()

def strip_wiki(wlinks):
    content_txt = []
    for link in wlinks:
        if not link.startswith(('http://', 'https://')):
            link = 'https:' + link
        response = requests.get(link)
        html_content = response.text
        soup = BeautifulSoup(html_content, 'lxml')
        content_txt.append(soup.get_text())
    return content_txt, wlinks


def process_and_save_chunk(chunk, output_file):
    plain_text = strip_xml(chunk)
    if plain_text.strip():
        with open(output_file, 'a', encoding='utf-8') as file:
            file.write(plain_text)

def extract_links(txt,keywords):
    linkPattern = r'\/\/en(.*?)(?:\s|$|:|,)'
    matches = re.findall(linkPattern, txt)
    modified_matches = ["//en" + match for match in matches]
    extracted = find_keywords_in_links(modified_matches, keywords)
    return extracted

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
def open_xml(file_path,output_text_file,keywords):
    chunk_size = 10000
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

    f = open(output_text_file, 'r', encoding='utf-8')
    txt = f.read()
    links_from_dump = extract_links(txt,keywords)
    return links_from_dump

def strip_newlines_and_whitespace(text):
    lines = text.splitlines()
    stripped_lines = [line.rstrip() for line in lines if line.strip()]
    result = '\n'.join(stripped_lines)
    return result

def link_to_dump(keywords):
    file_path = 'enwiki-latest-abstract1.xml'
    output_text_file = file_path + '_stripped_text'
    links = open_xml(file_path,output_text_file,keywords)
    return links

keywords = ["Star Wars","Star Trek"]
links = link_to_dump(keywords)
print(links)
wikitext,links = strip_wiki(links)
for i in range(len(wikitext)):
    title = ''.join(keywords).replace(' ', '_')
    filename = title + '_' + str(i) + ".txt"
    if os.path.exists(filename):
        with open(filename, 'w',encoding='utf-8') as file:
            stripped_wikitext = strip_newlines_and_whitespace(wikitext[i])
            file.write(stripped_wikitext)
    else:
        with open(filename, 'w',encoding='utf-8') as file:
            stripped_wikitext = strip_newlines_and_whitespace(wikitext[i])
            file.write(stripped_wikitext)

# output_file = 'output.txt'
# with open(output_file, 'w', encoding='utf-8') as file:
#     file.write(stripped_wikitext)