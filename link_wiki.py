import requests
from urllib.parse import urlencode, urljoin, quote
import re

def find_director(url):
    print(url)
    response = requests.get(url)
    html_content = response.text

    oneDirectorRegex = r'<th(.*)>\s*Created by\s*<\/th><td (.*)<a\s*href(.*)>\s*(.*)\s*<\/a><\/td>'
    moreDirectosRegex = r'<th[^>]*>\s*Created by([\s\S]*?)<ul>([\s\S]*?)<\/ul>'
    director_match = re.search(oneDirectorRegex, html_content)
    if director_match == None:
        matches = re.findall(moreDirectosRegex, html_content, re.DOTALL)
        a_contents = re.findall(r'<a\s*[^>]*>(.*?)<\/a>', matches[0][1])
        director_match = a_contents
    print(director_match.group(4))


def find_hyperlink_with_query(keywords, url):
    response = requests.get(url)
    page_content = response.text
    changed_keywords = []
    for word in keywords:
        word = word.replace(' ', '_')
        changed_keywords.append(word)

    links_with_keywords = set()
    pattern = re.compile(r'<a\s+[^>]*href="/wiki/([^"]*)"[^>]*>(.*?)</a>', re.IGNORECASE)
    matches = pattern.findall(page_content)
    for href, text in matches:
        for keyword in keywords:
            if re.search(fr'\b{re.escape(keyword)}\b', text, re.IGNORECASE):
                encoded_href = quote(href, safe='')
                full_url = urljoin(url, f'/wiki/{encoded_href}')
                links_with_keywords.add(full_url)
                for word in changed_keywords:
                    if "https://en.wikipedia.org/wiki/" + word == full_url:
                        titleLink = "https://en.wikipedia.org/wiki/" + word
    print(links_with_keywords)

# def open_wiki_link(wlinks):
#     links = wlinks
#     find_director(wlinks[1])
#
# open_wiki_link(['https://en.wikipedia.org/wiki/Cobra_Kai','https://en.wikipedia.org/wiki/Stranger_Things'])

find_hyperlink_with_query(["The Karate Kid",'Cobra Kai'],'https://en.wikipedia.org/wiki/Ralph_Macchio')


