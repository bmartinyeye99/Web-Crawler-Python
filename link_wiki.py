import requests
from urllib.parse import urlencode, urljoin, quote
import re

def find_director(url):
    print(url)
    response = requests.get(url)
    html_content = response.text
    # f = open("demofile2.txt", "w",encoding="utf-8")
    # f.write(html_content)
    # f.close()
    oneDirectorRegex = r'<th(.*)>\s*Created by\s*<\/th><td (.*)<a\s*href(.*)>\s*(.*)\s*<\/a><\/td>'
    moreDirectosRegex = r'<th[^>]*>\s*Created by\s*<\/th>([\s\S]*?)<ul>(?:\s*<li><a[^>]*>(.*?)<\/a><\/li>)'
    director_match = re.search(oneDirectorRegex, html_content).group(4)
    if director_match is None and director_match.strip() == "":
        print(director_match)
    # else:
    #     director_match = re.findall(moreDirectosRegex,html_content)
    #     print(director_match)

def create_wikipedia_link(query):
    base_url = "https://en.wikipedia.org/w/api.php"
    params = {
        'action': 'query',
        'format': 'json',
        'titles': query,
        'prop': 'info',
        'inprop': 'url'
    }
    full_url = f"{base_url}?{urlencode(params)}"
    return full_url

def request_wikipedia_link(query):
    link = create_wikipedia_link(query)
    response = requests.get(link)
    data = response.json()
    pages = data['query']['pages']
    page_id = next(iter(pages))
    page_url = pages[page_id]['fullurl']
    return page_url


def find_hyperlink_with_query(word, url):
    response = requests.get(url)
    page_content = response.text

    pattern = re.compile(r'<a\s+[^>]*href="/wiki/([^"]*)"[^>]*>(.*?)</a>', re.IGNORECASE)
    matches = pattern.findall(page_content)

    for href, text in matches:
        if re.search(fr'\b{re.escape(word)}\b', text, re.IGNORECASE):
            encoded_href = quote(href, safe='')
            full_url = urljoin(url, f'/wiki/{encoded_href}')
            return full_url

    return None

search_query = "Stranger Things"
wikipedia_link = request_wikipedia_link(search_query)

if wikipedia_link:
    #print(f"Wikipedia link for '{search_query}': {wikipedia_link}")

    query_word = ""
    hyperlink = find_hyperlink_with_query(query_word, wikipedia_link)
    find_director(wikipedia_link)
    # if hyperlink:
    #     print(f"Hyperlink containing '{query_word}': {hyperlink}")
    # else:
    #     print(f"No hyperlink found containing '{query_word}'.")
else:
    print(f"Could not retrieve Wikipedia link for '{search_query}'.")
