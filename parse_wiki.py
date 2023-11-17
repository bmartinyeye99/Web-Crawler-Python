import requests
from urllib.parse import urlencode, urljoin, quote, urlsplit, urlunsplit
import re

def find_director(urls):
    for link in urls:
        response = requests.get(link)
        html_content = response.text

        oneCreatorRegex = r'<th(.*)>\s*Created by\s*<\/th><td (.*)<a\s*href(.*)>\s*(.*)\s*<\/a><\/td>'
        moreCreatorsRegex = r'Created by([\s\S]*?)<ul>([\s\S]*?)<\/ul>'
        oneDirector = r'Directed by\s*<\/th><td (.*)<a\s*href(.*)>\s*(.*)\s*<\/a><\/td>'
        moreDirectorsRegex = r'Directed by([\s\S]*?)<ul>([\s\S]*?)<\/ul>'
        director_match = None
        if re.search(oneCreatorRegex,html_content) != None:
            director_match = re.search(oneCreatorRegex, html_content)
            print(director_match.group(4))

        elif len(re.findall(moreCreatorsRegex,html_content)) != 0:
            matches = re.findall(moreCreatorsRegex, html_content, re.DOTALL)
            a_contents = re.findall(r'<a\s*[^>]*>(.*?)<\/a>', matches[0][1])
            director_match = a_contents
            print("sadsadaasdasd")
            print(director_match)

        elif re.search(oneDirector,html_content) != None:
            print(re.search(oneDirector,html_content).group(3))

        elif len(re.findall(moreDirectorsRegex,html_content)) != 0:
            matches = re.findall(moreDirectorsRegex, html_content, re.DOTALL)
            a_contents = re.findall(r'<a\s*[^>]*>(.*?)<\/a>', matches[0][1])
            director_match = a_contents
            print(director_match)
        else:
            print("Not movie link")


def find_hyperlink_with_query(keywords, url):
    response = requests.get(url)
    page_content = response.text
    changed_keywords = [word.replace(' ', '_') for word in keywords]

    links_with_keywords = set()
    pattern = re.compile(r'<a\s+[^>]*href="/wiki/([^"]*)"[^>]*>(.*?)</a>', re.IGNORECASE)
    matches = pattern.findall(page_content)

    for href, text in matches:
        for keyword in changed_keywords:
            if re.search(fr'\b{re.escape(keyword)}\b', text, re.IGNORECASE):
                encoded_href = quote(href, safe='')
                full_url = urljoin(url, f'/wiki/{encoded_href}')
                links_with_keywords.add(full_url)

    return list(links_with_keywords)
# #function returns all links found on the url that contain the given keywords
# def find_hyperlink_with_query(keywords, url):
#     response = requests.get(url)
#     page_content = response.text
#     changed_keywords = []
#     for word in keywords:
#         word = word.replace(' ', '_')
#         changed_keywords.append(word)
#
#     links_with_keywords = set()
#     pattern = re.compile(r'<a\s+[^>]*href="/wiki/([^"]*)"[^>]*>(.*?)</a>', re.IGNORECASE)
#     matches = pattern.findall(page_content)
#     for href, text in matches:
#         for keyword in keywords:
#             if re.search(fr'\b{re.escape(keyword)}\b', text, re.IGNORECASE):
#                 encoded_href = quote(href, safe='')
#                 full_url = urljoin(url, f'/wiki/{encoded_href}')
#                 links_with_keywords.add(full_url)
#                 for word in changed_keywords:
#                     if "https://en.wikipedia.org/wiki/" + word == full_url:
#                         titleLink = "https://en.wikipedia.org/wiki/" + word
#
#     return list(links_with_keywords)

def open_wiki_link(wlinks, keywords):
    links_with_keywords = find_hyperlink_with_query(keywords, wlinks)
    print("ASDAD",links_with_keywords)
#    find_director(links_with_keywords)
#     find_director('https://en.wikipedia.org/wiki/The_Karate_Kid')
#     find_director('https://en.wikipedia.org/wiki/Cobra_Kai')
#     find_director('https://en.wikipedia.org/wiki/Grindhouse_(film)')
#
#     url_components = urlsplit('https://en.wikipedia.org/wiki/Grindhouse_(film)')
#     encoded_path = quote(url_components.path, safe='/')
#     encoded_link = urlunsplit(
#         (url_components.scheme, url_components.netloc, encoded_path, url_components.query, url_components.fragment))
#     print(encoded_link)
#     find_director(encoded_link)
#
# open_wiki_link(['https://en.wikipedia.org/wiki/Cobra_Kai',
#                 'https://en.wikipedia.org/wiki/Stranger_Things',

open_wiki_link('https://en.wikipedia.org/wiki/Ralph_Macchio',['The Karate Kid'])




