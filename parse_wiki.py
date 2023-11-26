import requests
from urllib.parse import urlencode, urljoin, quote, urlsplit, urlunsplit
import re

from bs4 import BeautifulSoup


def find_director(url):
    print(url)

    response = requests.get(url)
    html_content = response.text
    oneCreatorRegex = r'<th(.*)>\s*Created by\s*<\/th><td (.*)<a\s*href(.*)>\s*(.*)\s*<\/a><\/td>'
    moreCreatorsRegex = r'Created by([\s\S]*?)<ul>([\s\S]*?)<\/ul>'
    oneDirector = r'Directed by\s*<\/th><td (.*)<a\s*href(.*)>\s*(.*)\s*<\/a><\/td>'
    moreDirectorsRegex = r'Directed by([\s\S]*?)<ul>([\s\S]*?)<\/ul>'

    releaseDateRegex = r'Release date[s]*([.\s\S])*<ul><li>(.*)(\d{4}).*?<\/ul>'
    yearRegex = r'<ul><li>.*(\d{4}).*<\/li><\/ul>'

    musicRegex = r'Music by.*(<a.*>)(.*)<\/a>.*<th(.*)>P'

    if re.search('Release date[.\s\S]*<\/span><\/li>',html_content,re.DOTALL):
        releaseMatch = re.search('Release date[.\s\S]*<\/span><\/li>',html_content,re.DOTALL).group()
        if re.search(yearRegex,releaseMatch):
            releaseDate = re.search(yearRegex,releaseMatch)
            print("Release : ",releaseDate.group(1))

    if re.search(musicRegex,html_content):
        composerMatch = re.search(musicRegex,html_content).group(2)
        print("Music by : ", composerMatch)


    director_match = None
    if re.search(oneCreatorRegex,html_content) != None:
        director_match = re.search(oneCreatorRegex, html_content)
        print(director_match.group(4))

    elif len(re.findall(moreCreatorsRegex,html_content)) != 0:
        matches = re.findall(moreCreatorsRegex, html_content, re.DOTALL)
        a_contents = re.findall(r'<a\s*[^>]*>(.*?)<\/a>', matches[0][1])
        director_match = a_contents
        print(director_match)

    elif re.search(oneDirector,html_content) != None:
        print(re.search(oneDirector,html_content).group(3))

    elif len(re.findall(moreDirectorsRegex,html_content)) != 0:
        matches = re.findall(moreDirectorsRegex, html_content, re.DOTALL)
        a_contents = re.findall(r'<a\s*[^>]*>(.*?)<\/a>', matches[0][1])
        director_match = a_contents
        print(director_match)


def filter_link(links, keyword):
    valid_links = []
    for link in links:
        response = requests.get(link)
        page_content = response.text
        soup = BeautifulSoup(page_content, 'html.parser')
        h1_tag = soup.find('h1')
        filmkeyword = keyword + ' (film)'
        serieskeywod = keyword + ' (TV series)'
        if h1_tag:
            if (h1_tag.get_text().strip().lower() == keyword.strip().lower() or
                    h1_tag.get_text().strip().lower() == serieskeywod.lower().strip() or
                    h1_tag.get_text().strip().lower() == filmkeyword.lower().strip()):
                valid_links.append(link)
    return valid_links

def find_hyperlink_with_query(keyword, url):
    response = requests.get(url)
    page_content = response.text
    changed_keyword = keyword.replace(' ', '_')

    links_with_keywords = set()
    pattern = re.compile(r'<a\s+[^>]*href="/wiki/([^"]*)"[^>]*>(.*?)</a>', re.IGNORECASE)
    matches = pattern.findall(page_content)
    for href, text in matches:
        #for keyword in changed_keywords:
            #if re.search(fr'\b{re.escape(keyword)}\b', text, re.IGNORECASE):
         if changed_keyword in href:
            print(href)
            encoded_href = quote(href, safe='')
            full_url = urljoin(url, f'/wiki/{encoded_href}')
            links_with_keywords.add(full_url)
    return list(links_with_keywords)


def open_wiki_link(wlinks, keyword):
    links_with_keywords = find_hyperlink_with_query(keyword, wlinks)
    filtered_links = filter_link(links_with_keywords,keyword)
    print("ASDAD",filtered_links)
    for link in filtered_links:
        find_director(link)


open_wiki_link('https://en.wikipedia.org/wiki/Quentin_Tarantino','Grindhouse')




