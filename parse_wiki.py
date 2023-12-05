import html
import requests
from urllib.parse import urlencode, urljoin, quote
import re
from bs4 import BeautifulSoup

def find_release_date(html_content, headers):
    try:
        soup = BeautifulSoup(html_content, 'html.parser')
        release_date_th = soup.find('th', string=headers)
        release_date_row = release_date_th.find_parent('tr')
        date_element = release_date_row.find('li')
        info = date_element.text
        return info
    except Exception as e:
        print(f"Error in find_release_date: {e}")
        return None

def find_writer(html_content):
    try:
        soup = BeautifulSoup(html_content, 'html.parser')
        written_by_th = soup.find('th', string=["Written by", 'Story by'])
        writers = set()
        if written_by_th:

            written_by_row = written_by_th.find_parent('tr')
            td_tags = written_by_row.find_all('td')
            li_tags = written_by_row.find_all('li')
            if not li_tags:
                for td in td_tags:
                    writers.add(td.get_text())
            else:
                for li in li_tags:
                    writers.add(li.get_text())

        return writers
    except Exception as e:
        print(f"Error in find_writer: {e}")
        return None

def find_wiki_data(url):
    try:
        print(url)

        response = requests.get(url)
        html_content = response.text
        oneCreatorRegex = r'<th(.*)>\s*Created by\s*<\/th><td (.*)<a\s*href(.*)>\s*(.*)\s*<\/a><\/td>'
        moreCreatorsRegex = r'Created by([\s\S]*?)<ul>([\s\S]*?)<\/ul>'
        oneDirector = r'Directed by\s*<\/th><td (.*)<a\s*href(.*)>\s*(.*)\s*<\/a><\/td>'
        moreDirectorsRegex = r'Directed by([\s\S]*?)<ul>([\s\S]*?)<\/ul>'
        musicRegex = r'Music by.*(<a.*>)(.*)<\/a>.*<th(.*)>P'

        release_date = find_release_date(html_content, ['Release dates', 'Release', 'Release date'])
        writer = find_writer(html_content)

        print("Release date : ", release_date)
        print('Writer : ', writer)

        composerMatch = None
        if re.search(musicRegex, html_content):
            composerMatch = re.search(musicRegex, html_content).group(2)
            print("Music by : ", composerMatch)

        director_match = None
        if re.search(oneCreatorRegex, html_content) != None:
            director_match = re.search(oneCreatorRegex, html_content)
            print('Creator : ', director_match.group(4))

        elif len(re.findall(moreCreatorsRegex, html_content)) != 0:
            matches = re.findall(moreCreatorsRegex, html_content, re.DOTALL)
            a_contents = re.findall(r'<a\s*[^>]*>(.*?)<\/a>', matches[0][1])
            director_match = a_contents
            print("Creators : ", director_match)

        elif re.search(oneDirector, html_content) != None:
            print("Director : ", re.search(oneDirector, html_content).group(3))

        elif len(re.findall(moreDirectorsRegex, html_content)) != 0:
            matches = re.findall(moreDirectorsRegex, html_content, re.DOTALL)
            a_contents = re.findall(r'<a\s*[^>]*>(.*?)<\/a>', matches[0][1])
            director_match = a_contents
            print("Directors : ", director_match)

        return release_date, writer, composerMatch, director_match

    except Exception as e:
        print(f"Error in find_wiki_data: {e}")


def filter_link(links, keyword):
    try:
        valid_links = []
        for link in links:
            response = requests.get(link)
            page_content = response.text
            soup = BeautifulSoup(page_content, 'html.parser')
            h1_tag = soup.find('h1')
            film_keyword = keyword + ' (film)'
            series_keyword = keyword + ' (TV series)'
            if h1_tag:
                if (h1_tag.get_text().strip().lower() == keyword.strip().lower() or
                        h1_tag.get_text().strip().lower() == series_keyword.lower().strip() or
                        h1_tag.get_text().strip().lower() == film_keyword.lower().strip()):
                    valid_links.append(link)
        return valid_links
    except Exception as e:
        print(f"Error in filter_link: {e}")
        return []

def find_wiki_hyperlinks_with_query(keyword, urls):
    links_with_keywords = set()

    for url in urls:
        links_with_keywords.add(url)
        response = requests.get(url)
        page_content = response.text
        changed_keyword = []

        changed_keyword = keyword.replace(' ', '_')

        # find all alternative links in text
        pattern = re.compile(r'<a\s+[^>]*href="/wiki/([^"]*)"[^>]*>(.*?)</a>', re.IGNORECASE)
        matches = pattern.findall(page_content)
        full_url = ''
        for href, text in matches:
            if changed_keyword in href:
                encoded_href = quote(href, safe='')
                full_url = urljoin(url, f'/wiki/{encoded_href}')

                links_with_keywords.add(full_url)

    return list(links_with_keywords)


def open_wiki_link(wlinks, keyword):
        # links_with_keywords = find_wiki_hyperlinks_with_query(keyword[0], wlinks)
        # filtered_links = filter_link(links_with_keywords, keyword)
        # for link in filtered_links:
        #     find_wiki_data(link)
        find_wiki_data("https://en.wikipedia.org/wiki/Death_Proof")


open_wiki_link(['https://en.wikipedia.org/wiki/Quentin_Tarantino'], 'Death Proof')
