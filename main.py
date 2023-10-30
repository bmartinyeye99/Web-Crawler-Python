import os
import requests
import re
from collections import deque
import pandas as pd
import time
from bs4 import BeautifulSoup


def is_movie_link(html):
    movie_tv_pattern = r'/title/[^"\']*'
    person_pattern = r'/name/[^"\']*'
    movie_tv_links = re.findall(movie_tv_pattern,html)
    persons_links = re.findall(person_pattern,html)
    results = []

    for link in movie_tv_links:
        absoluted = "https://www.imdb.com" +  link
        if absoluted not in results:
            results.append(absoluted)
            #print(absoluted)

    for link in persons_links:
        absoluted = "https://www.imdb.com" +  link
        if absoluted not in results:
            results.append(absoluted)
            #print(absoluted)

    return results
def is_valid_url(url):
    return url.startswith('http://') or url.startswith('https://')

def get_dataframe_size_megabytes(dataframe):
    # Calculate the size of the DataFrame in megabytes
    return dataframe.memory_usage(deep=True).sum() / (1024 * 1024)

def parse(df):
    df['HTML_Content'].to_csv('html_content.txt', index=False, header=False, sep='\t')

    with open('html_content.txt', 'r', encoding='utf-8') as file:
        text = file.read()
        titleReg = r'<title>(.*? IMDb)</title>'
        directorReg = r'Directed by (.*?)(\.)'
        titleMatch = re.findall(titleReg, text)
        directorMatch = re.search(directorReg,text)
        print("Title : ", titleMatch)
        print("Director : ",directorMatch.group(1))
        print("Cast : ",)
        castPattern = r'""cast"":{""edges"":(.*$)'

        afterCast = re.findall(castPattern, text, re.MULTILINE)
        afterCast = str(afterCast)

        pattern = r'"nameText"":{""text"":""([^"]+)"",""__typename"":""NameText""'
        matches = re.findall(pattern, afterCast)

        for match in matches:
            print("\t",match)



def crawl(url):
    hdr = {
        'user-agent': 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Mobile Safari/537.36'
    }

    response = requests.get(url, headers=hdr)
    max_size_mb = 1000
    crawled_links = set()
    queue = deque([(url, 0)])

    # Check if the 'web_data.csv' file already exists
    if os.path.exists('web_data.csv'):
        df = pd.read_csv('web_data.csv')
    else:
        df = pd.DataFrame(columns=['URL', 'HTML_Content'])

    while queue:
        current_url, depth = queue.popleft()

        if current_url.startswith('https://apps.apple.com'):
            continue

        if current_url in crawled_links:
            continue

        else:
            try:
                response = requests.get(current_url, headers=hdr)
                time.sleep(0.2)

                if response.status_code == 200:
                    html_content = response.text
                    link_pattern = r'<a\s+[^>]*?href=["\'](https?://(?:www\.)?imdb\.com/[^"\']*)["\'][^>]*>'
                    links = re.findall(link_pattern, html_content)
                    crawled_links.add(current_url)
                    alternative_links = is_movie_link(html_content)

                    for link in links:
                        if link not in crawled_links:
                            queue.append((link, depth + 1))

                    for link in alternative_links:
                        if link not in crawled_links:
                            queue.append((link, depth + 1))

                    if current_url not in df['URL'].values:
                        df.loc[len(df.index)] = [current_url, html_content]
                    df_size_mb = get_dataframe_size_megabytes(df)
                    print(df_size_mb)
                    if df_size_mb >= max_size_mb:
                        print(f"DataFrame size exceeded {max_size_mb} MB. Stopping crawl.")
                        break

                else:
                    continue

            except requests.exceptions.RequestException as e:
                print(f"Request Exception for {e}")

    df.to_csv('web_data.csv', index=False)
    return df

url = 'https://www.imdb.com/title/tt28335725/?ref_=hm_inth_tt_t_2'
#webpages = crawl(url)
df = pd.read_csv("row_to_save.csv")
row_to_save = df.iloc[0:1]
row_to_save = row_to_save.astype({'HTML_Content':'string'})
parse(row_to_save)

