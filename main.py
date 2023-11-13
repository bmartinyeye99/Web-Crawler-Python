import os
import requests
import re
from collections import deque
import pandas as pd
import time

def get_dataframe_size_megabytes(dataframe):
    return dataframe.memory_usage(deep=True).sum() / (1024 * 1024)

def crawl(url):
    hdr = {
        'user-agent': 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Mobile Safari/537.36',
        'Accept-Language': 'en-US'
    }
    response = requests.get(url, headers=hdr)
    max_size_mb = 200
    crawled_links = set()
    queue = deque([(url, 0)])
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
                    alternative_links = find_alternative_links(html_content)
                    for link in links:
                        if link not in crawled_links:
                            queue.append((link, depth + 1))
                    for link in alternative_links:
                        if link not in crawled_links:
                            queue.append((link, depth + 1))
                    if current_url not in responses_df['URL'].values:
                        responses_df.loc[len(responses_df.index)] = [current_url, html_content]
                    df_size_mb = get_dataframe_size_megabytes(responses_df)
                    print(df_size_mb)
                    if df_size_mb >= max_size_mb:
                        print(f"DataFrame size exceeded {max_size_mb} MB. Stopping crawl.")
                        break
                else:
                    continue
            except requests.exceptions.RequestException as e:
                print(f"Request Exception for {e}")

    # if os.path.exists('web_data2.csv'):
    #     try:
    #         responses_df.to_csv('web_data2.csv', mode='a', index=False, header=False)
    #     except Exception as e:
    #         print(f"Error writing to CSV: {e}")
    # else:
    #     responses_df.to_csv('web_data2.csv', index=False)

def find_alternative_links(html):
    movie_tv_pattern = r'/title/[^"\']*'
    person_pattern = r'/name/[^"\']*'
    movie_tv_links = re.findall(movie_tv_pattern,html)
    persons_links = re.findall(person_pattern,html)
    results = []

    for link in movie_tv_links:
        absoluted = "https://www.imdb.com" +  link
        if absoluted not in results:
            results.append(absoluted)

    for link in persons_links:
        absoluted = "https://www.imdb.com" +  link
        if absoluted not in results:
            results.append(absoluted)

    return results

def is_valid_url(url):
    restrictedWords = ['language=','releaseinfo','ratings', 'fullcredits',
                        'mediaviewer', 'characters', 'companycredits', "news", 'mediaindex', 'officialsites',
                       'locations','taglines','plotsummary','synopsis',"technical",
                       'keywords', 'parentalguide','trivia','goofs','crazycredits', 'quotes','alternateversions',
                       'movieconnections','soundtracks','soundtrack','videogallery','awards','faq','reviews','externalsites',
                       'episodes']

    movie_tv_pattern = r'https://www\.imdb\.com/title/[^"\']*'
    person_pattern = r'https://www.imdb.com/name/[^"\']*'
    if re.match(movie_tv_pattern, url) and not any(word in url for word in restrictedWords):
        return 1

    elif re.match(person_pattern, url):
        return 0

    else:
        return -1


def create_new_row(url,titleMatch,directors,matches):
    if directors:
        directors = directors.strip()
    new_row = {'url': url.strip(), 'title': titleMatch.group(1).strip(), 'director': directors, 'cast': matches}
    if new_row['director'] == '{directorsOrCreatorsString}' or '{directorsOrCreatorsString}' in directors:
        new_row.update({'director': 'more'})
    if new_row['url'] not in movie_tv_df['url'].values:
        if new_row['director'] not in movie_tv_df['director'].values and new_row['title'] not in movie_tv_df['title']:
            movie_tv_df.loc[len(movie_tv_df)] = new_row

def movie_series_parser(url,html_content):
    if 'title' in url:
        titleReg = r'<title>(.*?)\((.*?) - IMDb<\/title'
        directorReg = r'Directed by (.*?)(?=\.\sWith)'
        creatorReg = r'Created by (.*?)(?=\.\sWith)'
        titleMatch = re.search(titleReg, html_content)
        directorMatch = None
        directors = ''
        if "(TV Series " in html_content:
            directorMatch = re.search(creatorReg, html_content)
        else: directorMatch = re.search(directorReg,html_content)

        if directorMatch:
            directors = directorMatch.group(1)
        else: directors = 'more'

        castPattern = r'"cast":\{"edges":(.*$)'
        afterCast = re.search(castPattern, html_content, re.MULTILINE)
        matches = []
        if afterCast:
            castData = afterCast.group(1)
            pattern = r'"nameText":\{"text":"([^"]+)","__typename":"NameText"'
            matches = re.findall(pattern, castData)
            if matches:
                matches.pop()

        create_new_row(url,titleMatch,directors,matches)

        # new_row = {'url': url.strip(),  'title': titleMatch.group(1).strip(), 'director': directors.strip(), 'cast': matches}
        # if new_row['director'] == '{directorsOrCreatorsString}' or '{directorsOrCreatorsString}' in directors:
        #     new_row.update({'director': 'more'})
        # if new_row['url'] not in movie_tv_df['url'].values:
        #     if new_row['director'] not in movie_tv_df['director'].values and new_row['title'] not in movie_tv_df['title']:
        #         movie_tv_df.loc[len(movie_tv_df)] = new_row


def parse(df):
    for index, row in df.iterrows():
        url_value = df['URL'].iloc[index]
        html_content = df['HTML_Content'].iloc[index]
        if url_value  in movie_tv_df['url'].values:
            continue
        tmp = is_valid_url(url_value)
        if tmp == -1:
            continue
        elif tmp == 1:
            movie_series_parser(url_value,html_content)
        elif tmp == 0:
            movie_series_parser(url_value,html_content)

url = 'https://www.imdb.com/title/tt7939766/?ref_=hm_tpks_tt_i_26_pd_tp1_pbr'

# if os.path.exists('extraction_movies.csv'):
#     movie_tv_df = pd.read_csv("extraction_movies.csv",encoding='utf-8')
# else:
#     movie_tv_df = pd.DataFrame(columns=['url', 'title', 'director', 'cast'])


# if os.path.exists('web_data2.csv'):
#     responses_df = pd.read_csv('web_data2.csv')

responses_df = pd.DataFrame(columns=['URL', 'HTML_Content'])
movie_tv_df = pd.DataFrame(columns=['url', 'title', 'director', 'cast'])

crawl(url)

parse(responses_df)

df_no_duplicates_subset = movie_tv_df.drop_duplicates(subset=['url', 'title'])

if os.path.exists('extraction_movies.csv'):
    try:
        df_no_duplicates_subset.to_csv('extraction_movies.csv', mode='a', index=False,header=False)
    except Exception as e:
        print(f"Error writing to CSV: {e}")
else:
    df_no_duplicates_subset.to_csv('extraction_movies.csv', index=False)


