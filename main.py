import os
import requests
import re
from collections import deque
import pandas as pd
import time

def get_dataframe_size_megabytes(dataframe):
    # Calculate the size of the DataFrame in megabytes
    return dataframe.memory_usage(deep=True).sum() / (1024 * 1024)
def crawl(url):
    hdr = {
        'user-agent': 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Mobile Safari/537.36'
    }

    response = requests.get(url, headers=hdr)
    max_size_mb = 3500
    crawled_links = set()
    queue = deque([(url, 0)])

    # Check if the 'web_data.csv' file already exists
    if os.path.exists('web_data2.csv'):
        df = pd.read_csv('web_data2.csv')
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
                    alternative_links = find_alternative_links(html_content)

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

    df.to_csv('web_data2.csv', index=False)
    return df

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
            #print(absoluted)

    for link in persons_links:
        absoluted = "https://www.imdb.com" +  link
        if absoluted not in results:
            results.append(absoluted)
            #print(absoluted)

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



def movie_series_parser(url,html_content):
    if 'title' in url:
        titleReg = r'<title>(.*?)\(+(.*)- IMDb<\/title'
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
            print(url)
            if matches:
                matches.pop()

        new_row = {'url': url, 'type': "M/S", 'title': titleMatch.group(1), 'director': directors, 'cast': matches}
        if new_row['director'] == '{directorsOrCreatorsString}' or '{directorsOrCreatorsString}' in directors:
            new_row.update({'director': 'more'})
        movie_tv_df.loc[len(movie_tv_df)] = new_row
    # else:
    #     nameReg = r'<\/script><title>(.*?)(\s)*\-(\s)*(IMDb)<\/title>'
    #     # moviesReg = r'"originalTitleText":\{"text":"(.*?)\s*","__typename":"TitleText"'
    #     moviesReg = r'caption":\{"\s*plainText\s*":"\s*(.*)\s+\(\d+\)\s*","__typename"\s*:"Markdown'
    #     nameMatch = re.search(nameReg, html_content)
    #     moviesMatch = re.search(moviesReg, html_content)
    #     print(nameMatch.group(1),moviesMatch.group(1))
    #     new_row = {'url': url, 'type': "person", 'name': nameMatch.group(1), 'knownfor': moviesMatch.group(1)}
    #     person_df.loc[len(person_df)] = new_row


def parse(df):
    for index, row in df.iterrows():
        url_value = df['URL'].iloc[index]
        html_content = df['HTML_Content'].iloc[index]

        if url_value == "https://www.imdb.com/name/nm1102278/?ref_=tt_mv_desc":
            with open("actor.txt", "w", encoding="utf-8") as f:
                f.write(html_content)

        if url_value == "https://www.imdb.com/name/nm6073955?ref_=ttfaq_eds_right-5_lk":
            with open("actress.txt", "w", encoding="utf-8") as f:
                f.write(html_content)

        tmp = is_valid_url(url_value)
        if tmp == -1:
            continue
        elif tmp == 1:
            movie_series_parser(url_value,html_content)
        elif tmp == 0:
            movie_series_parser(url_value,html_content)



url = 'https://www.imdb.com/title/tt1190634/?ref_=nv_sr_srsg_0_tt_8_nm_0_q_the%2520boy'
#webpages = crawl(url)

movie_tv_df = pd.DataFrame(columns=['url', 'type', 'title', 'director', 'cast'])
#person_df = pd.DataFrame(columns=['url', 'type', 'name', 'knownfor'])

df = pd.read_csv("web_data2.csv")
parse(df)
movie_tv_df = movie_tv_df.drop_duplicates(subset=['title', 'director'])
movie_tv_df.to_csv('extraction_movies.csv', index=False)

#person_df = person_df.drop_duplicates(subset=['name', 'knownfor'])
#person_df.to_csv('extraction_person.csv', index=False)


