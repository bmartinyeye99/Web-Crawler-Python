import requests
import re
from collections import deque
import pandas as pd


def is_valid_url(url):
    return url.startswith('http://') or url.startswith('https://')


def crawl(url, max_depth):
    hdr = {
        'user-agent': 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Mobile Safari/537.36'
    }

    crawled_links = set()
    all_links = []
    queue = deque([(url, 0)])

    df = pd.DataFrame(columns=['URL', 'HTML_Content'])

    while queue:
        current_url, depth = queue.popleft()

        if not is_valid_url(current_url) or current_url.startswith(
                'https://apps.apple.com') or current_url in crawled_links:
            continue

        if max_depth == 0:
            return df,all_links

        try:
            response = requests.get(current_url, headers=hdr)

            if response.status_code == 200:
                html_content = response.text
                link_pattern = r'<a\s+[^>]*?href=["\'](https?://[^"\']*)["\'][^>]*>'
                links = re.findall(link_pattern, html_content)

                crawled_links.add(current_url)
                max_depth = max_depth - 1
                for link in links:
                    if link not in crawled_links:
                        all_links.append(link)
                        queue.append((link, depth + 1))

                # Store the HTML content and URL in the DataFrame
                df.loc[len(df.index)] = [current_url, html_content]

            else:
                print(f"Failed to retrieve the web page: {current_url}")

        except requests.exceptions.RequestException as e:
            print(f"Request Exception for {e}")

    # Save the DataFrame to a CSV file
    df.to_csv('web_data.csv', index=False)

    return all_links


url = 'https://www.imdb.com'
max_depth = 10
webpages = pd.DataFrame()
webpages,result_links = crawl(url, max_depth)
print(webpages)
