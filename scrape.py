import requests
from lxml import etree
from functools import wraps
import os
import urlparse
import time
import psycopg2


def connectDB(wrapped):
    @wraps(wrapped)
    def inner(*args, **kwargs):
        urlparse.uses_netloc.append("postgres")
        url = urlparse.urlparse(os.environ["DATABASE_URL"])
        conn = psycopg2.connect(
            database=url.path[1:],
            user=url.username,
            password=url.password,
            host=url.hostname,
            port=url.port
        )
        cur = conn.cursor()
        ret = wrapped(conn, cur, *args, **kwargs)
        conn.commit()
        cur.close()
        conn.close()
        return ret
    return inner


@connectDB
def scrape(*args):
    conn, cur = args[:2]
    genre_page = etree.HTML(requests.get('http://www.imdb.com/genre').content)
    genres = [x.lower() for x in genre_page.xpath('//table[@class="splash"]//a/text()')]
    url = 'http://www.imdb.com/search/title?at=0&genres=<g>&sort=user_rating&start=<s>&title_type=feature'
    for genre in genres[14:]:
        url2 = url.replace('<g>', genre)
        for start in xrange(1, 451, 50):
            page_url = url2.replace('<s>', str(start))
            page = etree.HTML(requests.get(page_url).content)
            movies = page.xpath('//td[@class="title"]/a/text()')
            for i, movie in enumerate(movies):
                try:
                    print genre, str(start+i), movie 
                    cur.execute('INSERT INTO MOVIES (TITLE) VALUES (%s)', (movie,))
                except Exception:
                    pass
            conn.commit()
            time.sleep(0.5)
        time.sleep(1.5)


scrape()
