from requests import post, get
from bs4 import BeautifulSoup
import psycopg2
from dotenv import load_dotenv
import os

load_dotenv()

database_config = {
        "database": os.environ.get('DATABASE'),
        "user": os.environ.get('USER'),
        "password": os.environ.get('SQL_PASSWORD'),
        "host": os.environ.get('HOST'),
        "port": "5432"
    }
conn = psycopg2.connect(**database_config)

def get_artists():
    '''Output is list of artists with a song in top 100 UK official charts'''
    
    url = 'https://www.officialcharts.com/charts/singles-chart/'
    response = get(url)
    soup = BeautifulSoup(response.text, 'html.parser')
    artists_list = [x.span.text for x in soup.findAll('a', class_="chart-artist text-lg inline-block")]
    artists_set = {}
    
    def flatten(xss):
        return [x for xs in xss for x in xs]
    
    artists_s0 = [x.replace("'","") for x in artists_list]
    artists_s1 = [x.split('/') for x in artists_s0] #List of Lists
    artists_s1_flat = flatten(artists_s1)
    artists_s2 = [x.split(' FT ') for x in artists_s1_flat] #List of Lists
    artists_s2_flat = flatten(artists_s2)
    artists_s3 = [x.split(' & ')+[x] for x in artists_s2_flat] #List of Lists
    artists_s3_flat = flatten(artists_s3)
    artists_set = set(artists_s3_flat)

    week = str(soup.find('p',class_="text-brand-cobalt text-sm sm:text-lg lg:text-xl basis-full font-bold !my-4").text)
    artists_with_week = [[artist,week] for artist in artists_set]
    return artists_with_week

def update_table(artist, week):
    try:
        with psycopg2.connect(**database_config) as conn:
            with conn.cursor() as cur:
                cur.execute(f"""
                    insert into student.mm_artists
                    Values('{artist}', '{week}')
                """)

                conn.commit()  # Commit the changes

    except (Exception, psycopg2.DatabaseError) as error:
        print("Error:", error)

def get_stored_artists():
    try:
        with psycopg2.connect(**database_config) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT *
                    FROM student.mm_artists
                """)

                result = cur.fetchall()
                return result
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error:", error)
        return None
new_artists = get_artists()

stored_artists = get_stored_artists()

for artists_with_week in new_artists:
  if tuple(artists_with_week) not in stored_artists:
    update_table(artists_with_week[0], artists_with_week[1])
