import requests
from bs4 import BeautifulSoup
import sqlite3
import datetime
import time
import re
import sys
import os
import hashlib
import json
import random
from jinja2 import Environment, FileSystemLoader

# Configuration
SEASONS_URL = 'http://www.j-archive.com/listseasons.php'
BASE_URL = 'http://www.j-archive.com/'
DB_NAME = 'jarchive.db'
CACHE_DIR = 'cache'
DIST_DIR = 'dist'

def get_db_connection():
    conn = sqlite3.connect(DB_NAME)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = get_db_connection()
    c = conn.cursor()
    # Create table with columns matching the dictionary keys
    c.execute('''
        CREATE TABLE IF NOT EXISTS clues (
            uid TEXT PRIMARY KEY,
            episode TEXT,
            season TEXT,
            air_date REAL,
            category TEXT,
            answer TEXT,
            text TEXT,
            dollar_value TEXT,
            order_number TEXT,
            dj BOOLEAN,
            triple_stumper BOOLEAN,
            clue_row TEXT,
            contestant TEXT
        )
    ''')
    conn.commit()
    conn.close()

def save_clue(clue_data):
    conn = get_db_connection()
    c = conn.cursor()
    
    # Prepare the INSERT OR REPLACE statement
    columns = ', '.join(clue_data.keys())
    placeholders = ', '.join(['?'] * len(clue_data))
    sql = f'INSERT OR REPLACE INTO clues ({columns}) VALUES ({placeholders})'
    
    c.execute(sql, list(clue_data.values()))
    conn.commit()
    conn.close()

def get_soup(url):
    if not os.path.exists(CACHE_DIR):
        os.makedirs(CACHE_DIR)

    # Create a filename from the URL
    filename = hashlib.md5(url.encode('utf-8')).hexdigest() + ".html"
    filepath = os.path.join(CACHE_DIR, filename)

    if os.path.exists(filepath):
        print(f"Loading from cache: {url}")
        with open(filepath, 'r', encoding='utf-8') as f:
            return BeautifulSoup(f.read(), 'html.parser')

    # Randomized delay between 0.2 and 2 seconds
    delay = random.uniform(0.2, 2.0)
    print(f"Waiting {delay:.2f}s before fetching {url}...")
    time.sleep(delay)

    print(f"Fetching {url}...")
    try:
        resp = requests.get(url)
        resp.raise_for_status()
        # Decode content to string for saving
        content = resp.content.decode('utf-8', errors='replace')
        
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(content)
            
        return BeautifulSoup(content, 'html.parser')
    except Exception as e:
        print(f"Error fetching {url}: {e}")
        return None

def get_seasons_list():
    soup = get_soup(SEASONS_URL)
    if not soup: return []

    content = soup.find('div', {"id":"content"})
    if not content: return []
    
    seasons = []
    links = content.find_all('a')
    for link in links:
        href = link.get('href')
        if href and 'season=' in href:
            season_match = re.search(r'season=(\w+)', href)
            season_num = season_match.group(1) if season_match else "Unknown"
            seasons.append({
                'number': season_num,
                'url': BASE_URL + href
            })
    return seasons

def get_episodes_in_db(season_num):
    conn = get_db_connection()
    episodes = conn.execute('SELECT DISTINCT episode FROM clues WHERE season = ?', (season_num,)).fetchall()
    conn.close()
    return [row['episode'] for row in episodes]

def scrape_season(url, limit=None):
    # Extract season number from URL (e.g., season=30)
    season_match = re.search(r'season=(\w+)', url)
    season_num = season_match.group(1) if season_match else "Unknown"

    print(f"Checking Season {season_num}...")
    soup = get_soup(url)
    if not soup: return

    # Grab the div that contains the content and search for any links
    content = soup.find('div', {"id":"content"})
    if not content: return
    
    episodes = content.find_all('a', {"href": re.compile(r'showgame\.php')})
    
    # Get existing episodes to avoid re-scraping
    existing_episodes = get_episodes_in_db(season_num)
    
    count = 0
    new_episodes = 0
    for episode in episodes:
        if limit is not None and count >= limit:
            break
        text = episode.text.strip()
        ep_data = text.split(',')
        if len(ep_data) < 2:
            continue
            
        # ep_num extraction: "#6895" -> "6895"
        match_ep = re.search(r'#(\d+)', ep_data[0])
        ep_num = match_ep.group(1) if match_ep else ep_data[0].strip()

        if ep_num in existing_episodes:
            continue

        # air_date extraction
        try:
            date_str = ep_data[1].strip()
            match = re.search(r'(\d{4})-(\d{1,2})-(\d{1,2})', date_str)
            if match:
                year, month, day = map(int, match.groups())
                air_date = datetime.date(year, month, day)
                timestamp = time.mktime(air_date.timetuple())
                
                href = episode.get('href')
                if href:
                    scrape_episode(href, ep_num, season_num, timestamp)
                    count += 1
                    new_episodes += 1
            else:
                print(f"Could not parse date from {date_str}")
        except Exception as e:
            print(f"Error parsing episode data for {text}: {e}")
    
    if new_episodes == 0:
        print(f"Season {season_num} is already up to date.")
    else:
        print(f"Finished scraping {new_episodes} new episodes for Season {season_num}.")

def run_incremental_scrape():
    print("Checking for next season to scrape...")
    seasons = get_seasons_list()
    if not seasons:
        print("Could not fetch seasons list.")
        return

    conn = get_db_connection()
    existing_seasons = [row['season'] for row in conn.execute('SELECT DISTINCT season FROM clues').fetchall()]
    conn.close()
    
    # Sort seasons by number (numeric if possible)
    def season_key(s):
        try: return int(s['number'])
        except: return 0
    
    seasons.sort(key=season_key, reverse=True) # Newest first
    
    target_season = None
    
    # 1. Check for incomplete seasons
    for s in seasons:
        if s['number'] in existing_seasons:
            soup = get_soup(s['url'])
            if not soup: continue
            content = soup.find('div', {"id":"content"})
            if not content: continue
            expected_episodes = len(content.find_all('a', {"href": re.compile(r'showgame\.php')}))
            
            actual_episodes = len(get_episodes_in_db(s['number']))
            
            if actual_episodes < expected_episodes:
                print(f"Season {s['number']} is incomplete ({actual_episodes}/{expected_episodes}). Resuming...")
                target_season = s
                break
    
    # 2. If no incomplete seasons, find the next one we haven't started
    if not target_season:
        if not existing_seasons:
            target_season = seasons[0] # Start with the newest if empty
        else:
            for s in seasons:
                if s['number'] not in existing_seasons:
                    target_season = s
                    break
                    
    if target_season:
        print(f"Targeting Season {target_season['number']}...")
        scrape_season(target_season['url'])
        export_site()
    else:
        print("All seasons appear to be scraped and up to date!")

def scrape_episode(url, episode_num, season_num, air_date):
    if not url.startswith('http'):
        url = BASE_URL + url
        
    soup = get_soup(url)
    if not soup: return

    allCategories = soup.find_all('td', {"class" : "category_name"})
    cats = [] # List of categories without any html
    for cat in allCategories:
        cats.append(cat.get_text())

    allClues = soup.find_all(attrs={"class" : "clue"})
    for clue in allClues:
        clue_attribs = get_clue_attribs(clue, cats)
        if clue_attribs:
            clue_attribs['air_date'] = air_date
            clue_attribs['episode'] = episode_num
            clue_attribs['season'] = season_num

            # Create a unique ID
            clue_attribs['uid'] = f"{episode_num}_{clue_attribs['category']}_{clue_attribs['dollar_value']}_{clue_attribs['order_number']}"
            
            save_clue(clue_attribs)

def get_clue_attribs(clue, cats):
    # Simplified extraction based on current HTML structure
    try:
        # Extract Answer
        correct_response = clue.find('em', {"class" : "correct_response"})
        if correct_response:
            answer = correct_response.get_text()
        else:
            answer = "Unknown"
        
        # Extract Contestant (the one who got it right)
        contestant = "None"
        right_cell = clue.find('td', {"class": "right"})
        if right_cell:
            contestant = right_cell.get_text()
        
        # Check for Triple Stumper
        triple_stumper = False
        wrong_answers = clue.find_all('td', {"class": "wrong"})
        for wa in wrong_answers:
            if "Triple Stumper" in wa.get_text():
                triple_stumper = True
                contestant = "Triple Stumper"
                break
                        
        # Extract Clue ID and Category
        clue_unstuck = clue.find(attrs={"class" : "clue_unstuck"})
        if not clue_unstuck: return None
        
        clue_id_str = clue_unstuck.get('id')
        clue_id = clue_id_str.split("_")[1:4] 
        
        cat_idx = int(clue_id[1]) - 1
        if clue_id[0] == 'DJ':
            cat_idx += 6
        
        if cat_idx < len(cats):
            cat = cats[cat_idx]
        else:
            cat = "Unknown"
        
        dj = (clue_id[0] == "DJ")
        clue_row = clue_id[2]

        dollar_value_elem = clue.find(attrs={"class" : re.compile(r'clue_value')})
        dollar_value = dollar_value_elem.get_text() if dollar_value_elem else "0"
        
        clue_text_elem = clue.find(attrs={"class" : "clue_text"})
        clue_text = clue_text_elem.get_text() if clue_text_elem else ""
        
        clue_order_elem = clue.find(attrs={"class" : "clue_order_number"})
        clue_order_number = clue_order_elem.get_text() if clue_order_elem else "0"
        
        return {
            "answer" : answer, 
            "category" : cat, 
            "text" : clue_text, 
            "dollar_value": dollar_value, 
            "order_number" : clue_order_number, 
            "dj" : dj, 
            "triple_stumper" : triple_stumper, 
            "clue_row" : clue_row,
            "contestant": contestant
        }
    except Exception as e:
        print(f"Error parsing clue: {e}")
        return None

def export_site():
    print(f"Exporting site to {DIST_DIR}...")
    
    if not os.path.exists(DIST_DIR):
        os.makedirs(DIST_DIR)
    
    data_dir = os.path.join(DIST_DIR, 'data')
    if not os.path.exists(data_dir):
        os.makedirs(data_dir)
        
    conn = get_db_connection()
    
    # 1. Export Seasons Metadata
    seasons = conn.execute('SELECT DISTINCT season FROM clues ORDER BY season DESC').fetchall()
    seasons_list = [dict(s) for s in seasons]
    
    with open(os.path.join(data_dir, 'seasons.json'), 'w', encoding='utf-8') as f:
        json.dump(seasons_list, f)
        
    # 2. Export each season's data
    for season in seasons_list:
        s_num = season['season']
        print(f"  Exporting Season {s_num}...")
        
        # Get episodes for this season
        episodes = conn.execute('SELECT DISTINCT episode, air_date FROM clues WHERE season = ? ORDER BY air_date DESC', (s_num,)).fetchall()
        episodes_list = []
        for ep in episodes:
            e = dict(ep)
            e['formatted_date'] = datetime.datetime.fromtimestamp(e['air_date']).strftime('%Y-%m-%d') if e['air_date'] else 'N/A'
            episodes_list.append(e)
            
        # Get all clues for this season
        clues = conn.execute('SELECT * FROM clues WHERE season = ? ORDER BY air_date DESC, episode DESC, order_number ASC', (s_num,)).fetchall()
        clues_list = []
        for clue in clues:
            c = dict(clue)
            c['formatted_date'] = datetime.datetime.fromtimestamp(c['air_date']).strftime('%Y-%m-%d') if c['air_date'] else 'N/A'
            clues_list.append(c)
            
        season_data = {
            "episodes": episodes_list,
            "clues": clues_list
        }
        
        with open(os.path.join(data_dir, f'season_{s_num}.json'), 'w', encoding='utf-8') as f:
            json.dump(season_data, f)
            
    conn.close()
    
    # 3. Generate index.html from template
    file_loader = FileSystemLoader('templates')
    env = Environment(loader=file_loader)
    template = env.get_template('index.html')
    
    # We pass an empty list for clues/episodes because the JS will fetch them
    output = template.render(clues=[], episodes=[], is_static=False)
    
    with open(os.path.join(DIST_DIR, 'index.html'), 'w', encoding='utf-8') as f:
        f.write(output)
        
    conn = get_db_connection()
    total_clues = conn.execute('SELECT COUNT(*) FROM clues').fetchone()[0]
    conn.close()
    print(f"Export complete! Site is in the '{DIST_DIR}' directory.")
    print(f"Database Status: {total_clues} total clues stored in {DB_NAME}")

if __name__ == "__main__":
    init_db()
    
    if len(sys.argv) > 1 and sys.argv[1] == "--export":
        export_site()
    else:
        run_incremental_scrape()





