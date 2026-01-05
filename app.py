from flask import Flask, render_template
import sqlite3
import datetime

app = Flask(__name__)
DB_NAME = 'jarchive.db'

def get_db_connection():
    conn = sqlite3.connect(DB_NAME)
    conn.row_factory = sqlite3.Row
    return conn

@app.route('/')
def index():
    conn = get_db_connection()
    clues = conn.execute('SELECT * FROM clues ORDER BY air_date DESC, episode DESC, order_number ASC').fetchall()
    conn.close()
    
    # Convert row objects to dicts and format date
    clues_list = []
    for clue in clues:
        c = dict(clue)
        if c['air_date']:
            c['formatted_date'] = datetime.datetime.fromtimestamp(c['air_date']).strftime('%Y-%m-%d')
        else:
            c['formatted_date'] = 'N/A'
        clues_list.append(c)

    return render_template('index.html', clues=clues_list)

if __name__ == '__main__':
    app.run(debug=True)
