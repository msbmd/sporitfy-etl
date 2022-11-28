
import json 
import requests 
import pandas as pd
import sqlite3
from sqlalchemy import create_engine
import sqlite3
import datetime
import logging


url = 'https://api.spotify.com/v1/me/player/currently-playing'
access_token =''

#Logging setting
logging.basicConfig(filename='etljob.log',level=logging.DEBUG, format='%(asctime)s  %(name)s  %(levelname)s: %(message)s')

#Extract Data
def get_current_tracks():
    response = requests.get(
        url,
        headers={
            "Authorization": f"Bearer {access_token}"
        }
    )
    
    if not response:
        raise Exception("Token Expired, authentation fails!")
        
    json_resp = response.json()
    return json_resp
    

#Transformed Data
def transformed_data():
    json_resp = get_current_track()
    timestamp = datetime.datetime.fromtimestamp(int(json_resp['timestamp']/1000)).strftime('%Y-%m-%d %H:%M:%S')
    track_name = json_resp['item']['name']
    track_id    = json_resp['item']['id']
    track_artist = json_resp['item']['artists'][0]['name']
    track_duration    = round((json_resp['item']['duration_ms']/60000),2)
    
    
    my_dict = {
        "Timestamp": timestamp,
        "Artist" : track_artist,
        "Track_name": track_name,
        "Track_id":  track_id,
        "Track_duration": track_duration
        
    }
    
        
    song_df =  pd.DataFrame([my_dict], columns=my_dict.keys(), index=None)
    
    #Data validation
      
    if song_df.empty:
        raise Exception("Empty data, execution is finished")
    if song_df.isnull().values.any():
            raise Exception("Null values found!")
    
    return song_df

#Load Data
def loading_data():
    song_df = transformed_data()
    con = sqlite3.connect('example.db')
    print("Connected to DB successfully")
    cur = con.cursor()
    
        
    cur.execute('''CREATE TABLE IF NOT EXISTS sportify_play_music
    (timestamp CHAR(200) PRIMARY KEY NOT NULL,
        artist CHAR(200) NOT NULL,
        track_name CHAR(200) NOT NULL,
        track_id CHAR(200) NOT NULL,
        track_duration);''')
    print('Table created')

    
    song_df.to_sql('sportify_play_music' , con , index=False, if_exists='append')
    
    
    print("Loading done successfully")
    con.commit()
    con.close()
    print("Connection Closed successfully")
   


def main():
    get_current_tracks()
    transformed_data()
    loading_data()

if __name__ == "__main__":
    main()