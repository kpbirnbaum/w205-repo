import sys
import re
from unidecode import unidecode
import numpy as np
import pandas as pd
import psycopg2
import requests
from bs4 import BeautifulSoup
from datetime import datetime, date
from datetime import timedelta
year=int(sys.argv[1])

#Removed path
games1=pd.read_csv('/home/w205/final_project/csvfiles/games'+str(year)+'.csv',index_col='id')


BASE_URL = 'http://espn.go.com/nba/boxscore?gameId={0}'
print(games1.index[0])
request = requests.get(BASE_URL.format(games1.index[0]))

table = BeautifulSoup(request.text,'lxml').find('table', class_='mod-data')
heads = table.find_all("thead")

headers = heads[0].find_all('tr')[0].find_all('th')[1:]
headers = [th.text for th in headers]
columns = ['id', 'team', 'player'] + headers


players = pd.DataFrame(columns=columns)

def get_players(players, team_name):
    array = np.zeros((len(players), len(headers)+1), dtype=object)
    array[:] = np.nan
    for i, player in enumerate(players):
        cols = player.find_all('td')
        array[i, 0] = cols[0].text.split(',')[0]
        for j in range(1, len(headers) + 1):
            if not cols[1].text.startswith('DNP'):
                array[i, j] = cols[j].text

    frame = pd.DataFrame(columns=columns)
    for x in array:
        line = np.concatenate(([index, team_name], x)).reshape(1,len(columns))
        new = pd.DataFrame(line, columns=frame.columns)
        frame = frame.append(new)
    return frame

for index, row in games1.iterrows():
    try:

        request = requests.get(BASE_URL.format(index))

        table = BeautifulSoup(request.text,'lxml').find_all('table', class_='mod-data')
        heads1 = table[0].find_all('thead')
        bodies1 = table[0].find_all('tbody')
        heads2 = table[1].find_all('thead')
        bodies2 = table[1].find_all('tbody')


        #team_1 = heads[0].th.text #trouble with getting team names
        team_1 = games1['visit_team'][index]

        team_1_players = bodies1[0].find_all('tr') + bodies1[1].find_all('tr')

        team_1_players = get_players(team_1_players, team_1)
        players = players.append(team_1_players)


        #team_2 = heads[3].th.text #trouble with getting team names
        team_2 = games1['home_team'][index]
        #team_2_players = bodies[3].find_all('tr') + bodies[4].find_all('tr')
        team_2_players = bodies2[0].find_all('tr') + bodies2[1].find_all('tr')
        team_2_players = get_players(team_2_players, team_2)
        players = players.append(team_2_players)
    except Exception as e:
        pass

players = players.set_index('id')
players['position'] = players.player.apply(lambda x: x[-2:]  if x[-1] != 'C' else x[-1])
players['player'] = players.player.apply(lambda x: x[:-2] if x[-1] != 'C' else x[:-1])

#Trying something else

def conversion(x):
    if type(x)!=float:
        return unidecode(x)
    else:
        return x

players=players.applymap(conversion)

#Remove some useless nulls
players1 = players[pd.notnull(players.MIN)]

players1.to_csv('/home/w205/final_project/csvfiles/boxscores'+str(year)+'.csv')

if year != 2017:
    conn = psycopg2.connect(database="postgres", user="postgres", password="pass", host="localhost", port="5432")

    cur = conn.cursor()

    cur.execute('DROP TABLE IF EXISTS boxscores%s' %year)

    cur.execute('CREATE TABLE boxscores%s (id varchar(50), team varchar(50),player varchar(50),MIN varchar(50),FG varchar(50),threePT varchar(50),FT varchar(50),OREB varchar(50),DREB varchar(50),REB varchar(50),AST varchar(50),STL varchar(50),BLK varchar(50),turnovers varchar(50),PF varchar(50),plusminus varchar(50),PTS varchar(50),position varchar(50))' %year)


    cur.execute("COPY boxscores%s FROM '/home/w205/final_project/csvfiles/boxscores%s.csv' DELIMITER ',' CSV HEADER;" %(year,year))

    conn.commit()
