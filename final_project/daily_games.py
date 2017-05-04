import re
import numpy as np
import pandas as pd
import requests
from bs4 import BeautifulSoup
from datetime import datetime, date
from datetime import timedelta
from unidecode import unidecode
import psycopg2

year = datetime.today().year
teams = pd.read_csv('/home/w205/final_project/csvfiles/teams.csv')

BASE_URL = 'http://espn.go.com/nba/team/schedule/_/name/{0}/year/{1}/{2}'

match_id = []
dates = []
home_team = []
home_team_score = []
visit_team = []
visit_team_score = []
b=0
allcolumns = []
allrecords = []
for index, row in teams.iterrows():
    _team, url = row['team'], row['url']
    r = requests.get(BASE_URL.format(row['prefix_1'], year, row['prefix_2']))
    table = BeautifulSoup(r.text,'lxml').table
    try:
        for row in table.find_all('tr')[1:]: # Remove header

            columns = row.find_all('td')
            try:

                _home = True if columns[1].li.text == 'vs' else False

                _other_team = columns[1].find_all('a')[1].text

                _score = columns[2].a.text.split(' ')[0].split('-')
                _won = True if columns[2].span.text == 'W' else False
                a = re.sub('<td>','',str(columns[3]))
                b = re.sub('</td>','',a)
                _home_record = b if _home else None
                _away_record = b if not _home else 0
                match_id.append(columns[2].a['href'].split('id/')[1])
                home_team.append(_team if _home else _other_team)
                visit_team.append(_team if not _home else _other_team)
                d = datetime.strptime(columns[0].text, '%a, %b %d')
                if d.month > 8:
                    dates1 = date(year-1,d.month,d.day)
                    dates.append(date(year-1,d.month,d.day))
                else:
                    dates.append(date(year, d.month, d.day))
                    dates1 = date(year, d.month, d.day)
                a = re.sub('<td>','',str(columns[3]))
                b = re.sub('</td>','',a)
                c = b.split('-')
                _home_wins = c[0] if _home else 0
                _home_losses = c[1] if _home else 0
                _away_wins = c[0] if not _home else 0
                _away_losses = c[1] if not _home else 0
                allrecords.append([columns[2].a['href'].split('id/')[1],_home_wins,_home_losses,_away_wins,_away_losses])


                if _home:
                    if _won:
                        home_score=_score[0]
                        visit_score=_score[1]
                        home_team_score.append(_score[0])
                        visit_team_score.append(_score[1])
                    else:
                        home_score=_score[1]
                        visit_score=_score[0]
                        home_team_score.append(_score[1])
                        visit_team_score.append(_score[0])

                else:
                    if _won:
                        home_score=_score[1]
                        visit_score=_score[0]
                        home_team_score.append(_score[1])
                        visit_team_score.append(_score[0])
                    else:
                        home_score=_score[0]
                        visit_score=_score[1]
                        home_team_score.append(_score[0])
                        visit_team_score.append(_score[1])

                allcolumns.append([columns[2].a['href'].split('id/')[1],
                                   dates1,
                                   _team if _home else _other_team,
                                   _team if not _home else _other_team,
                                 home_score,visit_score])

            except Exception as e:

                pass # Not all columns row are a match, is OK
                    # print(e)
    except Exception as e:
        pass


dic = {'id': match_id, 'date': dates, 'home_team': home_team, 'visit_team': visit_team,'home_team_score': home_team_score, 'visit_team_score': visit_team_score}
headers = ['id','date','home_team','visit_team','home_team_score','visit_team_score']
games = pd.DataFrame(allcolumns,columns=headers)
recordheaders = ['id','home_wins','home_losses','away_wins','away_losses']
recordtable = pd.DataFrame(allrecords,columns=recordheaders)


#Create pivot table to correctly identify win and loss records of teams

#This table does not exist during the playoffs
'''temp = recordtable.fillna(0)

temp.home_wins = temp.home_wins.apply(int)
temp.home_losses = temp.home_losses.apply(int)
temp.away_wins = temp.away_wins.apply(int)
temp.away_losses = temp.away_losses.apply(int)
recdf = temp.pivot_table(index='id',aggfunc=np.sum)

recdf.to_csv('records'+str(year)+'.csv')
recdf.tail()'''


games1 = games.drop_duplicates(subset = 'id',keep = 'last').set_index('id')

newgames = games[games.date >date(2017,4,14)]

games1 = newgames.drop_duplicates(subset = 'id',keep = 'last').set_index('id')

games1.to_csv("/home/w205/final_project/csvfiles/temp_games.csv")

city_team = ['Atlanta', 'Boston', 'Brooklyn', 'Charlotte', 'Chicago',
       'Cleveland', 'Dallas', 'Denver', 'Detroit','Golden State', 'Houston', 'Indiana',
       'LA','Los Angeles', 'Memphis', 'Miami', 'Milwaukee', 'Minnesota','New Orleans','NY Knicks',
       'Oklahoma City','Orlando','Philadelphia', 'Phoenix', 'Portland', 'Sacramento','San Antonio', 'Toronto',
       'Utah','Washington','Atlanta Hawks', 'Boston Celtics', 'Brooklyn Nets',
       'Charlotte Hornets', 'Chicago Bulls', 'Cleveland Cavaliers',
       'Dallas Mavericks', 'Denver Nuggets', 'Detroit Pistons',
       'Golden State Warriors', 'Houston Rockets','Indiana Pacers',
       'LA Clippers', 'Los Angeles Lakers',
       'Memphis Grizzlies', 'Miami Heat', 'Milwaukee Bucks',
       'Minnesota Timberwolves',
       'New Orleans Pelicans', 'New York Knicks',
       'Oklahoma City Thunder', 'Orlando Magic', 'Philadelphia 76ers',
       'Phoenix Suns', 'Portland Trail Blazers', 'Sacramento Kings',
       'San Antonio Spurs', 'Toronto Raptors', 'Utah Jazz',
       'Washington Wizards']

final = ['ATL', 'BOS','BKN', 'CHA', 'CHI', 'CLE', 'DAL', 'DEN', 'DET',
       'GS', 'HOU', 'IND', 'LAC', 'LAL', 'MEM', 'MIA', 'MIL', 'MIN', 'NO',
       'NY', 'OKC', 'ORL', 'PHI', 'PHX', 'POR', 'SAC', 'SA', 'TOR', 'UTA',
       'WSH']*2

team_dict = {k:v for k,v in zip(city_team,final)}

games1['visit_team'] = games1.visit_team.map(team_dict)

games1['home_team'] = games1.home_team.map(team_dict)


for i,x in games1.iterrows():
    conn = psycopg2.connect(database="postgres", user="postgres", password="pass", host="localhost", port="5432")
    id1 = i
    date = x[0]
    home_team = x[1]
    visit_team = x[2]
    home_team_score = x[3]
    visit_team_score = x[4]




    #Using variables to update
    cur = conn.cursor()
    cur.execute("INSERT INTO games2017 (id,date,home_team,visit_team,home_team_score,visit_team_score) \
    VALUES (%s,%s,%s,%s,%s,%s);" %(id1,date,home_team,visit_team,home_team_score,visit_team_score))
conn.commit()



                                                


