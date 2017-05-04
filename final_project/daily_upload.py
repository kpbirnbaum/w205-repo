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

#games1.to_csv('games'+str(year)+'.csv')

newgames = games[games.date >date(2017,4,14)]

games1 = newgames.drop_duplicates(subset = 'id',keep = 'last').set_index('id')





BASE_URL = 'http://espn.go.com/nba/boxscore?gameId={0}'

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

#Remove nulls as well as blanks and Team values
players1 = players[pd.notnull(players.MIN)]
#players1 = players1[(players1.player!='\xa0') & (players1.player != 'TEAM')]
#players1.to_csv('boxscores' + str(year)+ '.csv')

#Trying something else

def conversion(x):
    if type(x)!=float:
        return unidecode(x)
    else:
        return x

players1=players1.applymap(conversion)


games1.to_csv("/home/w205/final_project/csvfiles/playoffs_game_temp_file.csv")
players1.to_csv("/home/w205/final_project/csvfiles/playoffs_boxscores_temp_file.csv")

conn = psycopg2.connect(database="postgres", user="postgres", password="pass", host="localhost", port="5432")
for i,x in players1.iterrows():
    #conn = psycopg2.connect(database="postgres", user="postgres", password="pass", host="localhost", port="5432")
    id1 = i
    team = x[0]
    player = x[1]
    minutes = x[2]
    fg = x[3]
    threept = x[4]
    ft = x[5]
    oreb = x[6]
    dreb = x[7]
    reb = x[8]
    ast = x[9]
    stl = x[10]
    blk = x[11]
    to_field = x[12]
    pf = x[13]
    plusminus = x[14]
    pts = x[15]
    position = x[16]
    
        

    #Using variables to update
    cur = conn.cursor()
    cur.execute("INSERT INTO boxscores2017 (id,team,player,min,fg,threept,ft,oreb,dreb,reb,ast,stl,blk,to_field,pf,plusminus,pts,position) \
          VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", (str(id1),str(team),str(player),str(minutes),str(fg),str(threept),
                                                                        str(ft),str(oreb),str(dreb),str(reb),str(ast),str(stl),str(blk),str(to_field),
                                                                        str(pf),str(plusminus),str(pts),str(position)));
conn.commit()
