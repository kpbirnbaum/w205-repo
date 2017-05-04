import re
import sys
import psycopg2
from unidecode import unidecode
import numpy as np
import pandas as pd
import requests
from bs4 import BeautifulSoup
from datetime import datetime, date
from datetime import timedelta
year=int(sys.argv[1])

checkpoint = pd.read_csv("/home/w205/final_project/csvfiles/boxscores%s.csv" %year)
checkpointx = pd.read_csv("/home/w205/final_project/csvfiles/Player_list_%s.csv"%year)
#Find players with invalid positions
fix_names = np.unique(checkpoint.player[checkpoint.position.isin(np.unique(checkpoint.position[~checkpoint.position.isin(['PG',
                                                                                                  'PF','SF','SG','C','AM',' '])&(pd.isnull(checkpoint.position)==False)]))])
fix_positions = (np.unique(checkpoint.position[~checkpoint.position.isin(['PG','PF','SF','SG','C','AM',' '])&(pd.isnull(checkpoint.position)==False)]))
#Manually replacing names for now

if year==2017:
    checkpoint['player'] = checkpoint.player.replace(to_replace=list(fix_names), value = ['B. JohnsonB. Johnson', 'J. UthoffJ. Uthoff', 'K. WiltjerK. Wiltjer','N. LaprovittolaN. Laprovittola', 'R. WilliamsR. Williams','W. SeldenW. Selden'])

    checkpoint['position'] = checkpoint.position.replace(to_replace= list(fix_positions), value = ['G','F','F','G','F','F'])


#Manually fix some names that are difficult to replace, but found mismatches from finding full names
    list1 = ['C.J. WatsonC.J. Watson', 'D. House Jr.D. House Jr.','D.J. AugustinD.J. Augustin', 'J. McAdooJ. McAdoo','J.J. BareaJ.J. Barea', 'L. Mbah a MouteL. Mbah a Moute','NeneNene ', 'O. Porter Jr.O. Porter Jr.', 'R.J. HunterR.J. Hunter','S. ZimmermanS. Zimmerman','T.J. McConnellT.J. McConnell', 'W. HernangomezW. Hernangomez','W. SeldenW. Selden']

    list2 = ['C. WatsonC. Watson','D. HouseD. House','D. AugustinD. Augustin','J.  McAdooJ.  McAdoo',
        'J. BareaJ. Barea','L.  Mbah a MouteL.  Mbah a Moute','N. HilarioN. Hilario','O. PorterO. Porter',
        'R. HunterR. Hunter','S. Zimmerman Jr.S. Zimmerman Jr.','T. McConnellT. McConnell','G. HernangomezG. Hernangomez',
        'W. Selden Jr.W. Selden Jr.']

    checkpoint['player'] = checkpoint.player.replace(to_replace=list1, value = list2)


else:
    print("Years before 2017, still need some minor cleanup")
players2 = checkpoint.merge(checkpointx[['First_name','Last_name','Full_name','player','team']],on=['player','team'], how = 'left')
#players2.index = checkpoint.index

#Check to see if any players play on the same team and have the same player index

duplications = checkpointx[checkpointx[['player','team']].duplicated(keep=False)]
print(duplications[['team','player','position']])

#Found them and for now manually change the names to add on position to uniquely identify
def same_team(row):
    if row['player']=="D. WilliamsD. Williams" and row.position == 'PG':
        return "D. WilliamsD. WilliamsPG"
    else:
        return row.player
    
checkpoint['player'] = checkpoint.apply(same_team,axis=1)
checkpointx['player'] = checkpointx.apply(same_team,axis=1)

#This is where we find out traded players
players2['player'] = players2.player.apply(lambda x: '' if pd.isnull(x) else x)
tradedplayers = np.unique(players2.player[pd.isnull(players2.Full_name) & (~players2.player.isin(['','TE']))])
traded = checkpointx[checkpointx.player.isin(tradedplayers)]

#For duplicate names we will find manually what they're teams were and append them to x for now

replace_teams = traded[traded.player.duplicated(keep=False)]

teams1 = []
for i in replace_teams.team:
    teams1.append(i)

replacement_teams = replace_teams.replace(to_replace=teams1,value = ['FA','DAL','PHI','CHI','NO','MIN','NY','POR','MIL'])

z = replace_teams.append(replacement_teams)
w = z[z.duplicated(keep=False)==False]
w = w[w.Full_name.duplicated(keep='first')]

checkpointx = checkpointx.append(w)

#Remerge the data with the new values for traded players with duplicate values
players2 = checkpoint.merge(checkpointx[['First_name','Last_name','Full_name','player','team']],on=['player','team'], how = 'left')
players2.index = checkpoint.index

#Here we take all players that were traded and merge only using names rather than teams and names
THE_NA = players2[players2.isnull().any(axis=1)]
THE_NA = THE_NA[['team', 'player', 'MIN', 'FG', '3PT', 'FT', 'OREB', 'DREB', 'REB',
                 'AST', 'STL', 'BLK', 'TO', 'PF', '+/-', 'PTS', 'position']]

#Notice on is only player. We then drop the na values and reappend with new players
THE_NA = THE_NA.merge(checkpointx[['First_name','Last_name','Full_name','player']],on=['player'],how='left')
players2.dropna(inplace=True)
players2 = players2.append(THE_NA)

players2 = players2[pd.isnull(players2.Full_name)==False]

def fgsplitmade(x):
    if '%' not in x:
        return x.split("-")[0]
    else:
        return 0
def fgsplittotal(x):
    if '%' not in x:
        return x.split("-")[1]
    else:
        return 0

players2['FG_made'] = players2.FG.apply(fgsplitmade)
players2['FG_taken'] = players2.FG.apply(fgsplittotal)
players2['FT_made'] = players2.FT.apply(fgsplitmade)
players2['FT_taken'] = players2.FT.apply(fgsplittotal)
players2['3PT_made'] = players2['3PT'].apply(fgsplitmade)
players2['3PT_taken'] = players2['3PT'].apply(fgsplittotal)


players2.drop(['FG','FT','3PT'],axis=1,inplace=True)

def to_int(x):
    if pd.isnull(x) or x in ['', '--', '0']:
        return 0
    else:
        return int(x)

    
numerical = ['+/-', 'AST', 'BLK', 'DREB','MIN', 'OREB', 'PF', 'PTS', 'REB', 'STL', 'TO', 
       'FG_made', 'FG_taken', 'FT_made', 'FT_taken',
       '3PT_made', '3PT_taken']
for i in numerical:
    players2[i]=players2[i].apply(to_int)


players2['Full_name'] = players2.Full_name.apply(lambda x: '' if pd.isnull(x) else x)

players2['fpoints'] = players2.PTS + players2.AST*1.5 - players2.TO + players2.REB*1.2 + players2.STL*2 + players2.BLK*2

def conversion(x):
    if type(x)!=float and type(x)!=int:
        return unidecode(x)
    else:
        return x

players2=players2.applymap(conversion)

players2 = players2[['id','First_name','Last_name','Full_name','position','team','player','MIN','PTS','FG_made', 'FG_taken','FT_made', 'FT_taken','3PT_made', '3PT_taken','OREB','DREB','REB','AST','STL','BLK','TO','PF','+/-','fpoints']]




players2.to_csv('/home/w205/final_project/csvfiles/playerdata'+str(year)+'.csv',index=False)

conn = psycopg2.connect(database="postgres", user="postgres", password="pass", host="localhost", port="5432")

cur = conn.cursor()

cur.execute('DROP TABLE IF EXISTS boxscores%s' %year)

cur.execute('CREATE TABLE boxscores%s (id varchar(50),First_name varchar(50),Last_name varchar(50),Full_name varchar(50),position varchar(50),team varchar(50),player varchar(50),MIN int,PTS int,FG_made int,FG_taken int,FT_made int,FT_taken int,threePT_made int,threePT_taken int,OREB int,DREB int,REB int,AST int,STL int,BLK int,Turnovers int,PF int,plusMinus int,fpoints float(8))' %year)


cur.execute("COPY boxscores%s FROM '/home/w205/final_project/csvfiles/playerdata%s.csv' DELIMITER ',' CSV HEADER;" %(year,year))  

conn.commit()
