import pandas as pd
import numpy as np
import datetime
codes = ['bos','gs','tor','lac','chi','cle','ind','mil','hou','mem','sa','atl','wsh','okc','por','utah']

year= datetime.date.today().year

upcomers=pd.read_html('http://www.espn.com/nba/team/schedule/_/name/bos/year/2017/seasontype/',header=2)[0]
upcomers['home_team'] = 'BOS'

for i in codes[1:]:
    upcomers1 = pd.read_html('http://www.espn.com/nba/team/schedule/_/name/%s/year/%s/seasontype/' %(i,year),header=2)[0]
    upcomers1['home_team'] = i.upper()
    upcomers = upcomers.append(upcomers1)

upcomers = upcomers[~upcomers.OPPONENT.str.contains('@')]

def to_date(x):
    y = datetime.datetime.strptime(x, '%a, %b %d')
    return datetime.date(2017,y.month,y.day)

upcomers.DATE = upcomers.DATE.apply(to_date)
upcomers.replace(to_replace='UTAH',value='UTA',inplace=True)

upcomers['OPPONENT'] = upcomers.OPPONENT.apply(lambda x: x[2:])

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

upcomers['away_team'] = upcomers.OPPONENT.map(team_dict)

upcomers.drop('OPPONENT',axis=1,inplace=True)

todays_games = upcomers[upcomers.DATE==datetime.date.today()]


#LOOK HERE
checkpointx = pd.read_csv('/home/w205/final_project/csvfiles/Player_list_2017.csv')
players2 = pd.read_csv('/home/w205/final_project/csvfiles/playerdata2017.csv')

todays_players = checkpointx.Full_name[(checkpointx.team.isin(todays_games.home_team))|(checkpointx.team.isin(todays_games.away_team))]

todaydf = players2[players2.Full_name.isin(todays_players)]

starters = []
for i in np.unique(players2.Full_name[players2.MIN>25]):
    if np.mean(players2.MIN[players2.Full_name==i])>25:
        starters.append(i)

opposingPTS = []
theirposition = []
opponentcount = []
team = []

for i in starters:
    opposingPTS.append(np.mean(players2.PTS[(players2['id'].isin(players2['id'][players2.Full_name==i])) &
        (players2.position == str(np.unique(players2.position[players2.Full_name==i])[0])) &
        ~(players2.team.isin(players2['team'][players2.Full_name==i])) &
                    (players2.MIN>25)]))
    opponentcount.append(np.count_nonzero(players2.PTS[(players2['id'].isin(players2['id'][players2.Full_name==i])) &
        (players2.position == str(np.unique(players2.position[players2.Full_name==i])[0])) &
        ~(players2.team.isin(players2['team'][players2.Full_name==i])) &
                    (players2.MIN>25)]))
    theirposition.append(str(np.unique(players2.position[players2.Full_name==i])[0]))
    team.append(str(np.unique(players2.team[players2.Full_name==i])[0]))


positionalpoints = pd.DataFrame(np.column_stack([starters,theirposition,team,opponentcount,opposingPTS]),
                                columns = ['Full_name','position','team','opponent_count','Points_allowed'])

positionalpoints = positionalpoints.sort_values(by='Points_allowed',ascending=False)


PPG = pd.DataFrame(players2.pivot_table(index=['Full_name','team','position'],values='fpoints'))

PPG = PPG.sort_values(by='fpoints',ascending=False)


PPG['all_values'] =PPG.index
PPG['Full_name'] = PPG.all_values.apply(lambda x: x[0])
PPG['team'] = PPG.all_values.apply(lambda x: x[1])
PPG['position'] = PPG.all_values.apply(lambda x: x[2])

today_PPG = PPG[PPG.Full_name.isin(todays_players)]

today_PA = positionalpoints[positionalpoints.Full_name.isin(todays_players)]

a = today_PPG.merge(todays_games[['home_team','away_team']],left_on='team',right_on='home_team',how='left')
b = a.merge(todays_games[['home_team','away_team']],left_on='team',right_on='away_team',how='left')
b['home_team'] = b['home_team_x'].fillna(b.home_team_y)
b['away_team'] = b['away_team_x'].fillna(b.away_team_y)
b.drop(['all_values','home_team_x','home_team_y','away_team_x','away_team_y'],axis=1,inplace=True)


def opponent(row):
    if row['team'] == row['home_team']:
        return row['away_team']
    else:
        return row['home_team']

b['opponent'] = b.apply(opponent,axis=1)

finaldf = b.merge(today_PA,left_on=['position','opponent'],right_on=['position','team'],how='right')
finaldf['best_ratio'] = finaldf['fpoints']/finaldf['Points_allowed'].apply(float)

finaldf = finaldf[['Full_name_x','Full_name_y','fpoints','Points_allowed','best_ratio']].sort_values(by='best_ratio',ascending=False)
finaldf = finaldf[finaldf.Full_name_x.duplicated(keep='first')]

finaldf.to_csv('/home/w205/final_project/csvfiles/todays_best_matchups.csv')

