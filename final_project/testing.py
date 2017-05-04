import pandas as pd
import numpy as np
year=2017
x=pd.read_csv('/home/w205/final_project/csvfiles/Player_list_'+str(year)+'.csv')

players1 = pd.read_csv('/home/w205/final_project/csvfiles/boxscores'+str(year)+'.csv')

players2 = players1.merge(x[['First_name','Last_name','Full_name','player','team']],on=['player','team'], how = 'left')
np.unique(players2.player[pd.isnull(players2.Full_name)])
THE_NA = players2[players2.isnull().any(axis=1)]
THE_NA = THE_NA[['team', 'player', 'MIN', 'FG', '3PT', 'FT', 'OREB', 'DREB', 'REB',
                 'AST', 'STL', 'BLK', 'TO', 'PF', '+/-', 'PTS', 'position']]
THE_NA = THE_NA.merge(x[['First_name','Last_name','Full_name','player']],on=['player'],how='left')
players2.dropna(inplace=True)
players2 = players2.append(THE_NA)
print(players2[['player','team','Full_name','PTS']][5450:5500])
