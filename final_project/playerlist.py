import pandas as pd
import datetime
import unidecode
import numpy as np
import sys

year = int(sys.argv[1])
x = pd.read_html('http://www.foxsports.com/nba/players?teamId=0&season=%s&position=0&playerName=&country=0&grouping=0&weightclass=0' %(year-1))[0]


for i in range(2,27):
    x = x.append(pd.read_html('http://www.foxsports.com/nba/players?teamId=0&season=%s&position=0&page=%s&country=0&grouping=0&weightclass=0' %(year-1,i)))


x.columns = ['Player','team','position','Height','Weight','Birthdate']

def splitter(x):
    return(x.split(",")[1])

x['Full_name'] = x.Player.apply(splitter)

x.index = [i for i in range(x.shape[0])]


def age(x):
    return ((datetime.datetime.today()-datetime.datetime.strptime(x,"%m/%d/%Y")).days)/365.25


x['age'] = x.Birthdate.apply(age)

def splitspaceone(x):
    return x.split(" ")[1]
def splitspacetwo(x):
    return " ".join(x.split(" ")[3:])

x['First_name'] = x.Full_name.apply(splitspaceone)
x['Last_name'] = x.Full_name.apply(splitspacetwo)

def scrapedname(row):
        return (row['First_name'][0] +'. ' +row['Last_name'])*2



x = x.drop('Player',axis=1)

x['player'] = x.apply(scrapedname,axis=1)


def conversion(x):
    if type(x) != int and type(x) != float and type(x)!=np.float64 and type(x) != np.int64:
        return unidecode.unidecode(x)
    else:
        return x
x = x.applymap(conversion)

x.to_csv("/home/w205/final_project/csvfiles/Player_list_%s.csv"%(year))
