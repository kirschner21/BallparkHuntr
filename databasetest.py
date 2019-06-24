import requests
import urllib.request
import time
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler	
from sklearn.linear_model import Ridge
from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists, create_database
import psycopg2



def MLBStandings(year):
    response = requests.get('https://www.baseball-reference.com/leagues/MLB/' +str(year)+ '-standings.shtml')
    soup = BeautifulSoup(response.text, 'html.parser')    
    temp=soup.find_all('th', class_='left')    
    a = soup.find_all('td')
    standings = pd.DataFrame(columns=['team','win','loss','winpct','gamesback'])
    for i in range(30):
        temps=str(temp[i])
        temps=temps[temps.find('title='):]
        teamname=temps[7:temps.find('>')-1]
        standings.loc[teamname,['team']]=teamname
        temper = str(a[i*4])
        standings.loc[teamname,['win']] = temper[temper.find('/')-4:temper.find('/')-1]
        temper = str(a[i*4+1])
        standings.loc[teamname,['loss']] = temper[temper.find('/')-4:temper.find('/')-1]
        temper = str(a[i*4+2])
        standings.loc[teamname,['winpct']] = temper[temper.find('/')-5:temper.find('/')-1]
        temper2 = str(a[i*4+3])
        standings.loc[teamname,['gamesback']] = temper2[temper2.find('/')-5:temper2.find('/')-1]
        standings.gamesback=standings.gamesback.str.strip('>')
    standings.win=standings.win.str.strip('>')
    standings.loss=standings.loss.str.strip('>')
    standings.loc[standings.gamesback=='g>--',['gamesback']]='0'
    standings.winpct=standings.winpct.astype(float)
    standings.gamesback=standings.gamesback.astype(float)
    return standings



standings2019 = MLBStandings(2019)


dbname = 'baseball'
username = 'postgres' # change this to your username



engine = create_engine('postgres://%s@localhost/%s'%(username,dbname))
print(engine.url)

if not database_exists(engine.url):
    create_database(engine.url)

standings2019.to_sql('standingsupdateddaily',engine, if_exists = 'replace')


