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

def DistanceDic(distance,distancedf):
    distancedic = {}
    for i in distancedf.columns:
        distancedic[i] = distancedf[(distancedf[i] < distance)].index.values
    return distancedic



def BuildRoadTrip(indexarray,datearray,finalday,totalgames,locationslist,distancedic):
    newindexarray = []
    newdatearray = []
    newlocationslist = []
    if min(datearray) >= finalday:
        return indexarray
    for tt, ind in enumerate(indexarray):
        if datearray[tt] == finalday:
            newdatearray.extend([finalday])
            newindexarray.append(ind)
            newlocationslist.append(locationslist[tt])
        else:
            if len(indexarray)<2:
                tempind = ind
                if isinstance(tempind,list):
                    tempind = tempind[-1]
                tempdf = totalgames.loc[tempind]
                newgames, newdays, newlocs = FindViableGames(totalgames,tempdf.Dayofyear,tempdf.Location, finalday, locationslist,distancedic)
            else:
                tempdf = totalgames.loc[ind].nlargest(1, columns=['Dayofyear'])
                newgames, newdays, newlocs = FindViableGames(totalgames,tempdf.Dayofyear.values[0],tempdf.Location.values[0], finalday,locationslist[tt],distancedic)
            if len(newgames) == 0:
                newdatearray.extend([finalday])
                newindexarray.append(ind)
                newlocationslist.append(locationslist[tt])
            else:
                for kk, k in enumerate(newgames):
                    if len(indexarray)<2:
                        temp = [ind] + [int(k)]
                        temploc = locationslist + [newlocs[kk]]
                    else:
                        temp = ind + [int(k)]
                        temploc = locationslist[tt] + [newlocs[kk]]
                    newindexarray.append(temp)
                    newdatearray.extend(newdays.astype(int))
                    newlocationslist.append(temploc)
    return BuildRoadTrip(newindexarray,newdatearray,finalday,totalgames,newlocationslist,distancedic)


def FindViableGames(listofgames,start_date,location,finalday,locationslist,distancedic):
    viablegames = listofgames.loc[((listofgames.Location.isin(distancedic[location])) &  ~(listofgames.Location.isin(locationslist))) & ((listofgames['Dayofyear'] > start_date) &  (listofgames['Dayofyear'] <= finalday)),:]
    if viablegames.empty:
        return [], [], []
    else:
        return viablegames.index, viablegames.Dayofyear, viablegames.Location.values



def DescribeItinerary(df,distancematrix,ind,indices):
    output = pd.DataFrame(columns = ['total_days','total_miles','total_games', 'Division',
                                     'percent_day_games','percent_weekends','min_temp','avg_temp','max_temp','game_quality_h','game_quality_a',
                                    'game_importance_h','game_importance_a','LowPrice','MedPrice','indices'],index = [ind])
    output['total_days'] = df.Dayofyear.max() - df.Dayofyear.min() + 1
    temp_dis = 0
    games = df.shape[0]
    for i in range(games-1):
        temp_dis += distancematrix.loc[df.loc[df.index[i]].Location,df.loc[df.index[i+1]].Location]
    for q in df.Location.unique():
        output[q] = 1
    output['total_miles'] = temp_dis
    output['total_games'] = games
    output['percent_weekends'] =  df.loc[df.Dayofweek > 3].shape[0] / games
    output['percent_day_games'] = df.loc[df.TimeofDay < 17].shape[0] / games
    output['min_temp'] = df.tmin.min()
    output['avg_temp'] = df.tavg.mean()
    output['max_temp'] = df.tmax.max()
    output['game_quality_h'] = (df.hwinpct).mean()
    output['game_quality_a'] = (df.awinpct).mean()
    output['game_importance_h'] = (df.hgamesback).mean()
    output['game_importance_a'] = (df.agamesback).mean()
    output['LowPrice'] = df.LowPrice.sum()
    output['Division'] = df.division.sum()
    output['MedPrice'] = df.MedPrice.sum()
    output['indices'] = [indices]
    return output


standings2019 = MLBStandings(2019)

auth1 = open('auth1.txt','r').read()
auth2 = open('auth2.txt','r').read()
bigapis = []

for i in range(1,31):
    bigapis.append(requests.get('https://api.seatgeek.com/2/events?performers[home_team].id=' + str(i) + '&per_page=100',auth=(auth1[:-1], auth2[:-1]) ).json())

data = pd.DataFrame()
for stadium in bigapis:
    for game in stadium['events']:
        
        date = game['datetime_local']
        lp = game['stats']['lowest_price']
        mp = game['stats']['median_price']
        at = game['short_title'].split(' ')[0]
        ht = game['short_title'].split(' ')[2]
        loc = game['venue']['name']
        url = game['url']
        temp = pd.DataFrame(data = {'Date' : date, 'LowPrice' : lp, 'MedPrice' : mp, 'AwayTeam' : at, 'HomeTeam' : ht, 'Location' : loc, 'url' : url}, index = [date])
        data = data.append(temp)

data['Date2']=pd.to_datetime(data['Date'])
data['Dayofyear']=data.Date2.dt.dayofyear
data['TimeofDay']=data.Date2.dt.hour
data['Dayofweek']=data.Date2.dt.weekday
distancedf = pd.read_csv('fulldistancematrix.csv',index_col=0)

distancedic = DistanceDic(300,distancedf)

data.loc[data.Location=='Angel Stadium of Anaheim',['Location']] = 'Angels Stadium of Anaheim'
data.loc[data.Location=='Petco Park',['Location']] = 'PETCO Park'
data.loc[data.Location=='Globe Life Park',['Location']] = 'Globe Life Park in Arlington'
data.loc[data.Location== 'Oakland-Alameda County Coliseum',['Location']] = 'OAC Coliseum'
data.loc[data.Location=='Oracle Park',['Location']] = 'ATT Park'
data.loc[data.Location=='T-Mobile Park',['Location']] = 'Safeco Field'

kk = pd.read_csv('baseballweatherdata.csv')


kk['date2']=pd.to_datetime(kk.date)
kk['Dayofyear']=kk.date2.dt.dayofyear
kk['month']=kk.date2.dt.month
kk['day']=kk.date2.dt.day


data.loc[data.AwayTeam == 'Red', ['AwayTeam']] = 'Red '

data = data[data.AwayTeam != 'TEST']

data = data.reset_index()
data = data.drop(['index'], axis=1)

for i in data.index:
    data.loc[i,['AwayTeam']] = standings2019.loc[standings2019.team.str.contains(data.loc[i].AwayTeam)].team.values


stadiumdiction={}


stadiumdiction['Angels Stadium of Anaheim'] = 'Los Angeles Angels'
stadiumdiction['Chase Field'] = 'Arizona Diamondbacks'
stadiumdiction['SunTrust Park'] = 'Atlanta Braves'
stadiumdiction['Oriole Park at Camden Yards'] = 'Baltimore Orioles'
stadiumdiction['Fenway Park'] = 'Boston Red Sox'
stadiumdiction['Wrigley Field'] = 'Chicago Cubs'
stadiumdiction['Guaranteed Rate Field'] = 'Chicago White Sox'
stadiumdiction['Great American Ball Park'] = 'Cincinnati Reds'
stadiumdiction['Progressive Field'] = 'Cleveland Indians'
stadiumdiction['Coors Field'] = 'Colorado Rockies'
stadiumdiction['Comerica Park'] = 'Detroit Tigers'
stadiumdiction['Marlins Park'] = 'Miami Marlins'
stadiumdiction['Minute Maid Park'] = 'Houston Astros'
stadiumdiction['Kauffman Stadium'] = 'Kansas City Royals'
stadiumdiction['Dodger Stadium'] = 'Los Angeles Dodgers'
stadiumdiction['Miller Park'] = 'Milwaukee Brewers'
stadiumdiction['Target Field'] ='Minnesota Twins'
stadiumdiction['Nationals Park'] = 'Washington Nationals'
stadiumdiction['Citi Field'] = 'New York Mets'
stadiumdiction['Yankee Stadium'] = 'New York Yankees'
stadiumdiction['OAC Coliseum'] = 'Oakland Athletics'
stadiumdiction['Citizens Bank Park'] ='Philadelphia Phillies'
stadiumdiction['PNC Park'] = 'Pittsburgh Pirates'
stadiumdiction['PETCO Park'] = 'San Diego Padres'
stadiumdiction['ATT Park'] = 'San Francisco Giants'
stadiumdiction['Safeco Field'] = 'Seattle Mariners'
stadiumdiction['Busch Stadium'] = 'St. Louis Cardinals'
stadiumdiction['Tropicana Field'] = 'Tampa Bay Rays'
stadiumdiction['Globe Life Park in Arlington'] = 'Texas Rangers'
stadiumdiction['Rogers Centre'] = 'Toronto Blue Jays'

for i in data.index:
    data.loc[i,['HomeTeam']] = stadiumdiction[data.loc[i].Location]


data['day']=data.Date2.dt.day
data['month']=data.Date2.dt.month

data['tmin'] = 0
data['tavg'] = 0
data['tmax'] = 0
data['hwinpct'] = 0
data['hgamesback'] = 0
data['awinpct'] = 0
data['agamesback'] = 0

for i in data.index:
    tempweather = kk.loc[(kk.park == data.loc[i].Location) & ((kk.day == data.loc[i].day) & (kk.month == data.loc[i].month))]
    data.loc[i,['tmin']] = tempweather.tmin.values
    data.loc[i,['tavg']] = tempweather.tavg.values
    data.loc[i,['tmax']] = tempweather.tmax.values
    temphome = standings2019.loc[data.loc[i].HomeTeam]
    data.loc[i,['hwinpct']] = temphome.winpct
    data.loc[i,['hgamesback']] = temphome.gamesback
    tempaway = standings2019.loc[data.loc[i].AwayTeam]
    data.loc[i,['awinpct']] = tempaway.winpct
    data.loc[i,['agamesback']] = tempaway.gamesback

divisiondic = {}
for i in range(30):
    divisiondic[standings2019.iloc[i].team] = int(i/5)


data['division'] = 0
for i in data.index:
    data.loc[i,'division'] = int(divisiondic[data.loc[i].AwayTeam] == divisiondic[data.loc[i].HomeTeam])

#tempindices = data.loc[(data.month == 7) & (data.Dayofweek > 3)].index


#bigbigarray = []
#for i in tempindices:

#    tempindex = i
#    tempdataframe = data.loc[tempindex]

#    bigbigarray += BuildRoadTrip([tempindex],[tempdataframe.Dayofyear],tempdataframe.Dayofyear+13-tempdataframe.Dayofweek,data,[tempdataframe.Location],distancedic)

#julyroadtrips = pd.DataFrame()
#for i in bigbigarray:
#    print(julyroadtrips.shape[0]/len(bigbigarray))
    
#    if isinstance(i,list):
#        julyroadtrips = julyroadtrips.append(DescribeItinerary(data.loc[i],distancedf,'a',i),sort=False)

tempindices2 = data.loc[(data.month == 8) & (data.Dayofweek > 3)].index

tempindices2b = tempindices2.tolist()

tempindices2b.remove(data.loc[(data.Dayofyear==215) & (data.Location == 'Minute Maid Park')].index.values[0])
tempindices2b.remove(data.loc[(data.Dayofyear==216) & (data.Location == 'Minute Maid Park')].index.values[0])


bigbigarray2 = []
for i in tempindices2b:

    tempindex = i
    tempdataframe = data.loc[tempindex]
    tempzz = BuildRoadTrip([tempindex],[tempdataframe.Dayofyear],tempdataframe.Dayofyear+13-tempdataframe.Dayofweek,data,[tempdataframe.Location],distancedic)
    if isinstance(tempzz,list):
        bigbigarray2 += tempzz



augroadtrips2 = pd.DataFrame()
for ind, i in enumerate(bigbigarray2):
 #   print(augroadtrips2.shape[0]/len(bigbigarray2))
    
    if isinstance(i,list):
        augroadtrips2 = augroadtrips2.append(DescribeItinerary(data.loc[i],distancedf,ind,i),sort=False)



augroadtrips2 = augroadtrips2.fillna(0)
#julyroadtrips = julyroadtrips.fillna(0)


#x = julyroadtrips.drop(['LowPrice','MedPrice','indices'],axis=1)
#y1 = julyroadtrips.LowPrice
#y2 = julyroadtrips.MedPrice

xb = augroadtrips2.drop(['LowPrice','MedPrice','indices'],axis=1)
yb1 = augroadtrips2.LowPrice
yb2 = augroadtrips2.MedPrice
#xb=xb[x.columns]


#x2 = x.drop(['total_days','total_miles','min_temp','max_temp','avg_temp'],axis=1)
#x2 = x.drop(['total_days','total_miles','min_temp','max_temp'],axis=1)
#x2['mean_temp'] = x.min_temp + x.max_temp

xb2 = xb.drop(['total_days','total_miles','min_temp','max_temp'],axis=1)



xb2.percent_day_games = (xb2.percent_day_games - xb2.percent_day_games.mean()) * xb2.total_games
xb2.percent_weekends = (xb2.percent_weekends - xb2.percent_weekends.mean())* xb2.total_games
xb2.game_quality_h= (xb2.game_quality_h-xb2.game_quality_h.mean()) * xb2.total_games
xb2.game_quality_a = (xb2.game_quality_a-xb2.game_quality_a.mean()) * xb2.total_games
xb2.game_importance_h = (xb2.game_importance_h - xb2.game_importance_h.mean()) * xb2.total_games
xb2.game_importance_a = (xb2.game_importance_a - xb2.game_importance_a.mean()) * xb2.total_games
xb2.avg_temp = (xb2.avg_temp - xb2.avg_temp.mean()) * xb2.total_games



#xb2 = xb.drop(['total_days','total_miles','min_temp','max_temp','avg_temp'],axis=1)
#xb2['mean_temp'] = xb.min_temp + xb.max_temp

#julyscaler = StandardScaler()
augustscaler = StandardScaler()

#julyscaler.fit(x2)
augustscaler.fit(xb2)

#x2t = julyscaler.transform(x2)
xb2t = augustscaler.transform(xb2)

lowprice = Ridge(alpha=1)
medprice = Ridge(alpha=1)

lowprice.fit(xb2t, yb1)
medprice.fit(xb2t, yb2)



augroadtrips2['day_games_value_low'] = xb2t[:,2] * lowprice.coef_[2]
augroadtrips2['weekend_value_low'] = xb2t[:,3] *lowprice.coef_[3]
augroadtrips2['quality_value_low'] = np.sum(xb2t[:,[1,5,6,7,8]] *lowprice.coef_[[1,5,6,7,8]],axis=1)
augroadtrips2['location_value_low'] = np.sum(xb2t[:,9:] *lowprice.coef_[9:],axis=1)
augroadtrips2['weather_value_low'] = xb2t[:,4] *lowprice.coef_[4]
augroadtrips2['predicted_low'] = lowprice.predict(xb2t)

augroadtrips2['day_games_value_med'] = xb2t[:,2] * medprice.coef_[2]
augroadtrips2['weekend_value_med'] = xb2t[:,3] *medprice.coef_[3]
augroadtrips2['quality_value_med'] = np.sum(xb2t[:,[1,5,6,7,8]] *medprice.coef_[[1,5,6,7,8]],axis=1)
augroadtrips2['location_value_med'] = np.sum(xb2t[:,9:] *medprice.coef_[9:],axis=1)
augroadtrips2['weather_value_med'] = xb2t[:,4] *medprice.coef_[4]
augroadtrips2['predicted_med'] = medprice.predict(xb2t)



augroadtrips2.columns = augroadtrips2.columns.str.lower()
augroadtrips2.columns = augroadtrips2.columns.str.replace(' ', '_')


data.columns = data.columns.str.lower()
data['gameindex']=data.index


dbname = 'baseball'
username = 'matthew:postsqlgres' # change this to your username



engine = create_engine('postgres://%s@localhost/%s'%(username,dbname))
print(engine.url)

if not database_exists(engine.url):
    create_database(engine.url)

augroadtrips2.to_sql('augroadtripsupdateddaily',engine, if_exists = 'replace')
data.to_sql('gamesdataupdateddaily',engine, if_exists = 'replace')
standings2019.to_sql('standingsupdateddaily',engine, if_exists = 'replace')
