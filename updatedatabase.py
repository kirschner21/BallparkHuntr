# import basic packages
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


# define function to scrape baseball-reference.com
def MLBStandings(year):
    # scrape website
    response = requests.get('https://www.baseball-reference.com/leagues/MLB/' +str(year)+ '-standings.shtml')
    
    # convert to soup
    soup = BeautifulSoup(response.text, 'html.parser')    
    
    # intialize output dataframe
    standings = pd.DataFrame(columns = ['team','win','loss','winpct','gamesback'])

    # find relevant html tags
    temp = soup.find_all('th', class_ = 'left')    
    a = soup.find_all('td')

    # get standings for each team
    for i in range(30):
        temps = str(temp[i])
        temps = temps[temps.find('title='):]
        teamname = temps[7:temps.find('>') - 1]
        standings.loc[teamname,['team']] = teamname
        temper = str(a[i * 4])
        standings.loc[teamname,['win']] = temper[temper.find('/') - 4:temper.find('/') - 1]
        temper = str(a[i * 4 + 1])
        standings.loc[teamname,['loss']] = temper[temper.find('/') - 4:temper.find('/') - 1]
        temper = str(a[i * 4 + 2])
        standings.loc[teamname,['winpct']] = temper[temper.find('/') - 5:temper.find('/') - 1]
        temper2 = str(a[i * 4 + 3])
        standings.loc[teamname,['gamesback']] = temper2[temper2.find('/') - 5:temper2.find('/') - 1]
        standings.gamesback = standings.gamesback.str.strip('>')
    
    # deal with division leaders
    standings.win = standings.win.str.strip('>')
    standings.loss = standings.loss.str.strip('>')
    standings.loc[standings.gamesback == 'g>--',['gamesback']] = '0'
    standings.winpct = standings.winpct.astype(float)
    standings.gamesback = standings.gamesback.astype(float)

    # return output dataframe
    return standings

# function to identify stadiums within a given distance of another stadium
def DistanceDic(distance, distancedf):
    distancedic = {}
    for i in distancedf.columns:
        distancedic[i] = distancedf[(distancedf[i] < distance)].index.values
    return distancedic


# function to recursively build possible road trips
def BuildRoadTrip(indexarray, datearray, finalday, totalgames, locationslist, distancedic):
    # intitialize new arrays
    newindexarray = [] # list of itineraries made of lists of indices of each game
    newdatearray = [] # list of the final day of each itinerary
    newlocationslist = [] # list of list of locations visited in each itinerary

    # terminate if all itineraries reach the final day
    if min(datearray) >= finalday:
        return indexarray

    for tt, ind in enumerate(indexarray):
        # check if an itinerary is on the last day, and if so resend it through the algorithm
        if datearray[tt] == finalday:
            newdatearray.extend([finalday])
            newindexarray.append(ind)
            newlocationslist.append(locationslist[tt])

        # actually build an itinerary
        else:
            # deal with corner case of only having one game
            if len(indexarray)<2:
                tempind = ind
                if isinstance(tempind, list):
                    tempind = tempind[-1]
                tempdf = totalgames.loc[tempind]

                # find new games
                newgames, newdays, newlocs = FindViableGames(totalgames, tempdf.Dayofyear, tempdf.Location, finalday, locationslist, distancedic)
            else:
                # find new game, extra check to make sure the games are in order
                tempdf = totalgames.loc[ind].nlargest(1, columns=['Dayofyear'])
                newgames, newdays, newlocs = FindViableGames(totalgames, tempdf.Dayofyear.values[0], tempdf.Location.values[0], finalday,locationslist[tt], distancedic)
                
                # deal with if the corner case of not having any more possible games available
            if len(newgames) == 0:
                newdatearray.extend([finalday])
                newindexarray.append(ind)
                newlocationslist.append(locationslist[tt])
            
                # add games to itinerary
            else:
                for kk, k in enumerate(newgames):
                    # deal with only having one game
                    if len(indexarray)<2:
                        temp = [ind] + [int(k)]
                        temploc = locationslist + [newlocs[kk]]
                    # general case
                    else:
                        temp = ind + [int(k)]
                        temploc = locationslist[tt] + [newlocs[kk]]
                    
                    # actually add to the list
                    newindexarray.append(temp)
                    newdatearray.extend(newdays.astype(int))
                    newlocationslist.append(temploc)

    # recursively call function
    return BuildRoadTrip(newindexarray, newdatearray, finalday, totalgames, newlocationslist, distancedic)

# function to identify possible games
def FindViableGames(listofgames, start_date, location, finalday, locationslist, distancedic):
    # find games within distance of the current location, that isn't at a previously visited stadium, after the day of the last game, and before the final day
    viablegames = listofgames.loc[((listofgames.Location.isin(distancedic[location])) &  ~(listofgames.Location.isin(locationslist))) & ((listofgames['Dayofyear'] > start_date) &  (listofgames['Dayofyear'] <= finalday)),:]

    # deal with no games being found
    if viablegames.empty:
        return [], [], []
    
    # else return all viable games
    else:
        return viablegames.index, viablegames.Dayofyear, viablegames.Location.values


# function to extract key characteristics about the itinerary
def DescribeItinerary(df,distancematrix,ind,indices):
    output = pd.DataFrame(columns = ['total_days', 'total_miles', 'total_games', 'Division',
                                     'percent_day_games', 'percent_weekends', 'min_temp', 'avg_temp',
                                      'max_temp', 'game_quality_h', 'game_quality_a','game_importance_h',
                                      'game_importance_a','LowPrice','MedPrice','indices'], index = [ind])
    
    # determine how long each itinerary is
    output['total_days'] = df.Dayofyear.max() - df.Dayofyear.min() + 1
    
    # calculate travel distance
    temp_dis = 0
    games = df.shape[0]
    for i in range(games-1):
        temp_dis += distancematrix.loc[df.loc[df.index[i]].Location,df.loc[df.index[i+1]].Location]
    
    # all other parameters
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


# actually get the baseball standings
standings2019 = MLBStandings(2019)

# load in API key info
auth1 = open('auth1.txt','r').read()
auth2 = open('auth2.txt','r').read()
bigapis = []

# make API calls to seatgeek to get info about each stadium (with location ids 1-30) 
for i in range(1,31):
    bigapis.append(requests.get('https://api.seatgeek.com/2/events?performers[home_team].id=' + str(i) + '&per_page=100',auth = (auth1[:-1], auth2[:-1]) ).json())

# convert json data to pandas dataframe
data = pd.DataFrame()

for stadium in bigapis:
    for game in stadium['events']:
        
        # extract info about each game
        date = game['datetime_local']
        lp = game['stats']['lowest_price']
        mp = game['stats']['median_price']
        at = game['short_title'].split(' ')[0]
        loc = game['venue']['name']
        url = game['url']

        # put it into a dataframe
        temp = pd.DataFrame(data = {'Date' : date, 'LowPrice' : lp, 'MedPrice' : mp, 'AwayTeam' : at, 'Location' : loc, 'url' : url}, index = [date])
        
        # add it to the overall dataframe
        data = data.append(temp)

# convert the input to a datetime object
data['Date2'] = pd.to_datetime(data['Date'])

# extract specific info about the date
data['Dayofyear'] = data.Date2.dt.dayofyear
data['TimeofDay'] = data.Date2.dt.hour
data['Dayofweek'] = data.Date2.dt.weekday
data['day']=data.Date2.dt.day
data['month']=data.Date2.dt.month


# import a distance matrix
distancedf = pd.read_csv('fulldistancematrix.csv', index_col = 0)

# calculate a dictionary that determines which stadiums are within 300 miles of one another
distancedic = DistanceDic(300, distancedf)

# make the names of the stadiums match up
data.loc[data.Location == 'Angel Stadium of Anaheim', ['Location']] = 'Angels Stadium of Anaheim'
data.loc[data.Location == 'Petco Park', ['Location']] = 'PETCO Park'
data.loc[data.Location == 'Globe Life Park', ['Location']] = 'Globe Life Park in Arlington'
data.loc[data.Location == 'Oakland-Alameda County Coliseum', ['Location']] = 'OAC Coliseum'
data.loc[data.Location == 'Oracle Park', ['Location']] = 'ATT Park'
data.loc[data.Location == 'T-Mobile Park', ['Location']] = 'Safeco Field'

# load in weather data
kk = pd.read_csv('baseballweatherdata.csv')

# create datetime object and extract date info
kk['date2'] = pd.to_datetime(kk.date)
kk['Dayofyear'] = kk.date2.dt.dayofyear
kk['month'] = kk.date2.dt.month
kk['day'] = kk.date2.dt.day


# Reds and Red Soxs both have the substring Red so we make them distinct
data.loc[data.AwayTeam == 'Red', ['AwayTeam']] = 'Red '

# clean erroneous data point
data = data[data.AwayTeam != 'TEST']

# reset index
data = data.reset_index()

# remove the resulting coumn
data = data.drop(['index'], axis=1)


# initialize data that we will correlate from the other data sets
data['HomeTeam'] = ''
data['tmin'] = 0
data['tavg'] = 0
data['tmax'] = 0
data['hwinpct'] = 0
data['hgamesback'] = 0
data['awinpct'] = 0
data['agamesback'] = 0

# convert away teams to the same way they appear in the standings
for i in data.index:
    data.loc[i, ['AwayTeam']] = standings2019.loc[standings2019.team.str.contains(data.loc[i].AwayTeam)].team.values


# create a dictionary of stadiums to determine the home teams
stadiumdiction = {}
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



# correlate the other data
for i in data.index:
    # add in weather data
    tempweather = kk.loc[(kk.park == data.loc[i].Location) & ((kk.day == data.loc[i].day) & (kk.month == data.loc[i].month))]
    data.loc[i, ['tmin']] = tempweather.tmin.values
    data.loc[i, ['tavg']] = tempweather.tavg.values
    data.loc[i, ['tmax']] = tempweather.tmax.values
    
    # add in standings    
    data.loc[i, ['HomeTeam']] = stadiumdiction[data.loc[i].Location]
    temphome = standings2019.loc[data.loc[i].HomeTeam]
    data.loc[i, ['hwinpct']] = temphome.winpct
    data.loc[i, ['hgamesback']] = temphome.gamesback
    tempaway = standings2019.loc[data.loc[i].AwayTeam]
    data.loc[i, ['awinpct']] = tempaway.winpct
    data.loc[i, ['agamesback']] = tempaway.gamesback

# determine what teams are in the same divisions
divisiondic = {}
for i in range(30):
    divisiondic[standings2019.iloc[i].team] = int(i / 5)

# determine if a game is a divisional game
data['division'] = 0
for i in data.index:
    data.loc[i,'division'] = int(divisiondic[data.loc[i].AwayTeam] == divisiondic[data.loc[i].HomeTeam])

# find games in August that start during the weekend (Friday, Saturday, or Sunday)
tempindices2 = data.loc[(data.month == 8) & (data.Dayofweek > 3)].index

# convert it to a list
tempindices2b = tempindices2.tolist()

# remove bad data
tempindices2b.remove(data.loc[(data.Dayofyear == 215) & (data.Location == 'Minute Maid Park')].index.values[0])
tempindices2b.remove(data.loc[(data.Dayofyear == 216) & (data.Location == 'Minute Maid Park')].index.values[0])


# Build road trips from the initial games
bigbigarray2 = []
for i in tempindices2b:

    # find road trips that start from each index
    tempindex = i
    tempdataframe = data.loc[tempindex]
    tempzz = BuildRoadTrip([tempindex], [tempdataframe.Dayofyear], tempdataframe.Dayofyear + 13 - tempdataframe.Dayofweek, 
        data, [tempdataframe.Location], distancedic)
    
    # add it to the list of itineraries
    if isinstance(tempzz, list):
        bigbigarray2 += tempzz


# get the details of each itinerary
augroadtrips2 = pd.DataFrame()
for ind, i in enumerate(bigbigarray2):
    if isinstance(i, list):
        augroadtrips2 = augroadtrips2.append(DescribeItinerary(data.loc[i], distancedf, ind, i), sort = False)


# deal with NAs (specifically the stadiums we don't visit)
augroadtrips2 = augroadtrips2.fillna(0)


# convert feature arrays
xb = augroadtrips2.drop(['LowPrice','MedPrice','indices'],axis=1)
yb1 = augroadtrips2.LowPrice
yb2 = augroadtrips2.MedPrice

# remove features that don't matter
xb2 = xb.drop(['total_days','total_miles','min_temp','max_temp'],axis=1)


# scale features that should scale with the number of games
xb2.percent_day_games = (xb2.percent_day_games - xb2.percent_day_games.mean()) * xb2.total_games
xb2.percent_weekends = (xb2.percent_weekends - xb2.percent_weekends.mean())* xb2.total_games
xb2.game_quality_h= (xb2.game_quality_h-xb2.game_quality_h.mean()) * xb2.total_games
xb2.game_quality_a = (xb2.game_quality_a-xb2.game_quality_a.mean()) * xb2.total_games
xb2.game_importance_h = (xb2.game_importance_h - xb2.game_importance_h.mean()) * xb2.total_games
xb2.game_importance_a = (xb2.game_importance_a - xb2.game_importance_a.mean()) * xb2.total_games
xb2.avg_temp = (xb2.avg_temp - xb2.avg_temp.mean()) * xb2.total_games

# implement a standard scaler
augustscaler = StandardScaler()
augustscaler.fit(xb2)
xb2t = augustscaler.transform(xb2)

# make the ridge regressions
lowprice = Ridge(alpha = 1)
medprice = Ridge(alpha = 1)
lowprice.fit(xb2t, yb1)
medprice.fit(xb2t, yb2)


# extract the values for specific aspects of an itinerary
augroadtrips2['day_games_value_low'] = xb2t[:, 2] * lowprice.coef_[2]
augroadtrips2['weekend_value_low'] = xb2t[:, 3] *lowprice.coef_[3]
augroadtrips2['quality_value_low'] = np.sum(xb2t[:, [1, 5, 6, 7, 8]] * lowprice.coef_[[1, 5, 6, 7, 8]], axis = 1)
augroadtrips2['location_value_low'] = np.sum(xb2t[:,9:] * lowprice.coef_[9:], axis = 1)
augroadtrips2['weather_value_low'] = xb2t[:, 4] * lowprice.coef_[4]
augroadtrips2['predicted_low'] = lowprice.predict(xb2t)
augroadtrips2['day_games_value_med'] = xb2t[:,2] * medprice.coef_[2]
augroadtrips2['weekend_value_med'] = xb2t[:,3] * medprice.coef_[3]
augroadtrips2['quality_value_med'] = np.sum(xb2t[:, [1, 5, 6, 7, 8]] * medprice.coef_[[1, 5, 6, 7, 8]], axis = 1)
augroadtrips2['location_value_med'] = np.sum(xb2t[:, 9:] * medprice.coef_[9:], axis = 1)
augroadtrips2['weather_value_med'] = xb2t[:,4] *medprice.coef_[4]
augroadtrips2['predicted_med'] = medprice.predict(xb2t)


# make the data a little nicer for sql
augroadtrips2.columns = augroadtrips2.columns.str.lower()
augroadtrips2.columns = augroadtrips2.columns.str.replace(' ', '_')
data.columns = data.columns.str.lower()
data['gameindex']=data.index

# prepare to write it to a database
dbname = 'baseball'
username = 'matthew:postsqlgres'
engine = create_engine('postgres://%s@localhost/%s'%(username,dbname))

if not database_exists(engine.url):
    create_database(engine.url)

# write the data to sql

augroadtrips2.to_sql('augroadtripsupdateddaily', engine, if_exists = 'replace')
data.to_sql('gamesdataupdateddaily', engine, if_exists = 'replace')
standings2019.to_sql('standingsupdateddaily', engine, if_exists = 'replace')