# importing basic packages
from flask import render_template
from Baseball import app
from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists, create_database
import pandas as pd
import numpy as np
import psycopg2
from flask import request


dbname = 'baseball' # setting the database name



# defining several useful functions

# making the string for minutes in the time look better
def AddTens(minutes):
  if len(minutes) == 1:
    minutes = '0' + minutes
  return minutes

# making sure 12 pm appears as 12 pm rather than 0 pm
def AddHour(hour):
  if hour == 0:
    hour = 12
  return hour

# find the five cheapest itineraries while removing similar itineraries (itineraries visiting the same stadiums in the same order with the same teams playing)
def OptimizeTrip(example):
    
    #ititialize dataframe
    toproadtrips = pd.DataFrame()

    # keep adding itineraries until five are found or there are no more left
    while toproadtrips.shape[0] < 5 and example.shape[0] > 0:
        
        # find the cheapest itinerary
        tempexample = example.loc[example.price == example.price.min()].nsmallest(1, columns = ['price'])
        
        # add it to the output
        toproadtrips = toproadtrips.append(tempexample)

        # remove similar itineraries
        example = example.drop(example.loc[(((example.total_miles == tempexample.total_miles.values[0]) & (example.game_importance_a == tempexample.game_importance_a.values[0])) & (example.game_quality_a == tempexample.game_quality_a.values[0]))].index, axis = 0)

    return(toproadtrips)
      
# find colors with green being a good value and red being a bad value
def IndividualColor(value):

  if value <= 0:
    return "green"

  else:
    return "magenta"

# convert effects of price to effective string
def SavingsToString(value):

  if value < 0:
    return '$' + '{:0.2f}'.format((abs(value))) + ' saved'
  else:
    return '$' + '{:0.2f}'.format((abs(value))) + ' more'


# initialize input page
@app.route('/')
@app.route('/index')
@app.route('/input')
def index():
    return render_template("input.html")

# get the output page
@app.route('/output', methods = ['GET','POST'])
def output():

  # connect to SQL data base
  con = None
  con = psycopg2.connect(database = dbname, user = 'matthew', password = 'postsqlgres', host = 'localhost')
  
  # get the user inputs
  numgames = int(request.args.get('numgames'))
  numgamesimp = float(request.args.get('numgamesimp'))
  daygames = float(request.args.get('daygames'))
  daygamesimp = float(request.args.get('daygamesimp'))
  qimp = float(request.args.get('qimp'))
  dimp = float(request.args.get('dimp'))
  avgtemp = float(request.args.get('avgtemp'))
  avgtempimp = float(request.args.get('avgtempimp'))
  pricing = int(request.args.get('try'))
  pimp = float(request.args.get('pimp'))
  
  # get the maximum budget and change it to 0 if the user doesn't input a number
  maxb = request.args.get('maxb')
  try:
    maxb = float(maxb)
  except:
    maxb = 0

  
  # determine if the user wants to sort by lowest or median price
  pricecheck = 'checked'
  if pricing == 1:
    pricing = 'median'
    check = 0
    query1 = "SELECT * FROM augroadtripsupdateddaily WHERE medprice < %s" % (str(maxb))
  else:
    pricing = 'lowest'
    pricecheck = ''
    check = 1 
    query1 = "SELECT * FROM augroadtripsupdateddaily WHERE lowprice < %s" % (str(maxb))

  # return the parameters so the user can remember their input parameters
  p = dict(numgames = numgames, numgamesimp = numgamesimp, daygames = daygames, daygamesimp = daygamesimp, qimp = qimp, dimp = dimp, maxb = maxb, avgtemp = avgtemp, avgtempimp = avgtempimp, pimp = pimp, pricecheck = 1-check, pricing = pricing)
  
  # convert to celcius
  avgtemp = (avgtemp-32) * 5 / 9

  # scale the importance
  avgtempimp = (avgtempimp / 50)**3
  qimp = ((qimp - 50) / 25)**3
  pimp = int(10**(pimp / 50 + 1) / 2)
  numgamesimp = (numgamesimp / 50)**3
  daygames =  daygames / 100
  daygamesimp = (daygamesimp / 50)**3

  # get the list of desired stadiums to visit
  locs = request.args.getlist('loclist')
  locs2 = request.args.getlist('example')
  locdic = {}
  for i in locs:
    locdic[i] = 'checked'

  for i in locs2:
    locdic[i] = 'checked'
  
  # run a SQL query to get the data
  query_results1 = pd.read_sql_query(query1,con)

  # if no itineraries are found, return nothing
  if query_results1.shape[0] == 0:
    con.close()
    return render_template("outputnone.html", p = p, locdic = locdic)
 
  # set value to normalize distance of trip
  miles_norm = query_results1.total_miles.std()
  
  # set weighting function for locations data
  query_results1['locationmatch'] = 1
  
  if len(locs)>1:
    query_results1['locationmatch'] = 1-query_results1[locs].sum(axis=1)
    
  elif len(locs)==1:
    query_results1['locationmatch'] = 1-query_results1[locs] 




  # perform euclidean distance calculation
  query_results1['eucdis'] = np.sqrt(50 * numgamesimp * ((query_results1.total_games - numgames))**2 + .25 * daygamesimp * ((query_results1.percent_day_games - daygames))**2 + 5 * dimp * (query_results1.total_miles / miles_norm)**2 + .1 * abs(qimp) * ((1 - (qimp > 0) * 2) - (query_results1.game_quality_a - query_results1.game_quality_h) / query_results1.game_quality_h.std())**2 + 10 * query_results1.locationmatch + .5 * avgtempimp * ((query_results1.avg_temp - avgtemp) / query_results1.avg_temp.std())**2)
  
  # find the itineraries that are sufficiently close to the users preferences
  query_results2 = query_results1.nsmallest(pimp, columns = ['eucdis'])

  # determine the price to consider
  if check == 0:
    query_results2['price'] = query_results2.medprice
  else:
    query_results2['price'] = query_results2.lowprice

  # choose the itineraries
  query_results2 = OptimizeTrip(query_results2)
  

  # get the baseball standings
  baseballquery = "SELECT * FROM standingsupdateddaily" 
  standings = pd.read_sql_query(baseballquery,con)

  # initialize output arrays
  bigoutput = []
  tempcolors = []
  roadtripmeta = []

  # iterate through each possible itinerary
  for index, row in query_results2.iterrows():
    # convert string of game indices to a readable format
    tempvalues = row.indices.replace('[','(').replace(']',')')
    tempvalues = row.indices.replace('{','(').replace('}',')')

    # get the relevant game data
    query2 = "SELECT * FROM gamesdataupdateddaily WHERE gameindex IN %s" % tempvalues  
    query_results = pd.read_sql_query(query2,con)

    # order the games by day of the year
    query_results = query_results.nsmallest(query_results.shape[0], columns = ['dayofyear'])
    
    # initialize an array for the games
    tempgames = []
    
    # build the array of games for an itinerary
    for i in range(0,query_results.shape[0]):
      tempgames.append(dict(hometeam = query_results.iloc[i]['hometeam'], tavg = round(query_results.iloc[i]['tavg']*9/5 + 32, 1), awayteam = query_results.iloc[i]['awayteam'], date2 = (query_results.iloc[i]['date2'].day_name() + ' ' + str(query_results.iloc[i]['date2'].month) + '/' + str(query_results.iloc[i]['date2'].day) + ' at ' + str(AddHour(query_results.iloc[i]['date2'].hour-12)) + ':' + AddTens(str(query_results.iloc[i]['date2'].minute)) + ' pm'), homewin = standings.loc[standings.team == query_results.iloc[i]['hometeam']]['win'].values[0], homeloss = standings.loc[standings.team == query_results.iloc[i]['hometeam']]['loss'].values[0], awaywin = standings.loc[standings.team == query_results.iloc[i]['awayteam']]['win'].values[0], awayloss = standings.loc[standings.team == query_results.iloc[i]['awayteam']]['loss'].values[0], url = query_results.iloc[i]['url']))
    
    # add it to output array
    bigoutput += [tempgames]

    # add roadtrip meta data
    if check == 0:
      roadtripmeta.append(dict(total_games = row.total_games, total_miles = int(row.total_miles), price = row.medprice, day_value = SavingsToString(round(row.day_games_value_med, 2)), quality_value = SavingsToString(round(row.quality_value_med, 2)), loc_value = SavingsToString(round(row.location_value_med, 2)), expected_price = '{:0.2f}'.format(round(row.predicted_med, 2)), weather_value = SavingsToString(round(row.weather_value_med, 2)), findme = row.quality_value_med))
      tempcolors.append(dict(day = IndividualColor(row.day_games_value_med),quality = IndividualColor(row.quality_value_med),loc = IndividualColor(row.location_value_med),med = IndividualColor(row.medprice - row.predicted_med),weather = IndividualColor(row.weather_value_med)))
    else:
      roadtripmeta.append(dict(total_games = row.total_games, total_miles = int(row.total_miles), price = row.lowprice, day_value = SavingsToString(round(row.day_games_value_low, 2)), quality_value = SavingsToString(round(row.quality_value_low, 2)), loc_value = SavingsToString(round(row.location_value_low, 2)), expected_price = '{:0.2f}'.format(round(row.predicted_low, 2)), weather_value = SavingsToString(round(row.weather_value_low, 2)), findme = row.quality_value_med))
      tempcolors.append(dict(day = IndividualColor(row.day_games_value_low),quality = IndividualColor(row.quality_value_low),loc = IndividualColor(row.location_value_low), med = IndividualColor(row.lowprice - row.predicted_low), weather = IndividualColor(row.weather_value_low)))
  
  # close the connection to the SQL database
  con.close()
  
  # render output page
  return render_template("output.html", bigoutput = bigoutput, roadtripmeta = roadtripmeta, p = p, locdic = locdic, length = len(roadtripmeta), colors = tempcolors)


# find similar itineraries
@app.route('/outputsimilar', methods = ['GET','POST'])
def output_similar():
  # connect to SQL database
  con = None
  con = psycopg2.connect(database = dbname, user = 'matthew',password = 'postsqlgres', host = 'localhost')

  # getting metadata to identify similar itineraries
  total_miles = request.args.get('total_miles')
  quality_value = float(request.args.get('quality_value'))
  location_value = request.args.get('loc_value')
  
  # here we find similar itineraries, because we round quality values we need to use a range and thus need to treat values less than zero different
  if quality_value<0:
    query1 = "SELECT * FROM augroadtripsupdateddaily WHERE total_miles = %s AND quality_value_med BETWEEN %s AND %s" % (str(total_miles), str((quality_value * 1.01)), str((quality_value * .99)))
  else:
    query1 = "SELECT * FROM augroadtripsupdateddaily WHERE total_miles = %s AND quality_value_med BETWEEN %s AND %s" % (str(total_miles), str((quality_value * .99)), str((quality_value * 1.01)))

  # SQL query
  query_results1 = pd.read_sql_query(query1,con)

  # order the itineraries by price
  if query_results1.shape[0] > 1:
    query_results1 = query_results1.nsmallest(query_results1.shape[0], columns = ['medprice'])
  baseballquery = "SELECT * FROM standingsupdateddaily"
  
  # read in baseball standings
  standings = pd.read_sql_query(baseballquery, con)
  
  # initialize output arrays
  bigoutput = []
  roadtripmeta = []

  # iterate through all output arrays
  for index, row in query_results1.iterrows():
    # make game indexs readable
    tempvalues = row.indices.replace('[','(').replace(']',')')
    tempvalues = row.indices.replace('{','(').replace('}',')')

    # SQL query for relevant games and sorted by day of year
    query2 = "SELECT * FROM gamesdataupdateddaily WHERE gameindex IN %s" % tempvalues  
    query_results = pd.read_sql_query(query2, con)
    query_results = query_results.nsmallest(query_results.shape[0], columns = ['dayofyear'])
    
    tempgames = []
    for i in range(0, query_results.shape[0]):
      tempgames.append(dict(hometeam = query_results.iloc[i]['hometeam'], tavg = round(query_results.iloc[i]['tavg'] * 9 / 5 + 32, 1), awayteam = query_results.iloc[i]['awayteam'], date2 = (query_results.iloc[i]['date2'].day_name() + ' ' + str(query_results.iloc[i]['date2'].month) + '/' + str(query_results.iloc[i]['date2'].day) + ' at ' + str(AddHour(query_results.iloc[i]['date2'].hour - 12)) + ':' + AddTens(str(query_results.iloc[i]['date2'].minute)) + ' pm'), homewin = standings.loc[standings.team == query_results.iloc[i]['hometeam']]['win'].values[0], homeloss = standings.loc[standings.team == query_results.iloc[i]['hometeam']]['loss'].values[0], awaywin = standings.loc[standings.team == query_results.iloc[i]['awayteam']]['win'].values[0], awayloss = standings.loc[standings.team == query_results.iloc[i]['awayteam']]['loss'].values[0], url = query_results.iloc[i]['url']))
    bigoutput += [tempgames]
    roadtripmeta.append(dict(total_games = row.total_games,lowprice = row.lowprice, medprice = row.medprice, total_miles = total_miles, quality_value = quality_value))

  # close connection to database
  con.close()

  # render template
  return render_template("outputsimilar.html", bigoutput = bigoutput, roadtripmeta = roadtripmeta, length = len(roadtripmeta))