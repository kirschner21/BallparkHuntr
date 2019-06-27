from flask import render_template
from Baseball import app
from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists, create_database
import pandas as pd
import numpy as np
import psycopg2
from flask import request


user = 'matthew:postsqlgres' #add your username here (same as previous postgreSQL)                      
host = '@localhost'
dbname = 'baseball'
#db = create_engine('postgres://%s%s/%s'%(user,host,dbname))



def AddTens(minutes):
  if len(minutes) == 1:
    minutes = '0' + minutes
  return minutes

def AddHour(hour):
  if hour == 0:
    hour = 12
  return hour

def OptimizeTrip(example):
    toproadtrips = pd.DataFrame()

    while toproadtrips.shape[0] < 5 and example.shape[0] > 0:
        tempexample = example.loc[example.price == example.price.min()].nsmallest(1,columns=['relativevalue'])
        
        toproadtrips = toproadtrips.append(tempexample)

        example = example.drop(example.loc[(((example.total_miles == tempexample.total_miles.values[0]) & (example.game_importance_a == tempexample.game_importance_a.values[0])) & (example.game_quality_a == tempexample.game_quality_a.values[0]))].index, axis = 0 )
    return(toproadtrips)
      
def IndividualColor(value):

  if value<=0:
    return "green"
  else:
    return "magenta"

def SavingsToString(value):

  if value < 0:
    return '$' + '{:0.2f}'.format((abs(value))) + ' saved'
  else:
    return '$' + '{:0.2f}'.format((abs(value))) + ' more'

@app.route('/')
@app.route('/index')
def index():
    return render_template("input.html")


@app.route('/input')
def input():
    return render_template("input.html")



@app.route('/output', methods = ['GET','POST'])
def output():


  con = None
  con = psycopg2.connect(database = dbname, user = 'matthew',password='postsqlgres',host='localhost')
  

  numgames = int(request.args.get('numgames'))
  numgamesimp = (float(request.args.get('numgamesimp'))/50)**3
  daygames = float(request.args.get('daygames'))/100
  daygamesimp = (float(request.args.get('daygamesimp'))/50)**3
  qimp = float(request.args.get('qimp'))
  dimp = (float(request.args.get('dimp'))/50)**3
  maxb = request.args.get('maxb')
  try:
    maxb = float(maxb)
  except:
    maxb = 0
  avgtemp = float(request.args.get('avgtemp'))
  avgtempimp = float(request.args.get('avgtempimp'))
  pricing = int(request.args.get('try'))
  pimp = float(request.args.get('pimp'))


  locs = request.args.getlist('loclist')
  locs2 = request.args.getlist('example')
  
  pricecheck = 'checked'
  if pricing == 1:
    pricing = 'median'
    check = 0
    query1 = "SELECT * FROM augroadtripsupdateddaily WHERE medprice < %s" % (str(maxb) )
  else:
    pricing = 'lowest'
    pricecheck =''
    check = 1 
    query1 = "SELECT * FROM augroadtripsupdateddaily WHERE lowprice < %s" % (str(maxb) )

  p = dict(numgames = numgames, numgamesimp = numgamesimp**(1/3)*50, daygames = daygames*100, daygamesimp = daygamesimp**(1/3)*50, qimp = qimp, dimp = dimp**(1/3)*50, mintemp = 0, maxtemp = 0, maxb = maxb,avgtemp=avgtemp,avgtempimp = avgtempimp, pimp = pimp, pricecheck=1-check, pricing = pricing)
  qimp = ((qimp-50)/25)**3
  avgtemp = (avgtemp-32) * 5/9
  avgtempimp = (avgtempimp/50)**3
  pimp = int(10**(pimp/50+1)/2)
  locdic = {}
  for i in locs:
    locdic[i] = 'checked'

  for i in locs2:
    locdic[i] = 'checked'
  

  

  query_results1 = pd.read_sql_query(query1,con)

  if query_results1.shape[0] == 0:
    con.close()
    return render_template("outputnone.html", p = p,locdic = locdic)
 
  miles_norm = query_results1.total_miles.std()
  
  query_results1['locationmatch']=1
  
  if len(locs)>1:
    query_results1['locationmatch'] = 1-query_results1[locs].sum(axis=1)
    
  elif len(locs)==1:
    query_results1['locationmatch'] = 1-query_results1[locs] 





  query_results1['eucdis'] = np.sqrt(50 * numgamesimp * ((query_results1.total_games - numgames))**2 + .25*daygamesimp * ((query_results1.percent_day_games - daygames))**2  + 5* dimp * (query_results1.total_miles/miles_norm)**2  + .1 * abs(qimp) * ((1-(qimp > 0)*2)-(query_results1.game_quality_a -query_results1.game_quality_h)/query_results1.game_quality_h.std())**2 + 10*query_results1.locationmatch + .5*avgtempimp*((query_results1.avg_temp-avgtemp)/query_results1.avg_temp.std())**2)
  
  locs = request.args.getlist('loclist')
  locs2 = request.args.getlist('example')
  query_results2 = query_results1.nsmallest(pimp, columns = ['eucdis'])
  if check == 0:
    query_results2['relativevalue'] = query_results2.predicted_med / query_results2.medprice
    query_results2['price'] = query_results2.medprice

  else:
    query_results2['relativevalue'] = query_results2.predicted_low / query_results2.lowprice
    query_results2['price'] = query_results2.lowprice

  query_results2 = OptimizeTrip(query_results2)
  


  baseballquery = "SELECT * FROM standingsupdateddaily"
  
  standings = pd.read_sql_query(baseballquery,con)


  bigoutput = []
  tempcolors = []
  
  roadtripmeta = []

  for index, row in query_results2.iterrows():
    tempvalues = row.indices.replace('[','(').replace(']',')')
    tempvalues = row.indices.replace('{','(').replace('}',')')
    query2 = "SELECT * FROM gamesdata2 WHERE gameindex IN %s" % tempvalues  
    query2 = "SELECT * FROM gamesdataupdateddaily WHERE gameindex IN %s" % tempvalues  
    query_results = pd.read_sql_query(query2,con)
    query_results = query_results.nsmallest(query_results.shape[0], columns = ['dayofyear'])
    tempgames = []
    
    #print(query_results.shape[0])
    for i in range(0,query_results.shape[0]):
      tempgames.append(dict(hometeam=query_results.iloc[i]['hometeam'],tavg=round(query_results.iloc[i]['tavg']*9/5 + 32,1), awayteam=query_results.iloc[i]['awayteam'], date2=(query_results.iloc[i]['date2'].day_name() + ' ' + str(query_results.iloc[i]['date2'].month) + '/' + str(query_results.iloc[i]['date2'].day) + ' at ' + str(AddHour(query_results.iloc[i]['date2'].hour-12)) + ':' + AddTens(str(query_results.iloc[i]['date2'].minute)) + ' pm'), homewin = standings.loc[standings.team==query_results.iloc[i]['hometeam']]['win'].values[0], homeloss = standings.loc[standings.team==query_results.iloc[i]['hometeam']]['loss'].values[0], awaywin = standings.loc[standings.team==query_results.iloc[i]['awayteam']]['win'].values[0], awayloss = standings.loc[standings.team==query_results.iloc[i]['awayteam']]['loss'].values[0], url = query_results.iloc[i]['url']))
    bigoutput += [tempgames]
    #roadtripmeta.append(dict(total_miles = row.total_miles, lowprice = row.lowprice, medprice = row.medprice, relativevalue = round(row.eucdis,2), eucdis = row.eucdis, day_value = round(row.day_games_value_med,2),quality_value = round(row.quality_value_med,2),loc_value = round(row.location_value_med,2),expected_med = round(row.predicted_med,2),weather_value = round(row.weather_value_med,2)))
    if check == 0:
      roadtripmeta.append(dict(total_games = row.total_games, total_miles = int(row.total_miles), price = row.medprice, day_value = SavingsToString(round(row.day_games_value_med,2)),quality_value = SavingsToString(round(row.quality_value_med,2)),loc_value = SavingsToString(round(row.location_value_med,2)),expected_price = '{:0.2f}'.format(round(row.predicted_med,2)),weather_value = SavingsToString(round(row.weather_value_med,2)),findme = row.quality_value_med))
      tempcolors.append(dict(day = IndividualColor(row.day_games_value_med),quality = IndividualColor(row.quality_value_med),loc = IndividualColor(row.location_value_med),med = IndividualColor(row.medprice - row.predicted_med),weather = IndividualColor(row.weather_value_med)))
    else:
      roadtripmeta.append(dict(total_games = row.total_games, total_miles = int(row.total_miles), price = row.lowprice, day_value = SavingsToString(round(row.day_games_value_low,2)),quality_value = SavingsToString(round(row.quality_value_low,2)),loc_value = SavingsToString(round(row.location_value_low,2)),expected_price = '{:0.2f}'.format(round(row.predicted_low,2)),weather_value = SavingsToString(round(row.weather_value_low,2)),findme = row.quality_value_med))
      tempcolors.append(dict(day = IndividualColor(row.day_games_value_low),quality = IndividualColor(row.quality_value_low),loc = IndividualColor(row.location_value_low),med = IndividualColor(row.lowprice - row.predicted_low),weather = IndividualColor(row.weather_value_low)))
  con.close()
  return render_template("output.html", bigoutput = bigoutput, roadtripmeta = roadtripmeta, p = p, locdic = locdic, length = len(roadtripmeta), colors = tempcolors)



@app.route('/outputsimilar', methods = ['GET','POST'])
def output_similar():
  con = None
  con = psycopg2.connect(database = dbname, user = 'matthew',password='postsqlgres',host='localhost')


  total_miles = request.args.get('total_miles')
  quality_value = float(request.args.get('quality_value'))
  location_value = request.args.get('loc_value')
  

  if quality_value<0:
    query1 = "SELECT * FROM augroadtripsupdateddaily WHERE total_miles = %s AND quality_value_med BETWEEN %s AND %s" % (str(total_miles), str((quality_value*1.01)), str((quality_value*.99)))
  else:
    query1 = "SELECT * FROM augroadtripsupdateddaily WHERE total_miles = %s AND quality_value_med BETWEEN %s AND %s" % (str(total_miles), str((quality_value*.99)), str((quality_value**1.01)))

  query_results1 = pd.read_sql_query(query1,con)

  if query_results1.shape[0] > 1:
    query_results1 = query_results1.nsmallest(query_results1.shape[0], columns = ['medprice'])
  baseballquery = "SELECT * FROM standingsupdateddaily"
  

  standings = pd.read_sql_query(baseballquery,con)
  bigoutput = []
  example1 = 0

  roadtripmeta = []

  for index, row in query_results1.iterrows():
    tempvalues = row.indices.replace('[','(').replace(']',')')
    tempvalues = row.indices.replace('{','(').replace('}',')')
    query2 = "SELECT * FROM gamesdataupdateddaily WHERE gameindex IN %s" % tempvalues  
    query_results = pd.read_sql_query(query2,con)
    query_results = query_results.nsmallest(query_results.shape[0], columns = ['dayofyear'])
    tempgames = []
    for i in range(0,query_results.shape[0]):
      tempgames.append(dict(hometeam=query_results.iloc[i]['hometeam'],tavg=round(query_results.iloc[i]['tavg']*9/5 + 32,1), awayteam=query_results.iloc[i]['awayteam'], date2=(query_results.iloc[i]['date2'].day_name() + ' ' + str(query_results.iloc[i]['date2'].month) + '/' + str(query_results.iloc[i]['date2'].day) + ' at ' + str(AddHour(query_results.iloc[i]['date2'].hour-12)) + ':' + AddTens(str(query_results.iloc[i]['date2'].minute)) + ' pm'), homewin = standings.loc[standings.team==query_results.iloc[i]['hometeam']]['win'].values[0], homeloss = standings.loc[standings.team==query_results.iloc[i]['hometeam']]['loss'].values[0], awaywin = standings.loc[standings.team==query_results.iloc[i]['awayteam']]['win'].values[0], awayloss = standings.loc[standings.team==query_results.iloc[i]['awayteam']]['loss'].values[0], url = query_results.iloc[i]['url']))
    bigoutput += [tempgames]
    roadtripmeta.append(dict(total_games = row.total_games,lowprice = row.lowprice, medprice = row.medprice, total_miles = total_miles, quality_value = quality_value))


  con.close()
  return render_template("outputsimilar.html", example1 = example1, bigoutput = bigoutput, roadtripmeta = roadtripmeta, length = len(roadtripmeta))