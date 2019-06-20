from flask import render_template
from Baseball import app
from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists, create_database
import pandas as pd
import numpy as np
import psycopg2
from flask import request
from a_Model import ModelIt
from UsefulFunctions import ConvertSQLtoIndices

user = 'matthew:postsqlgres' #add your username here (same as previous postgreSQL)                      
host = '@localhost'
dbname = 'baseball'
db = create_engine('postgres://%s%s/%s'%(user,host,dbname))
con = None
con = psycopg2.connect(database = dbname, user = 'matthew',password='postsqlgres',host='localhost')


@app.route('/')
@app.route('/index')
def index():
    return render_template("index.html",
       title = 'Home', user = { 'nickname': 'Miguel' },
       )

@app.route('/db')
def birth_page():
    sql_query = """                                                             
                SELECT * FROM birth_data_table WHERE delivery_method='Cesarean'\
;                                                                               
                """
    query_results = pd.read_sql_query(sql_query,con)
    births = ""
    print (query_results)
    for i in range(0,10):
        births += query_results.iloc[i]['birth_month']
        births += "<br>"
    return births

@app.route('/db_fancy')
def cesareans_page_fancy():
    sql_query = """
               SELECT index, attendant, birth_month FROM birth_data_table WHERE delivery_method='Cesarean';
                """
    query_results=pd.read_sql_query(sql_query,con)
    births = []
    for i in range(0,query_results.shape[0]):
        births.append(dict(index=query_results.iloc[i]['index'], attendant=query_results.iloc[i]['attendant'], birth_month=query_results.iloc[i]['birth_month']))
    return render_template('cesareans.html',births=births)

@app.route('/input')
def cesareans_input():
    return render_template("input.html")

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
        tempexample = example.loc[example.eucdis == example.eucdis.min()].nlargest(1,columns=['eucdis'])
        
        toproadtrips = toproadtrips.append(tempexample)

        example = example.drop(example.loc[(((example.total_miles == tempexample.total_miles.values[0]) & (example.game_importance_a == tempexample.game_importance_a.values[0])) & (example.game_quality_a == tempexample.game_quality_a.values[0]))].index, axis = 0 )
    return(toproadtrips)
      
def IndividualColor(value):

  if value<=0:
    return "green"
  else:
    return "red"

def SavingsToString(value):

  if value < 0:
    return '$ ' + str(abs(value)) + ' saved'
  else:
    return '$ ' + str(abs(value)) + ' more'


@app.route('/output', methods = ['GET','POST'])
def cesareans_output():


  

  numgames = int(request.args.get('numgames'))
  numgamesimp = (float(request.args.get('numgamesimp'))/50)**3
  daygames = float(request.args.get('daygames'))/100
  daygamesimp = (float(request.args.get('daygamesimp'))/50)**3
  qimp = float(request.args.get('qimp'))
  dimp = (float(request.args.get('dimp'))/50)**3
  #mintemp = float(request.args.get('mintemp'))
  #maxtemp = float(request.args.get('maxtemp'))
  maxb = float(request.args.get('maxb'))
  avgtemp = float(request.args.get('avgtemp'))
  avgtempimp = float(request.args.get('avgtempimp'))
  pricing = request.args.get('try')
  pimp = float(request.args.get('pimp'))


  #print('here is the value')
  #print(request.args.getlist('loclist'))
  locs = request.args.getlist('loclist')
  locs2 = request.args.getlist('example')
  #print(len(locs))
  #print(((qimp**(1/3))*25)+50)
  p = dict(numgames = numgames, numgamesimp = numgamesimp**(1/3)*50, daygames = daygames*100, daygamesimp = daygamesimp**(1/3)*50, qimp = qimp, dimp = dimp**(1/3)*50, mintemp = 0, maxtemp = 0, maxb = maxb,avgtemp=avgtemp,avgtempimp = avgtempimp, pimp = pimp)
  qimp = ((qimp-50)/25)**3
  avgtemp = (avgtemp-32) * 5/9
  avgtempimp = (avgtempimp/50)**3

  
  query1 = "SELECT * FROM julyroadtrips"
  #query1 = "SELECT * FROM julyroadtrips3 WHERE min_temp > %s AND max_temp < %s AND lowprice < %s" % (str(mintemp), str(maxtemp), str(maxb) )
  query1 = "SELECT * FROM julyroadtrips3 WHERE lowprice < %s" % (str(maxb) )

  query1 = "SELECT * FROM augroadtripsupdateddaily WHERE lowprice < %s" % (str(maxb) )


  query_results1 = pd.read_sql_query(query1,con)
  #days_norm = query_results1.total_days.max() - query_results1.total_days.min()
  #games_norm = query_results1.total_games.max() - query_results1.total_games.min()
  miles_norm = query_results1.total_miles.std()
  #query_results1['game_quality_norm'] = (query_results1.game_quality.max() - query_results1.game_quality) / (query_results1.game_quality.max() - query_results1.game_quality.min()) 
  #query_results1['game_importance_norm'] = (query_results1.game_importance - query_results1.game_importance.min()) / (query_results1.game_importance.max() - query_results1.game_importance.min()) 
  query_results1['locationmatch']=1
  
  if len(locs)>1:
    query_results1['locationmatch'] = 1-query_results1[locs].sum(axis=1)
    
  elif len(locs)==1:
    query_results1['locationmatch'] = 1-query_results1[locs] 

  locdic = {}
  for i in locs:
    locdic[i] = 'checked'

  for i in locs2:
    locdic[i] = 'checked'



  query_results1['eucdis'] = np.sqrt(10 * numgamesimp * ((query_results1.total_games - numgames))**2 + .25*daygamesimp * ((query_results1.percent_day_games - daygames))**2  + 5* dimp * (query_results1.total_miles/miles_norm)**2  + .1 * abs(qimp) * ((1-(qimp > 0)*2)-(query_results1.game_quality_a -query_results1.game_quality_h)/query_results1.game_quality_h.std())**2 + 100*query_results1.locationmatch + .5*avgtempimp*((query_results1.avg_temp-avgtemp)/query_results1.avg_temp.std())**2)
  #query_results1['eucdis'] = np.sqrt( numgamesimp * ((query_results1.total_games - numgames)/games_norm)**2 + daygamesimp * ((query_results1.total_days - daygames)/days_norm)**2  + dimp * (query_results1.total_miles/miles_norm)**2  + qimp * (query_results1.game_quality_norm)**2 + gimp * (quer/y_results1.game_importance_norm)**2 )


  query_results2 = query_results1.nsmallest(50, columns = ['eucdis'])
  
  query_results2['relativevalue'] = query_results2.predicted_med / query_results2.medprice
  #query_results2 = query_results2.nsmallest(100, columns = ['relativevalue'])
  #print(query_results2.relativevalue)
  query_results2 = OptimizeTrip(query_results2)
  


  #tempvalues = query_results2.iloc[0].indices.replace('[','(').replace(']',')')
  #query2 = "SELECT hometeam, awayteam, date2, dayofyear FROM gamesdata WHERE gameindex IN %s" % tempvalues
  #query2 = "SELECT * FROM gamesdata2 WHERE gameindex IN %s" % tempvalues
  #print('hello world')
  #query_results = pd.read_sql_query(query2,con)
  #query_results = query_results.nsmallest(query_results.shape[0], columns = ['dayofyear'])
  #print(query_results)
  #births = []

  baseballquery = "SELECT * FROM standings"
  baseballquery = "SELECT * FROM standingsupdateddaily"
  #print('hello world')
  standings = pd.read_sql_query(baseballquery,con)


  bigoutput = []
  tempcolors = []
  #print(query_results1.shape[0])
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
      tempgames.append(dict(hometeam=query_results.iloc[i]['hometeam'],tavg=round(query_results.iloc[i]['tavg']*9/5 + 32,2), awayteam=query_results.iloc[i]['awayteam'], date2=(query_results.iloc[i]['date2'].weekday_name + ' ' + str(query_results.iloc[i]['date2'].month) + '/' + str(query_results.iloc[i]['date2'].day) + ' at ' + str(AddHour(query_results.iloc[i]['date2'].hour-12)) + ':' + AddTens(str(query_results.iloc[i]['date2'].minute)) + ' pm'), homewin = standings.loc[standings.team==query_results.iloc[i]['hometeam']]['win'].values[0], homeloss = standings.loc[standings.team==query_results.iloc[i]['hometeam']]['loss'].values[0], awaywin = standings.loc[standings.team==query_results.iloc[i]['awayteam']]['win'].values[0], awayloss = standings.loc[standings.team==query_results.iloc[i]['awayteam']]['loss'].values[0], url = query_results.iloc[i]['url']))
    bigoutput += [tempgames]
    #roadtripmeta.append(dict(total_miles = row.total_miles, lowprice = row.lowprice, medprice = row.medprice, relativevalue = round(row.eucdis,2), eucdis = row.eucdis, day_value = round(row.day_games_value_med,2),quality_value = round(row.quality_value_med,2),loc_value = round(row.location_value_med,2),expected_med = round(row.predicted_med,2),weather_value = round(row.weather_value_med,2)))
    roadtripmeta.append(dict(total_miles = row.total_miles, lowprice = row.lowprice, medprice = row.medprice, relativevalue = round(row.eucdis,2), eucdis = row.eucdis, day_value = SavingsToString(round(row.day_games_value_med,2)),quality_value = SavingsToString(round(row.quality_value_med,2)),loc_value = SavingsToString(round(row.location_value_med,2)),expected_med = round(row.predicted_med,2),weather_value = SavingsToString(round(row.weather_value_med,2))))
    tempcolors.append(dict(day = IndividualColor(row.day_games_value_med),quality = IndividualColor(row.quality_value_med),loc = IndividualColor(row.location_value_med),med = IndividualColor(row.medprice - row.predicted_med),weather = IndividualColor(row.weather_value_med)))
    #print('home: ' + str(row.game_quality_h))
    #print('away: ' + str(row.game_quality_a))
    #print(.1 * abs(qimp) * ((1-(qimp > 0)*2)-(row.game_quality_a -row.game_quality_h)/query_results1.game_quality_h.std())**2)
  #request.form['birth_day']
#  for i in range(0,query_results.shape[0]):
#    births.append(dict(hometeam=query_results.iloc[i]['hometeam'],tavg=query_results.iloc[i]['tavg'], awayteam=query_results.iloc[i]['awayteam'], date2=query_results.iloc[i]['date2'], homewin = standings.loc[standings.team==query_results.iloc[i]['hometeam']]['win'].values[0], homeloss = standings.loc[standings.team==query_results.iloc[i]['hometeam']]['loss'].values[0], awaywin = standings.loc[standings.team==query_results.iloc[i]['awayteam']]['win'].values[0], awayloss = standings.loc[standings.team==query_results.iloc[i]['awayteam']]['loss'].values[0]))


#  tempvalues = query_results2.iloc[1].indices.replace('[','(').replace(']',')')

#  query2 = "SELECT * FROM gamesdata2 WHERE gameindex IN %s" % tempvalues
  #print('hello world')
#  query_results = pd.read_sql_query(query2,con)
#  query_results = query_results.nsmallest(query_results.shape[0], columns = ['dayofyear'])
  #print(query_results)
 # births2 = []
 # birth_day = 1
 # the_result = 2

  #request.form['birth_day']
 # for i in range(0,query_results.shape[0]):
 #   births2.append(dict(hometeam=query_results.iloc[i]['hometeam'],tavg=query_results.iloc[i]['tavg'], awayteam=query_results.iloc[i]['awayteam'], date2=query_results.iloc[i]['date2'], homewin = standings.loc[standings.team==query_results.iloc[i]['hometeam']]['win'].values[0], homeloss = standings.loc[standings.team==query_results.iloc[i]['hometeam']]['loss'].values[0], awaywin = standings.loc[standings.team==query_results.iloc[i]['awayteam']]['win'].values[0], awayloss = standings.loc[standings.team==query_results.iloc[i]['awayteam']]['loss'].values[0]))
    
 # births3 = []
  
 # tempvalues = query_results2.iloc[2].indices.replace('[','(').replace(']',')')

  #query2 = "SELECT * FROM gamesdata2 WHERE gameindex IN %s" % tempvalues
  #print('hello world')
  #query_results = pd.read_sql_query(query2,con)
  #query_results = query_results.nsmallest(query_results.shape[0], columns = ['dayofyear'])


  #request.form['birth_day']
  #for i in range(0,query_results.shape[0]):
  #  births3.append(dict(hometeam=query_results.iloc[i]['hometeam'],tavg=query_results.iloc[i]['tavg'], awayteam=query_results.iloc[i]['awayteam'], date2=query_results.iloc[i]['date2'], homewin = standings.loc[standings.team==query_results.iloc[i]['hometeam']]['win'].values[0], homeloss = standings.loc[standings.team==query_results.iloc[i]['hometeam']]['loss'].values[0], awaywin = standings.loc[standings.team==query_results.iloc[i]['awayteam']]['win'].values[0], awayloss = standings.loc[standings.team==query_results.iloc[i]['awayteam']]['loss'].values[0]))
  
  #print(tempvalues)
  #print(births2)
 # roadtripmeta1 = dict(triplength = query_results2.iloc[0].total_miles, lowprice = query_results2.iloc[0].lowprice, medprice = query_results2.iloc[0].medprice, relativevalue = round(query_results2.iloc[0].eucdis,2), eucdis = query_results2.iloc[0].eucdis, day_value = round(query_results2.iloc[0].day_games_value_med,2),quality_value = round(query_results2.iloc[0].quality_value_med,2),loc_value = round(query_results2.iloc[0].location_value_med,2))
        
 # roadtripmeta2 = dict(triplength = query_results2.iloc[1].total_miles, lowprice = query_results2.iloc[1].lowprice, medprice = query_results2.iloc[1].medprice, relativevalue = round(query_results2.iloc[1].eucdis,2), eucdis = query_results2.iloc[1].eucdis, day_value = round(query_results2.iloc[1].day_games_value_med,2),quality_value = round(query_results2.iloc[1].quality_value_med,2),loc_value = round(query_results2.iloc[1].location_value_med,2))

#  roadtripmeta3 = dict(triplength = query_results2.iloc[2].total_miles, lowprice = query_results2.iloc[2].lowprice, medprice = query_results2.iloc[2].medprice, relativevalue = round(query_results2.iloc[2].eucdis,2), eucdis = query_results2.iloc[2].eucdis, day_value = round(query_results2.iloc[2].day_games_value_med,2),quality_value = round(query_results2.iloc[2].quality_value_med,2),loc_value = round(query_results2.iloc[2].location_value_med,2),quality_value_true = query_results2.iloc[2].quality_value_med,loc_value_true = query_results2.iloc[2].location_value_med)
 
  #return render_template("output.html", births = births,  births2 = births2, births3 = births3, roadtripmeta1 = roadtripmeta1, roadtripmeta2 = roadtripmeta2, roadtripmeta3 = roadtripmeta3, p = p, loclists = locs, amicrazy = 'checked', locdic = locdic)
  return render_template("output.html", bigoutput = bigoutput, roadtripmeta = roadtripmeta, p = p, locdic = locdic, length = len(roadtripmeta), colors = tempcolors)



@app.route('/outputsimilar', methods = ['GET','POST'])
def output_similar():


  total_miles = request.args.get('total_miles')
  quality_value = float(request.args.get('quality_value'))
  location_value = request.args.get('loc_value')
  #print(quality_value*.9)
  
  #query1 = "SELECT * FROM julyroadtrips3 WHERE total_miles = %s AND quality_value_med = %s AND location_value_med = %s" % (str(total_miles), str(quality_value), str(location_value))
  if quality_value<0:
    query1 = "SELECT * FROM julyroadtrips3 WHERE total_miles = %s AND quality_value_med BETWEEN %s AND %s" % (str(total_miles), str((quality_value*1.01)), str((quality_value*.99)))
  else:
    query1 = "SELECT * FROM julyroadtrips3 WHERE total_miles = %s AND quality_value_med BETWEEN %s AND %s" % (str(total_miles), str((quality_value*.99)), str((quality_value**1.01)))


  if quality_value<0:
    query1 = "SELECT * FROM augroadtripsupdateddaily WHERE total_miles = %s AND quality_value_med BETWEEN %s AND %s" % (str(total_miles), str((quality_value*1.01)), str((quality_value*.99)))
  else:
    query1 = "SELECT * FROM augroadtripsupdateddaily WHERE total_miles = %s AND quality_value_med BETWEEN %s AND %s" % (str(total_miles), str((quality_value*.99)), str((quality_value**1.01)))
  #query1 = "SELECT * FROM julyroadtrips3 WHERE total_miles = %s AND quality_value_med BETWEEN %s AND %s" % (str(total_miles), str(-27), str(-22))

  query_results1 = pd.read_sql_query(query1,con)

  print(query1)
  if query_results1.shape[0] > 1:
    query_results1 = query_results1.nsmallest(query_results1.shape[0], columns = ['medprice'])
  baseballquery = "SELECT * FROM standings"
  baseballquery = "SELECT * FROM standingsupdateddaily"
  

  standings = pd.read_sql_query(baseballquery,con)
  bigoutput = []
  example1 = 0

  print(query_results1.shape[0])
  roadtripmeta = []

  for index, row in query_results1.iterrows():
    tempvalues = row.indices.replace('[','(').replace(']',')')
    tempvalues = row.indices.replace('{','(').replace('}',')')
    query2 = "SELECT * FROM gamesdata2 WHERE gameindex IN %s" % tempvalues  
    query2 = "SELECT * FROM gamesdataupdateddaily WHERE gameindex IN %s" % tempvalues  
    query_results = pd.read_sql_query(query2,con)
    query_results = query_results.nsmallest(query_results.shape[0], columns = ['dayofyear'])
    tempgames = []
    #print(query_results.shape[0])
    for i in range(0,query_results.shape[0]):
      tempgames.append(dict(hometeam=query_results.iloc[i]['hometeam'],tavg=query_results.iloc[i]['tavg'], awayteam=query_results.iloc[i]['awayteam'], date2=(query_results.iloc[i]['date2'].weekday_name + ' ' + str(query_results.iloc[i]['date2'].month) + '/' + str(query_results.iloc[i]['date2'].day) + ' at ' + str(query_results.iloc[i]['date2'].hour-12) + ':' + AddTens(str(query_results.iloc[i]['date2'].minute)) + ' pm'), homewin = standings.loc[standings.team==query_results.iloc[i]['hometeam']]['win'].values[0], homeloss = standings.loc[standings.team==query_results.iloc[i]['hometeam']]['loss'].values[0], awaywin = standings.loc[standings.team==query_results.iloc[i]['awayteam']]['win'].values[0], awayloss = standings.loc[standings.team==query_results.iloc[i]['awayteam']]['loss'].values[0]))
    bigoutput += [tempgames]
    roadtripmeta.append(dict(lowprice = row.lowprice, medprice = row.medprice, total_miles = total_miles, quality_value = quality_value))

    #print(tempgames)

 
  return render_template("outputsimilar.html", example1 = example1, bigoutput = bigoutput, roadtripmeta = roadtripmeta, length = len(roadtripmeta))