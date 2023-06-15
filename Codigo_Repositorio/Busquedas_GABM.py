# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
import time
import json
import urllib.parse
from pymongo import MongoClient
from tqdm import tqdm
import requests
from mongoDB import *
from tokens_access import *
import sys
from procesamiento1 import *
import warnings # Para las advertencias
warnings.filterwarnings("ignore")




url = 'https://api.twitter.com/2/users/by?usernames=twitterdev,twitterapi,adsapi&user.fields=created_at&expansions=pinned_tweet_id&tweet.fields=author_id,created_at'


def extract_timeline(username, count, fromdate, todate):
    
    headers1 = {'authorization': 'XXXX',
     'content-type': 'application/json'} # XXXX - # Por motivos de seguridad se eliminaron estas direcciones / claves / puertos

    header = {
         'authorization': headers1['authorization'],
         'content-type': 'application/json'
     }
    
    url = 'https://api.twitter.com/1.1/statuses/user_timeline.json?screen_name='+username+'&count='+str(count)+'&until='+todate+'&since='+fromdate
    
    
    response = requests.get(url = url, headers=header).json()
    df = pd.DataFrame(response)
    
    return df

def extract_timeline(username, count):
    
    headers1 = {'authorization': 'XXXX',
     'content-type': 'application/json'} # XXXX - # Por motivos de seguridad se eliminaron estas direcciones / claves / puertos

    header = {
         'authorization': headers1['authorization'],
         'content-type': 'application/json'
     }
    
    url = 'https://api.twitter.com/1.1/statuses/user_timeline.json?screen_name='+username+'&count='+str(count)
    
    
    response = requests.get(url = url, headers=header).json()
    df = pd.DataFrame(response)
    
    return df


def tipo_publicacion(df, clave, directorio):
    
    import plotly.express as px
    import plotly.graph_objects as go
    from plotly.offline import download_plotlyjs, init_notebook_mode,  plot
    import plotly.graph_objects as go
    from plotly.subplots import make_subplots
    import pandas as pd
        
    retweets = len(df[(df['retweeted_status'] != 0) & (df['retweeted_status'].notnull())][['retweeted_status']])
    replies = len(df[(df['in_reply_to_screen_name'] != 0) & (df['in_reply_to_screen_name'].notnull())])

    try: 
        quotes = len(df[(df['quoted_status'].notnull()) & (df['quoted_status'] != 0)])

    except:
        
        quotes = 0


    tweets =   len(df) - retweets - replies - quotes

    busqueda_p = pd.DataFrame({'tipo':['Retweets','Tweets','Respuestas', 'Citas'],
             'cantidad':[retweets,tweets,replies, quotes]})

    labels = busqueda_p.tipo

    colors = ['#2dabd2', '#93d704','#f05707','#26367e']
    colors = ['#ea5913', '#2facd3', '#93d704','#eba710']
    

    # Create subplots: use 'domain' type for Pie subplot
    fig = go.Figure(go.Pie(labels='<i>'+labels+'</i>', values=busqueda_p['cantidad'], name="Tipo publicacion"))

    # Use `hole` to create a donut-like pie chart
    fig.update_traces(hole=.4, hoverinfo="label+percent+name", 
                  marker=dict(colors=colors),
                  textposition="auto",
                  texttemplate="%{percent:.1%f}",
                  textfont= dict(color='#FFFFFF', 
                                 family="Arial",
                                 size=15),
                 )


    fig.update_layout(
    title_text="",
    titlefont= dict(
        family="Raleway",
        size=22
        ),
    font= dict(
        family="Arial",
        size=18,
        color = 'white'
        ),
    legend=dict(
        font=dict(
            family = 'Raleway',
            size = 18,
            color = 'white')),
    # Add annotations in the center of the donut pies.
    annotations=[dict(text='Frecuencia', x=0.50, y=0.5, font_size=15, showarrow=False)],
    uniformtext_minsize=8, uniformtext_mode='show')

    fig.layout.plot_bgcolor = 'rgba(0,0,0,0)'
    fig.layout.paper_bgcolor = 'rgba(0,0,0,0)'
    #     fig.layout.plot_bgcolor = '#2b2d4d'
    #     fig.layout.paper_bgcolor = '#2b2d4d'
    fig.update_traces(textfont_color='white')
    fig.write_image(directorio + "/" + clave +"_DistribuicionPorcentualPublicaciones.png",width=1230, height=660)
  
    return fig


def regresiva (tiempo):
    
    
    for remaining in range(tiempo - 1, 0, -1):
        sys.stdout.write("\r")
        sys.stdout.write("{:2d} segundos para reiniciar las consultas.".format(remaining))
        sys.stdout.flush()
        time.sleep(1)


def search_tweets(query, fromDate = "202104250000", toDate = "202105102359",n = 500, next_token = None):

    
    if next_token == None:
    
        header = {
            'authorization': headers['authorization'],
            'content-type': 'application/json'
        }
    
        data = '{"query":"'+query+ '","maxResults":"'+str(n)+'","fromDate":"'+fromDate+'","toDate":"'+ toDate+ '"}'
    
        url = 'https://api.twitter.com/1.1/tweets/search/30day/DevNA.json'
    
        response = requests.post(url = url, headers=header, data=data).json()
    
        return(response)

    else:
        
        header = {
            'authorization': headers1['authorization'],
            'content-type': 'application/json'
        }
    
        data = '{"query":"'+query+ '","maxResults":"'+str(n)+'","fromDate":"'+fromDate+'","toDate":"'+ toDate+ '","'+'next":"'+next_token+'"}'
    
        url = 'https://api.twitter.com/1.1/tweets/search/30day/DevNA.json'
    
        response = requests.post(url = url, headers=header, data=data).json()
    
        return(response)
        

def search_followers(screen_name, cursor = None):

    
    if cursor == None:
    

        url = "https://api.twitter.com/1.1/followers/ids.json?" + "cursor=-1"+ "&screen_name="+ screen_name
    
        response = requests.get(url = url, headers=headers)
    
        return(response)

    else:
        

    
        url = "https://api.twitter.com/1.1/followers/ids.json?" + "cursor=" + str(cursor) + "&screen_name="+ screen_name
    
        response = requests.get(url = url, headers=headers)
    
        return(response)
  
 ########################################################################################################


def complete(l):
    
    lr = l["results"]
    
    for i in range(len(lr)):
    
        j = 0
    
    
        try:
            
            lr[i]['text'] = lr[i]["extended_tweet"]["full_text"]
        except:
            j = j+1 
    
        try:
            
            if j == 1:
            
                lr[i]['text'] = "rt " + lr[i]["retweeted_status"]["extended_tweet"]["full_text"]
        except:
            j = j + 1
            
            
        try:
            if j == 2:
                lr[i]['text'] = "rt " + lr[i]["retweeted_status"]["text"]
        except:
            pass
        
    return(lr)
        



########################################################### Funciones para guardar timeline

def timeline(database, fromDate , toDate, username, clave, token = None):
    '''
    

    Parameters
    ----------
    database : STRING
        NOMBRE DE BASE DE DATOS.
    fromDate : STRING
        FECHA DE INICIO DE LA CONSULTA. EJEMPLO --> '20210601' FORMATO AAAMMDD
    toDate : STRING
        FECHA FINAL DE LA CONSULTA. EJEMPLO --> '20210601' FORMATO AAAMMDD
    names : LISTA
        LISTA DE USUARIOS A CONSULTAR EL TIMELINE DE TWITTER. EJEMPLO names= [a, b ,c,d]".

    Returns
    -------
    None.

    '''

    
    client = auth_mongo('XXXX', 'XXXX', database)  # XXXX - # Por motivos de seguridad se eliminaron estas direcciones / claves / puertos
    username = list(username.split(","))

    query = ""
    for i in tqdm(username):
        query = query + "from:" + i + " OR "
    query =  query[:len(query) - 4]
    
    
    # El query
    
    l = l = search_tweets(query = query, fromDate = fromDate, toDate = toDate,n = 500, next_token = token)
    l["results"] = complete(l)
    
    
    # insertando        
    
    for i in tqdm(range(len(l["results"]))):
    
        l["results"][i]['screen_name'] = l["results"][i]["user"]['screen_name']
        
        df = pd.DataFrame(l['results'])
        df = procesamiento(df)
    insert_mongo(client, database, 't_'+clave+'_feed', df)       
        
    req = 1 
    while "next" in l.keys(): 
        fic = open("token.txt", "w")      
        fic.write(l["next"])
        fic.close()
        
        # El query
        print(l["next"])
        l = l = search_tweets(query = query, fromDate = fromDate, toDate = toDate,n = 500, next_token = l["next"])
        l["results"] = complete(l)
        
        # insertando
        
        for i in tqdm(range(len(l["results"]))):
        
            l["results"][i]['screen_name'] = l["results"][i]["user"]['screen_name']
            df = pd.DataFrame(l['results'])
            df = procesamiento(df)
        insert_mongo(client, database, 't_'+clave+'_feed', df) 
        
        req = req + 1
        print(req)
        

############################### Funcion para guardar una busqueda


def BusquedaSave(database, query, fromDate, toDate, clave , token=None):
    
    client = auth_mongo('XXXX', 'XXXX', database)    # XXXX - # Por motivos de seguridad se eliminaron estas direcciones / claves / puertos
    l = search_tweets(query = query, fromDate = fromDate, toDate = toDate,n = 500, next_token = token)
    l["results"] = complete(l)
    df = pd.DataFrame(l['results'])
    df = Procesamiento_busqueda(df, clave)
    # insertando        
    insert_mongo(client, database, 't_'+clave+'_busqueda', df) 
        
    req = 1   
    print(req)
    regresiva(2)
        
        
    while "next" in l.keys():
        

        # El query
        print(l["next"])
        
        fic = open("token.txt", "w")
        fic.write(l["next"])
        fic.close()
        
        
        l = search_tweets(query = query, fromDate = fromDate, toDate = toDate,n = 500, next_token = l["next"])
        l["results"] = complete(l)
        df = pd.DataFrame(l['results'])
        df = Procesamiento_busqueda(df, clave)
        
        # insertando
        insert_mongo(client, database, 't_'+clave+'_busqueda', df)       
        
        req = req + 1 
        print(req)
        regresiva(3)
        
        if req == 2000:
            break
        print("Loop Terminado")
    
        
        

        
###############################################################################################################        

def FollowersIdSave(database , coleccion, screen_name = "nombre", cursor=None):
    '''
    

    Parameters
    ----------
    database : STRING
        NOMBRE DE LA BASE DE DATOS.
    coleccion : STRING
        NOMBRE DE LA COLECCION.
    screen_name : STRING.
        NOMBRE DE LA CUENTA A CONSULTAR. EJEMPLO "GoyoGraterol".
    cursor : TYPE, optional
        DESCRIPTION. The default is None.

    Returns
    -------
    None.

    '''


    cliente = auth_mongo('XXXX', 'XXXX', database) # XXXX - # Por motivos de seguridad se eliminaron estas direcciones / claves / puertos
    
    # El query
    l = search_followers(screen_name, cursor = None).json()

    # insertando        
    df1 = pd.DataFrame(l["ids"])
    df1.columns = ["ids"]
    insert_mongo(client = cliente, database = database, collection_name  = coleccion, df = df1 )
    req = 1  
    print(req)
    while l["next_cursor"] != 0:
        

        # El query
        print(l["next_cursor"])
        
        fic = open("token.txt", "w")
        fic.write(l["next_cursor_str"])
        fic.close()
        
        
        l = search_followers(screen_name, cursor = l["next_cursor"]).json()
        df1 = pd.DataFrame(l["ids"])
        df1.columns = ["ids"]

        # insertando
        insert_mongo(client = cliente, database = database, collection_name  = coleccion, df = df1 )     
        
        
        
        req = req + 1 
        if (req % 15) == 0:
            
            regresiva(960)
        
        print(req)
        

#######################################################################################################################################
def search_followers_des(ids):
    
    str1 = ','.join(ids) 
 
    url = 'https://api.twitter.com/1.1/users/lookup.json?user_id=' +  str1

    response = requests.get(url = url, headers=headers).json()

    return(response)
  
        
###########################################################################################################################################   
        
        
    
  
def FollowersDesSave(database, coleccion, coleccion_save):
    '''
    

    Parameters
    ----------
    database : STRING
        NOMBRE DE BASE DE DATOS.
    coleccion : STRING
        NOMBRE DE LA COLECCION A CONSULTAR.
    coleccion_save : STRING
        NOMBRE DE LA COLECCION A GUARDAR.

    Returns
    -------
    None.

    '''

    
    cliente = auth_mongo('XXXX', 'XXXX', database) # XXXX - # Por motivos de seguridad se eliminaron estas direcciones / claves / puertos
    
    
    df = extract_mongo(cliente, database, coleccion)
    
    df["ids"] = df["ids"].astype(str)
    
    ids = df["ids"].to_list()
    
 

    lista = ids     
    
    if int(len(lista)%100) == 0:
        for i in tqdm(range(0, int(len(lista)/100))):
    
            if i % 300 == 0 and i !=0:
                regresiva(960)     

            lo = search_followers_des(lista[i*100:(i+1)*100])  
            insert_dict_to_mongo(client = cliente, database = database, collection_name = coleccion_save, data_dict = lo)
    elif int(len(lista)) > 100:
        
        for i in tqdm(range(0, int(len(lista) // 100))):
            if i % 300 == 0 and i !=0:
                regresiva(960)
            lo = search_followers_des(lista[i*100:(i+1)*100])
            insert_dict_to_mongo(client = cliente, database = database, collection_name = coleccion_save, data_dict = lo) 
            
        lo = search_followers_des(lista[(i+1)*100:])
        
        insert_dict_to_mongo(client = cliente, database = database, collection_name = coleccion_save, data_dict = lo) 
    else:
        lo = search_followers_des(lista)
    
        insert_dict_to_mongo(client = cliente, database = database, collection_name = coleccion_save, data_dict = lo)         

        
        
        
def search_followers_des1(ids):
    
    str1 = ','.join(ids) 
 
    url = 'https://api.twitter.com/1.1/users/lookup.json?screen_name=' +  str1

    response = requests.get(url = url, headers=headers).json()

    return(response)
  
        
###########################################################################################################################################   
        
        
    
  
def FollowersDesSave_for_username(database, coleccion, coleccion_save):
    '''
    

    Parameters
    ----------
    database : STRING
        NOMBRE DE BASE DE DATOS.
    coleccion : STRING
        NOMBRE DE LA COLECCION A CONSULTAR.
    coleccion_save : STRING
        NOMBRE DE LA COLECCION A GUARDAR.

    Returns
    -------
    None.

    '''

    
    cliente = auth_mongo('XXXX', 'XXXX', database) # XXXX - # Por motivos de seguridad se eliminaron estas direcciones / claves / puertos
    
    
    df = extract_mongo(cliente, database, coleccion)
    
    df["username"] = df["username"].astype(str)
    
    ids = df["username"].to_list()
    
 

    lista = ids     
    
    if int(len(lista)%100) == 0:
        for i in tqdm(range(0, int(len(lista)/100))):
    
            if i % 300 == 0 and i !=0:
                regresiva(960)     

            lo = search_followers_des1(lista[i*100:(i+1)*100])  
            insert_dict_to_mongo(client = cliente, database = database, collection_name = coleccion_save, data_dict = lo)
    elif int(len(lista)) > 100:
        
        for i in tqdm(range(0, int(len(lista) // 100))):
            if i % 300 == 0 and i !=0:
                regresiva(960)
            lo = search_followers_des1(lista[i*100:(i+1)*100])
            insert_dict_to_mongo(client = cliente, database = database, collection_name = coleccion_save, data_dict = lo) 
            
        lo = search_followers_des1(lista[(i+1)*100:])
        
        insert_dict_to_mongo(client = cliente, database = database, collection_name = coleccion_save, data_dict = lo) 
    else:
        lo = search_followers_des1(lista)
    
        insert_dict_to_mongo(client = cliente, database = database, collection_name = coleccion_save, data_dict = lo)   
        
        
