#!/usr/bin/env python
# coding: utf-8

# # PASA
# 
# - toc: true 
# - badges: false
# - comments: true
# - categories: [jupyter]
# - image: images/chart-preview.png

# In[1]:


#hide
from datetime import datetime, date, timedelta
import requests
import re
from tqdm import tqdm

import pandas as  pd
import numpy as np

import plotly
import plotly.express as px
from plotly.subplots import make_subplots
import plotly.graph_objects as go

import altair as alt

from IPython.display import display_html, HTML

import ipywidgets as widgets

import psycopg2
import psycopg2.extras


# In[2]:


#hide

# Config postgre connection
db = "SRA_Analysis"
userid = "postgres"
passwd = "iforgot23"
myHost = "localhost"

# Create a connection to the database
conn = None
try:
    # Parses the config file and connects using the connect string
    conn = psycopg2.connect(database=db,
                                user=userid,
                                password=passwd,
                                host=myHost)
except psycopg2.Error as sqle:
    print("psycopg2.Error : " + sqle.pgerror)


# In[3]:


#hide

# Wrapper function for querying
def pgquery( conn, sqlcmd, args, silent=False, returntype='tuple'):
   """ utility function to execute some SQL query statement
       it can take optional arguments (as a dictionary) to fill in for placeholder in the SQL
       will return the complete query result as return value - or in case of error: None
       error and transaction handling built-in (by using the 'with' clauses) """
   retval = None
   with conn:
      cursortype = None if returntype != 'dict' else psycopg2.extras.RealDictCursor     
      with conn.cursor(cursor_factory=cursortype) as cur:
         try:
            if args is None:
                cur.execute(sqlcmd)
            else:
                cur.execute(sqlcmd, args)
            retval = cur.fetchall() # we use fetchall() as we expect only _small_ query results
         except Exception as e:
            if e.pgcode != None and not(silent):
                print("db read error: ")
                print(e)
   return retval


# In[4]:


#hide

## Create a table showing the daily change in temp forecasts
query_stmt = "select * from \"MTPASA_DUIDAvailability\""

raw_pasa = pgquery(conn, query_stmt, None)
raw_pasa = pd.DataFrame(raw_pasa)
raw_pasa.columns = ['Publish_Datetime','Day','RegionID','DUID','PasaAvailability','LatestOfferDatetime','LastChanged']
raw_pasa = raw_pasa.sort_values(by=['Publish_Datetime','RegionID','Day','DUID'])

# Get the most recent publish datetime
most_recent = raw_pasa.Publish_Datetime.unique()[-1]
# Get todays date
today = np.datetime64(date.today())
yesterday = np.datetime64(date.today() - timedelta(days = 1))
oneweekago = np.datetime64(date.today() - timedelta(days = 7))


# # MTPASA
# Total Availability

# In[ ]:


#hide

# Create a subset DF first
# Create a DF to use
regional_df = raw_pasa.copy()
# Drop duplicates, keeping the most recent publish date (can differ)
regional_df = regional_df.sort_values(['Publish_Datetime','Day','RegionID','DUID']).drop_duplicates(subset=['Day','RegionID','DUID'], keep='last')
# Drop PASA from dates older than today
regional_df = regional_df[regional_df.Day>yesterday]# filter to get the most recent date and region
# Group by day and region (sum pasa)
regional_pivot = regional_df.groupby(by=['Day','RegionID']).sum()
regional_pivot = regional_pivot.reset_index()
regional_pivot

# Add plot
xline = regional_df.Day.unique()
xline = [pd.to_datetime(x).strftime('%Y-%m-%d') for x in xline]

regional_fig = go.Figure()
regional_fig.add_trace(go.Scatter(x=xline, y=regional_pivot[regional_pivot.RegionID=='NSW1'].PasaAvailability, name='NSW'))
regional_fig.add_trace(go.Scatter(x=xline, y=regional_pivot[regional_pivot.RegionID=='QLD1'].PasaAvailability, name='QLD'))
regional_fig.add_trace(go.Scatter(x=xline, y=regional_pivot[regional_pivot.RegionID=='SA1'].PasaAvailability, name='SA'))
regional_fig.add_trace(go.Scatter(x=xline, y=regional_pivot[regional_pivot.RegionID=='VIC1'].PasaAvailability, name='VIC'))
regional_fig.add_trace(go.Scatter(x=xline, y=regional_pivot[regional_pivot.RegionID=='TAS1'].PasaAvailability, name='TAS'))

regional_fig.update_layout(title='MTPASA')


# In[ ]:


#hide_input
HTML(regional_fig.to_html(include_plotlyjs='cdn'))


# ## MTPASA Change

# In[ ]:


#hide
regional_df = raw_pasa.copy()
regional_df = regional_df[regional_df.Publish_Datetime>=oneweekago]
regional_df = regional_df.sort_values(['Publish_Datetime','Day','RegionID','DUID']).drop_duplicates(subset=['Publish_Datetime','Day','RegionID','DUID'], keep='last')
regional_df = regional_df.groupby(['Publish_Datetime','Day','RegionID']).sum()
regional_df = regional_df.reset_index()

# Drop duplicates, keeping the most recent publish date (can differ)
regional_df = regional_df.sort_values(['Publish_Datetime','Day','RegionID'])

# Drop PASA from dates older than today
regional_df = regional_df[regional_df.Day>yesterday]# filter to get the most recent date and region

regional_df = regional_df.reset_index(drop=True)


# In[ ]:


#hide
alt.data_transformers.disable_max_rows()    # altair doesnt plot > 5000 row DFs... disable this (think about longer-term file size consequences)


# In[ ]:


#hide_input
regional_df['Publish_Datetime'] = regional_df['Publish_Datetime'].dt.strftime('%Y-%m-%d %H:%M:%S')

regional_df = regional_df.reset_index(drop=True)

publish_dates = list(regional_df.Publish_Datetime.unique())
publish_dates = publish_dates[-15:]
publish_dates = pd.DataFrame({'Publish_Datetime': publish_dates})    # use this when trying the selection_multi method

regions = list(regional_df.RegionID.unique())

date_selection = alt.selection_multi(fields=['Publish_Datetime'])
color = alt.condition(date_selection, alt.Color('Publish_Datetime:N'), alt.value('lightgray'))
date_selector = alt.Chart(publish_dates).mark_rect().encode(y='Publish_Datetime', color=color).add_selection(date_selection)


region_selector = alt.binding_select(options=regions, name=" ")
region_selection = alt.selection_single(fields=['RegionID'], bind=region_selector, init={'RegionID':'NSW1'})

chart = alt.Chart(regional_df).mark_line().encode(
    x=alt.X('Day:T', scale=alt.Scale(zero=False)),
    y=alt.Y('PasaAvailability:Q', scale=alt.Scale(zero=False)),
    color='Publish_Datetime'
).add_selection(region_selection
).transform_filter(date_selection     #, date_selection)
).transform_filter(region_selection
).properties(
    width=650, height=600,
    title={
        'text': ['MTPASA Delta'],
        'subtitle':['Use shift + click to compare multiple dates', 'Use mouse wheel to zoom on chart, double-click to reset']
    }
).interactive()
                
date_selector | chart


# # STPASA

# In[ ]:


#hide

## IMPORT SQL DATA ##
## Create a table showing the daily change in temp forecasts
query_stmt = "select * from \"STPASA\""

raw_stpasa = pgquery(conn, query_stmt, None)
raw_stpasa = pd.DataFrame(raw_stpasa)

raw_stpasa.columns = ['Publish_Datetime','Interval','RegionID','Unconstrained_Capacity','Constrained_Capacity','Surplus_Reserve']
raw_stpasa = raw_stpasa.drop_duplicates(subset=['Publish_Datetime','Interval','RegionID'], keep='last')
raw_stpasa = raw_stpasa.sort_values(by=['Publish_Datetime','RegionID','Interval'])
raw_stpasa = raw_stpasa[raw_stpasa.Interval > yesterday]
raw_stpasa = raw_stpasa.reset_index(drop=True)

# Get the data types right
raw_stpasa['Unconstrained_Capacity'] = np.int64(raw_stpasa['Unconstrained_Capacity'])
raw_stpasa['Constrained_Capacity'] = np.int64(raw_stpasa['Constrained_Capacity'])
raw_stpasa['Surplus_Reserve'] = np.int64(raw_stpasa['Surplus_Reserve'])
raw_stpasa['Publish_Datetime'] = raw_stpasa['Publish_Datetime'].dt.strftime('%Y-%m-%d %H:%M:%S')


# In[ ]:


#hide_input

stpasa_dates = list(raw_stpasa['Publish_Datetime'].unique())
stpasa_dates = stpasa_dates[-15:]
stpasa_dates = pd.DataFrame({'Publish_Datetime': stpasa_dates})

stpasa_date_selection = alt.selection_multi(fields=['Publish_Datetime'])
color = alt.condition(stpasa_date_selection, alt.Color('Publish_Datetime:N'), alt.value('lightgray'))
stpasa_date_selector = alt.Chart(stpasa_dates).mark_rect().encode(y='Publish_Datetime', color=color).add_selection(stpasa_date_selection)

chart = alt.Chart(raw_stpasa).mark_line().encode(
    x=alt.X('Interval:T', scale=alt.Scale(zero=False)),
    y=alt.Y('Unconstrained_Capacity:Q', scale=alt.Scale(zero=False)),
    color='Publish_Datetime'
).add_selection(region_selection    # taken from above cell
).transform_filter(stpasa_date_selection
).transform_filter(region_selection
).properties(
    width=650, height=600,
    title={
        'text': ['STPASA Delta'],
        'subtitle':['Use shift + click to compare multiple dates', 'Use mouse wheel to zoom on chart, double-click to reset']
    }
).interactive()

# Add a second chart for surplus reserve
chart_reserve = alt.Chart(raw_stpasa).mark_line().encode(
    x=alt.X('Interval:T', scale=alt.Scale(zero=False)),
    y=alt.Y('Surplus_Reserve:Q', scale=alt.Scale(zero=False)),
    color='Publish_Datetime'
).add_selection(region_selection    # taken from above cell
).transform_filter(stpasa_date_selection
).transform_filter(region_selection
).properties(
    width=650, height=600,
    title={
        'text': ['Reserve Delta'],
        'subtitle':['Use shift + click to compare multiple dates', 'Use mouse wheel to zoom on chart, double-click to reset']
    }
).interactive()

stpasa_date_selector | alt.vconcat(chart, chart_reserve)

