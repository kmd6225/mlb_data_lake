from google.cloud import bigquery 
from google.oauth2 import service_account
import pandas as pd
import numpy as np
from dash import Dash, html, dash_table, dcc, callback, Output, Input
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

credentials = service_account.Credentials.from_service_account_file('C:/Users/kduba/OneDrive/Desktop/compute_keys.json')
project_id = 'apt-terrain-390117'
client = bigquery.Client(credentials= credentials,project=project_id)



# Perform a query.
record_query = ('select * from mlb_db.team_record_vw')


record = client.query(record_query)
record = record.to_dataframe()

#intialize app 

app = Dash(__name__)

#traces 
fig = go.Figure()
fig.add_trace(go.Scatter(x=record.game_date, y= record.cum_wins,
                    id = 'scatter',
                    mode='lines',
                    name='cumulative wins'))



# App layout
app.layout = html.Div([
    html.Div(children= 'Cumulative Wins vs Expected Total Wins'),
    dcc.Graph(figure = fig),
    dcc.Dropdown(options= np.unique(record['team_name']).tolist(), value='Colorado Rockies', id='dropdown')
])

@app.callback(
    Output("scatter", "figure"),
    Input("dropdown", "value"),)

def update_line_chart(value):
    df = record
    mask = record[record['team_name'] == value]
    fig = px.line(mask, 
        x="game date", y="cumulative wins", color='dark blue')
    return fig

app.run_server()