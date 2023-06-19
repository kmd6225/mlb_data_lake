from google.cloud import bigquery 
import streamlit as st
import seaborn as sns
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np


def plot_zone(p_type, pitches, pitcher):
    
        
    joint_chart = sns.jointplot(pitches[(pitches['pitch_number'] == p_type) & (
                                        pitches['details_call_description'].str.contains('Ball') == False) ].pitch_Data_coordinates_x,
                                    pitches[(pitches['pitch_number'] == p_type) & (
                                                                        pitches['details_call_description'].str.contains('Ball') == False)].pitch_Data_coordinates_y,
                                     color='r',
                                     marker='o',
                                     s=50,
                                     kind='scatter',
                                     space=0,
                                     label='Strikes/Hits/Fouls',
                                     alpha=1.0)
    
    joint_chart.fig.set_size_inches(6,6)
    
    joint_chart.x = pitches[(pitches['pitch_number'] == p_type) & (pitches['details_call_description'].str.contains('Ball'))].pitch_Data_coordinates_x
    joint_chart.y =  pitches[(pitches['pitch_number'] == p_type) & (pitches['details_call_description'].str.contains('Ball'))].pitch_Data_coordinates_y
    joint_chart.plot_joint(plt.scatter, marker='o',
                           c='b', s=50,label='Balls')
    
    ax = joint_chart.ax_joint
    
    ax.set_xlim(25,200)
    ax.set_ylim(75,300)
    
    
    
    # Add a title and legend
    ax.set_title('{} {}: Stikes/Fouls/Balls Hit'.format(pitcher, p_type),
                 y=1.2, fontsize=18)
    ax.legend(loc=3, frameon=True, shadow=True)
    
    return(joint_chart)




credentials = service_account.Credentials.from_service_account_file('/home/kdubartell/compute_keys.json')
project_id = 'apt-terrain-390117'
client = bigquery.Client(credentials= credentials,project=project_id)

pitcher = st.text_input = ('Insert Pitcher', 'German Marquez')

# Perform a query.
QUERY = (
    
'select matchup_pitcher_fullName, pitch_Number, details_call_description, pitch_Data_coordinates_x, pitch_Data_coordinates_y'
'from mlb_db.pitch_fact'

)

pitches = client.query(QUERY)
pitches = pitches.to-dataframe()
pitches = pitches[pitches['matchup_pitcher_fullName'] == pitcher]

p_type_v = st.select_slider('select a pitch type', options = np.unique(pitches['pitch_number'].tolist()).tolist())

chart = plot_zone(p_type_v, pitches, pitcher)

st.pyplot(chart)





