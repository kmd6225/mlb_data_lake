import streamlit as st
import seaborn as sns
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np

pitches = pd.read_csv('C:/Users/kduba/Downloads/bqtest.csv')


def plot_zone(p_type):
    
        
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
    ax.set_title('German Marquez {}: Stikes/Fouls/Balls Hit'.format(p_type),
                 y=1.2, fontsize=18)
    ax.legend(loc=3, frameon=True, shadow=True)
    
    return(joint_chart)

p_type_v = st.select_slider('select a pitch type', options = np.unique(pitches['pitch_number'].tolist()).tolist())

chart = plot_zone(p_type_v)

st.pyplot(chart)





