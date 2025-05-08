import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import os
os.chdir('C:/Users/obriene/Projects/Discrete Event Simulation/EPR Paper Notes model')
attendances = pd.read_csv('Attendances.csv')
population = pd.read_csv('Population.csv')
results = pd.read_csv('Results.csv')


attendances['Day'] = (attendances['Arrival Time'] / (60*24)).apply(np.floor)

os.chdir('C:/Users/obriene/Projects/Discrete Event Simulation/EPR Paper Notes model/Plots')

def attendances_plots(df, title):
    #Cumulative Sum of Requests
    df['cumsum'] = df['Notes Requested?'].cumsum()
    df.plot(x='Day', y='cumsum', 
            title=f'Cumulative Sum of Paper Requests for {title} Patients',
            xlabel='Day Number', ylabel='Cumulative Sum of Requests')
    plt.savefig(title + ' cumulative sum.png', bbox_inches='tight')
    plt.close()

    #Number of requests per day
    df.groupby('Day')['Notes Requested?'].sum().plot(x='Day', y='Notes Requested?', 
            title=f'Number of Paper Requests per Day for {title} Patients',
            xlabel='Day Number', ylabel='Number of Requests')
    plt.savefig(title + ' number of requests.png', bbox_inches='tight')
    plt.close()


def population_plots(df, title):
    df.plot(x='Day', y=title, title=f'Proportion of Patients who would require paper notes for {title} attendances',
            xlabel='Day Number', ylabel='Proportion')
    plt.savefig(title + ' proportion still requiring notes.png', bbox_inches='tight')
    plt.close()


attendances_plots(attendances, 'All')
population_plots(population, 'All')
population_plots(population, 'All Prop')

#Plot for All and All Prop on the same axis
population[['Day', 'All', 'All Prop']].set_index('Day').plot(title=f'Proportion of Patients who would require paper notes for attendances',
            xlabel='Day Number', ylabel='Proportion')
plt.savefig('proportion still requiring notes.png', bbox_inches='tight')

for spec in attendances['Specialty'].drop_duplicates().tolist():
    attendances_plots(attendances.loc[attendances['Specialty'] == spec].copy(),
                      spec)
    population_plots(population, spec)