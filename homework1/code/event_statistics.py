import time
import pandas as pd
from datetime import datetime
import numpy as np

def read_csv(filepath):
    '''
    Read the events.csv and mortality_events.csv files. Variables returned from this function are passed as input to the metric functions.
    This function needs to be completed.
    '''

    events = pd.read_csv(filepath + 'events.csv')
    mortality = pd.read_csv(filepath + 'mortality_events.csv')

    #events_f = open(filepath + 'events.csv', 'r').readlines()
    #events = [line.split(',') for line in events_f]
    #mortality_f = open(filepath + 'mortality_events.csv', 'r').readlines()
    #mortality = [line.split(',') for line in mortality_f]

    return events, mortality

def event_count_metrics(events, mortality):

    '''
    Event count is defined as the number of events recorded for a given patient.
    This function needs to be completed.
    '''

    dead_df = pd.merge(events, mortality, on='patient_id')

    #dead_patients = events[events.value == 1.0]

    alive_df = events[~events.patient_id.isin(dead_df.patient_id)]

    #print events.loc[events['patient_id'] == 7391].count()

    agg_dead_df = dead_df.groupby(['patient_id']).count()

    agg_alive_df = alive_df.groupby(['patient_id']).count()

    #print agg_dead_df.loc[agg_dead_df['event_id'].idxmax()]

    avg_dead_event_count = agg_dead_df['event_id'].mean()

    max_dead_event_count = agg_dead_df['event_id'].max()

    min_dead_event_count = agg_dead_df['event_id'].min()

    avg_alive_event_count = agg_alive_df['event_id'].mean()

    max_alive_event_count = agg_alive_df['event_id'].max()

    min_alive_event_count = agg_alive_df['event_id'].min()

    return min_dead_event_count, max_dead_event_count, avg_dead_event_count, min_alive_event_count, max_alive_event_count, avg_alive_event_count

def encounter_count_metrics(events, mortality):

    '''
    Encounter count is defined as the count of unique dates on which a given patient visited the ICU. 
    This function needs to be completed.
    '''

    dead_df = pd.merge(events, mortality, on='patient_id')
    alive_df = events[~events.patient_id.isin(dead_df.patient_id)]

    agg_dead_df = dead_df.groupby(['patient_id']).timestamp_x.nunique()

    agg_alive_df = alive_df.groupby(['patient_id']).timestamp.nunique()
    
    avg_dead_encounter_count = agg_dead_df.mean()

    max_dead_encounter_count = agg_dead_df.max()

    min_dead_encounter_count = agg_dead_df.min() 

    avg_alive_encounter_count = agg_alive_df.mean()

    max_alive_encounter_count = agg_alive_df.max()

    min_alive_encounter_count = agg_alive_df.min()

    return min_dead_encounter_count, max_dead_encounter_count, avg_dead_encounter_count, min_alive_encounter_count, max_alive_encounter_count, avg_alive_encounter_count

def record_length_metrics(events, mortality):
    '''
    Record length is the duration between the first event and the last event for a given patient. 
    This function needs to be completed.
    '''
    date_format = "%Y-%m-%d"

    dead_df = pd.merge(events, mortality, on='patient_id')
    
    dead_dates = dead_df.groupby(['patient_id']).timestamp_x
    #new_df = test_df.agg({'timestamp_x' : [np.max, np.min]})
    dead_max = list(dead_dates.max())
    dead_min = list(dead_dates.min())
    dead_diff = []
    for i in range(0, len(dead_min)):
        val = datetime.strptime(dead_max[i], date_format) - datetime.strptime(dead_min[i], date_format)
        dead_diff.append(val.days)
   
    alive_df = events[~events.patient_id.isin(dead_df.patient_id)]
    alive_dates = alive_df.groupby(['patient_id']).timestamp
    alive_max = list(alive_dates.max())
    alive_min = list(alive_dates.min())

    alive_diff = []
    for i in range(0, len(alive_min)):
        val = datetime.strptime(alive_max[i], date_format) - datetime.strptime(alive_min[i], date_format)
        alive_diff.append(val.days)

    avg_dead_rec_len = sum(dead_diff)/float(len(dead_diff))

    max_dead_rec_len = max(dead_diff)

    min_dead_rec_len = min(dead_diff)

    avg_alive_rec_len = sum(alive_diff)/float(len(alive_diff))

    max_alive_rec_len = max(alive_diff)

    min_alive_rec_len = min(alive_diff)

    return min_dead_rec_len, max_dead_rec_len, avg_dead_rec_len, min_alive_rec_len, max_alive_rec_len, avg_alive_rec_len

def main():
    '''
    DONOT MODIFY THIS FUNCTION. 
    Just update the train_path variable to point to your train data directory.
    '''
    #Modify the filepath to point to the CSV files in train_data
    train_path = '../data/train/'
    events, mortality = read_csv(train_path)

    #Compute the event count metrics
    start_time = time.time()
    event_count = event_count_metrics(events, mortality)
    end_time = time.time()
    print("Time to compute event count metrics: " + str(end_time - start_time) + "s")
    print event_count

    #Compute the encounter count metrics
    start_time = time.time()
    encounter_count = encounter_count_metrics(events, mortality)
    end_time = time.time()
    print("Time to compute encounter count metrics: " + str(end_time - start_time) + "s")
    print encounter_count

    #Compute record length metrics
    start_time = time.time()
    record_length = record_length_metrics(events, mortality)
    end_time = time.time()
    print("Time to compute record length metrics: " + str(end_time - start_time) + "s")
    print record_length
    
if __name__ == "__main__":
    main()



