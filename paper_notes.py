
# -*- coding: utf-8 -*-

import simpy
import random
import numpy as np
import pandas as pd
import math
from sqlalchemy import create_engine

class Default_Params:
    #################Historical In and Out Patient attendances##################
    #Get the data of every patient arrival over a year.
    sdmart_engine = create_engine('mssql+pyodbc://@SDMartDataLive2/InfoDB?'\
                                'trusted_connection=yes&driver=ODBC+Driver+17'\
                                '+for+SQL+Server')
    query = """(SELECT [Hospital ID], [Appointment Datetime] AS [Visit Datetime],
    [Appointment Specialty] AS [Specialty], [Referral Datetime],
    [patnt_create_dttm], 'Outpatient' AS [Type]
    FROM [InfoDB].[dbo].[DS_Outpatient_2023_To_Present]
    LEFT JOIN [PiMSMarts].[dbo].[patients] pat
    ON [Hospital ID] = [pasid]
    WHERE [Appointment Datetime] > DATEADD(YEAR,-1,CAST(GETDATE() AS DATE))
    AND [Appointment Datetime] < GETDATE())

    UNION

    (SELECT [Hospital ID], [Admitted Datetime] AS [Visit Datetime],
    [Admitted Specialty] AS [Specialty], NULL AS [Referral Datetime],
    [patnt_create_dttm], 'In Patient' AS [Type]
    FROM [InfoDB].[dbo].[DS_Inpatient_2023_To_Present]
    LEFT JOIN [PiMSMarts].[dbo].[patients] pat
    ON [Hospital ID] = [pasid]
    WHERE [Admitted Datetime] > DATEADD(YEAR,-1,CAST(GETDATE() AS DATE))
    AND [Admitted Datetime] < GETDATE())"""
    patients = pd.read_sql(query, sdmart_engine)

    ##############################Model parameters############################
    #run_time = 5000
    run_time = ((patients['Visit Datetime'].max()
               - patients['Visit Datetime'].min()) / np.timedelta64(1, 'm')) + 1
    iterations = 1#10
    sample_time = 1440 #sample daily
    high_risk_thresh = 0.005
    clinic_dec_thresh = 0.001

    ##############################New Patients###############################
    patients['time_since_created'] = (patients['Referral Datetime'].fillna(
                                     patients['Visit Datetime'])
                                     - patients['patnt_create_dttm']).dt.days
    patients['new_patient'] = np.where(((patients['time_since_created'] < 1)
                              & (patients['patnt_create_dttm'].dt.year > 2020)),
                              True, False)
    
    ###########################Inter Arrival Time###########################
    patients.sort_values(by=['Visit Datetime'], inplace=True)
    patients['inter_arr'] = (patients['Visit Datetime'].diff()
                             / np.timedelta64(1, 'm')).fillna(0)
    
    #######################Visit and Patient IDs############################
    ids = pd.DataFrame(patients['Hospital ID'].drop_duplicates().copy())
    ids['ID'] = [i+1 for i in range(len(ids))]
    patients = patients.merge(ids, on='Hospital ID', how='left')
    patients['visit_no'] = [i+1 for i in range(len(patients))]

    pat_lookup = patients[['visit_no', 'ID', 'inter_arr', 'new_patient',
                           'Specialty']].to_dict('records')
    ##########################Record Results################################
    #Results table, start with everyone requiring paper notes, reduce as model
    #runs.
    results = patients[['ID']].drop_duplicates().copy()
    specialties = patients['Specialty'].drop_duplicates().to_list()
    for spec in specialties:
        results[spec] = True
    results = results.set_index('ID')
    #Empty lists for results
    population = []
    attendances = []
    
# Class representing our patients coming in for the weight loss clinic.
# Here, we only have a constructor method, that sets up the patient's ID
class Spawn_Attendance:
    def __init__(self, att_id, pat_lookup, high_risk_prob, clinic_dec_prob):
        self.id = att_id
        #Get information about the attendance from the table
        self.specialty = pat_lookup['Specialty']
        self.pat_id = pat_lookup['ID']
        self.new_pat = pat_lookup['new_patient']
        self.inter_arr = pat_lookup['inter_arr']
        #Is patient high risk or is there a clinical decision to get the notes
        self.high_risk = (True if random.uniform(0, 1) < high_risk_prob
                          else False)
        self.clinic_dec = (True if random.uniform(0, 1) < clinic_dec_prob
                           else False)
        #Record some results for testing
        self.arrival_time = np.nan
        self.notes_requested = np.nan
        self.out = ''
        
# Class representing our model of the GP Surgery.
class Paper_Notes_Model:
    # Here, the constructor sets up the SimPy environment, sets a patient
    # counter to 0 (which we'll use for assigning patient IDs), and sets up
    # our resources (here just a nurse resource, with capacity given by
    # the number stored in the g class)
    def __init__(self):
        self.env = simpy.Environment()
        self.visit_counter = 0
        self.paper_notes = simpy.Resource(self.env, capacity=np.inf)
        
    # A method that generates patients arriving for the weight loss clinic
    def generate_attendances(self):
        # Keep generating indefinitely (until all patients have been done)
        while self.visit_counter < len(Default_Params.pat_lookup):
            # Increment the patient counter by 1
            self.visit_counter += 1
            att = Spawn_Attendance(self.visit_counter,
                                   Default_Params.pat_lookup[self.visit_counter-1],
                                   Default_Params.high_risk_thresh,
                                   Default_Params.clinic_dec_thresh)
            self.env.process(self.paper_notes_journey(att))
            yield self.env.timeout(att.inter_arr)
            
    # A method that models the processes for attending the weight loss clinic.
    # The method needs to be passed a patient who will go through these
    # processes
    def paper_notes_journey(self, attendance):
        # Print the time the patient started queuing for a nurse
        attendance.arrival_time = self.env.now
        #print('----------------------------------------------------------------')
        print(f"attendence {attendance.id} for patient {attendance.pat_id} started at {self.env.now:.1f}")
        #print(f"""New patient: {attendance.new_pat}
        #high risk: {attendance.high_risk}
        #clinical decision: {attendance.clinic_dec}""")
        #print(f"Patient to see {attendance.specialty}")

        if attendance.new_pat:
            #If a new patient to the trust, paper notes are not required as they
            #don't exist. Set their status in the results table to False for all
            #sepcialties.
            attendance.out = 'New Patient'
            attendance.notes_requested = False
            #print(f'New patient - no paper notes required for patient {attendance.pat_id}')
            Default_Params.results.loc[attendance.pat_id] = False


        else:
            #Patient is not new to the hospital.  Check if the patient is new to
            #this specialty or not. If spec_status is true, then patient is
            #new to this specialty within run time
            spec_status = Default_Params.results.loc[attendance.pat_id,
                                                     attendance.specialty]
            #print(f'''patient {attendance.pat_id} has paper notes request status {spec_status} for {attendance.specialty}''')
            if spec_status:
                #If first attendance to the specialty, request the notes and
                #reset their notes requirement in the results table
                with self.paper_notes.request() as req:
                    attendance.out = 'New to Specialty'
                    attendance.notes_requested = True
                    #print(f'''New to spec - paper notes requested for patient {attendance.pat_id}''')
                    yield req
                Default_Params.results.loc[attendance.pat_id,
                                           attendance.specialty] = False
            else:
                # if not first attendance to the specialty, no paper notes are
                #required unless patient is high risk or a clinical decision.

                #High Risk
                if attendance.high_risk:
                    with self.paper_notes.request() as req:
                        attendance.out = 'High Risk'
                        attendance.notes_requested = True
                        #print(f'High risk - paper notes requested for patient {attendance.pat_id}')
                        yield req
                #Clinical Decision
                elif attendance.clinic_dec:
                    with self.paper_notes.request() as req:
                        attendance.out = 'Clinical Decision'
                        attendance.notes_requested = True
                        #print(f'Clinical Decision - paper notes requested for patient {attendance.pat_id}')
                        yield req
                #No Paper Notes required
                else:
                    attendance.out = 'Sink'
                    attendance.notes_requested = False
                    #print(f'End - no paper notes required for patient {attendance.pat_id}')
        #Store results for this attendance.
        self.store_attendance_results(attendance)
    
    def store_attendance_results(self, attendance):
        Default_Params.attendances.append([attendance.id,
                                          attendance.pat_id,
                                          attendance.specialty,
                                          attendance.arrival_time,
                                          attendance.out,
                                          attendance.notes_requested])
    
    def store_notes_proportions(self):
        while True:
            #Calculate the proportion of patients at each time step for each
            #specialty and overall who would still require paper notes.
            #print('===========================================================')
            #print('SAMPLING PROPORTIONS')
            #print('===========================================================')
            ####Specialties
            paper_notes_prop = (Default_Params.results.sum()
                                / Default_Params.results.count())
            no_spec = len(Default_Params.specialties)
            ####Overall
            #Get the proportion of patients who would definately need paper notes
            #when they arrive at the hospital.
            all_spec = Default_Params.results.all(axis=1)
            paper_notes_prop['All'] = all_spec.sum() / all_spec.count()
            #Get the average proportion of specialties where paper notes would
            #still be required
            paper_notes_prop['All Prop'] = (Default_Params.results.sum(axis=1)
                                            / no_spec).mean()
            ####Add sample time and append
            paper_notes_prop['Time'] = self.env.now
            paper_notes_prop['Day'] = self.env.now / Default_Params.sample_time
            Default_Params.population.append(paper_notes_prop)
            ####Pause until the next sample time
            yield self.env.timeout(Default_Params.sample_time)

    def run(self):
        self.env.process(self.generate_attendances())
        self.env.process(self.store_notes_proportions())
        self.env.run(until=Default_Params.run_time)

#Run the model
my_gp_model = Paper_Notes_Model()
my_gp_model.run()

#Create and save outputs
attendances_df = pd.DataFrame(Default_Params.attendances,
                              columns=['Visit ID', 'Patient ID', 'Specialty',
                                       'Arrival Time', 'Description',
                                       'Notes Requested?'])
attendances_df.to_csv('Attendances.csv', index=False)
requests_pop = pd.DataFrame(Default_Params.population,
                            columns=Default_Params.specialties
                                    + ['All', 'All Prop', 'Time', 'Day'])
requests_pop.to_csv('Population.csv', index=False)
Default_Params.results.to_csv('Results.csv')
