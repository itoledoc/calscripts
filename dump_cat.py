__author__ = 'itoledo'

import os
import cx_Oracle
import ephem as eph
import math
from datetime import datetime, timedelta
import numpy as np
import pandas as pd


conx_string = 'SRCCAT_RO/ssrtest@ALMA_ONLINE.OSF.CL'
connection = cx_Oracle.connect(conx_string)
cursor = connection.cursor()

sql_sourcename = "SELECT * FROM sourcecatalogue.source_name"
sql_names = "SELECT * FROM sourcecatalogue.names"
sql_database = "SELECT * FROM sourcecatalogue.measurements"

cursor.execute(sql_sourcename)
sourcetonames = pd.DataFrame(cursor.fetchall(), columns=['name_id', 'source_id'])

cursor.execute(sql_names)
names = pd.DataFrame(cursor.fetchall(), columns=['name_id', 'name'])

cursor.execute(sql_database)
measurements = pd.DataFrame(cursor.fetchall(),
                            columns=['measurement_id', 'catalogue_id', 'source_id', 'RA', 'RA_uncer', 'DEC',
                                     'DEC_uncer', 'frequency', 'flux', 'flux_uncer', 'degree', 'degree_uncer',
                                     'angle', 'angle_uncer', 'extension', 'fluxratio', 'origin', 'date_observed',
                                     'date_created', 'valid', 'uvmin', 'uvmax'])
vlbi = pd.io.parsers.read_table(
    '/home/itoledo/Downloads/rfc_2014b_cat.txt',
    skiprows=119,
    skipinitialspace=True,
    sep=' ',
    names=['Category', 'IVS', 'name', 'ra_h', 'ra_m', 'ra_s', 'dec_d', 'dec_m', 'dec_s', 'ra_err',
           'dec_err', 'ra_dec_corr', 'numobs'],
    usecols=[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12])


# create data frame with only alma measurementes

alma_measurements = measurements.query('catalogue_id == 5')

# create data frame with vlbi sources that are in the name database

vlbi_j1 = pd.merge(vlbi, names, on='name', how='inner')

# create data frame that adds the column source_id based on the name

vlbi_j2 = pd.merge(vlbi_j1, sourcetonames, on='name_id', how='inner')

# create data frame merging vlbi_j2 with alma_measurments, a full table with the addition of vlbi information for the
# sources that are in the vlbi  by name

alma_vlbi = pd.merge(alma_measurements, vlbi_j2, on='source_id', how='inner')

# lists with source_id of alma_measurments and alma_in_vlbi

almasources = pd.unique(alma_measurements.source_id)
almavlbisources = pd.unique(alma_vlbi.source_id)

# list of source_id that are not in the alma_measurement table

not_in_vlbi = list(set(almasources) - set(almavlbisources))

# Creating columns to compare coordinates in degrees.
alma_vlbi['RA_vlbi'] = alma_vlbi['ra_h'] * 15. + alma_vlbi['ra_m'] * 15. / 60. + alma_vlbi['ra_s'] * 15. / 3600.
alma_vlbi['DEC_vlbi'] = alma_vlbi['dec_d'] + alma_vlbi['dec_m'] * pd.np.sign(alma_vlbi['dec_d'])/ 60. + alma_vlbi['dec_s'] * pd.np.sign(alma_vlbi['dec_d'])/ 3600.
d0 = alma_vlbi.query('dec_d == 0')
alma_vlbi.loc[d0.index.tolist(), 'DEC_vlbi'] = (alma_vlbi.loc[d0.index.tolist(), 'dec_d'] + alma_vlbi.loc[d0.index.tolist(), 'dec_m'] / 60. + alma_vlbi.loc[d0.index.tolist(), 'dec_s'] / 3600.) * pd.np.sign(alma_vlbi.loc[d0.index.tolist(), 'DEC'])

# Creating columns with RA and DEC differences, in arcsec.
alma_vlbi['DEC_diff'] = 3600. * np.abs(alma_vlbi['DEC'] - alma_vlbi['DEC_vlbi'])
alma_vlbi['RA_diff'] = 3600. * np.abs((alma_vlbi['RA'] - alma_vlbi['RA_vlbi']) * np.cos(alma_vlbi['DEC']))

# Create group by source_id
alma_group = alma_vlbi.groupby(['source_id'])

# Create a data frame with only the latest measurements per source_id
alma_vlbi_latest = alma_group.apply(lambda t: t[t.date_observed == t.date_observed.max()]).drop_duplicates(cols=['source_id','date_observed'])
alma_vlbi_latest.to_excel('/home/itoledo/Downloads/latestmeasurements.xls')

pd.options.display.max_columns = 40

# Recipe to get sources not observed in the last year

non_grid = alma_vlbi_latest.query('date_observed < "2013-10-01"')

g4 = non_grid.query('frequency < 1.30e+11 and RA >=30 and RA < 60')[['source_id', 'name', 'ra_h', 'ra_m', 'ra_s', 'dec_d', 'dec_m', 'dec_s', 'frequency', 'flux']]
str(g4.name.tolist()).replace('\'','').replace(', ',',').replace('J','j')