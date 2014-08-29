__author__ = 'itoledo'

import cx_Oracle
from numpy import dtype
import pandas as pd
pd.options.display.width = 200
pd.options.display.max_columns = 50
pd.options.display.max_rows = 20

conx_string = 'almasu/alma4dba@ALMA_ONLINE.OSF.CL'

connection = cx_Oracle.connect(conx_string)
cursor = connection.cursor()

sql_measurements = 'SELECT * FROM sourcecatalogue.measurements'
cursor.execute(sql_measurements)
measurements = pd.DataFrame(
    cursor.fetchall(),
    columns=[rec[0] for rec in cursor.description]).set_index('MEASUREMENT_ID')

sql_sourcename = 'SELECT * FROM sourcecatalogue.source_name'
cursor.execute(sql_sourcename)
source_name = pd.DataFrame(
    cursor.fetchall(), columns=[rec[0] for rec in cursor.description])

sql_names = 'SELECT * FROM sourcecatalogue.names'
cursor.execute(sql_names)
names = pd.DataFrame(
    cursor.fetchall(),
    columns=[rec[0] for rec in cursor.description])

dt = {'Category': dtype('O'),
      'IVS': dtype('O'),
      'dec_d': dtype('O'),
      'dec_err': dtype('float64'),
      'dec_m': dtype('int64'),
      'dec_s': dtype('float64'),
      'name': dtype('O'),
      'numobs': dtype('int64'),
      'ra_dec_corr': dtype('float64'),
      'ra_err': dtype('float64'),
      'ra_h': dtype('int64'),
      'ra_m': dtype('int64'),
      'ra_s': dtype('float64')}

vlbi = pd.io.parsers.read_table(
    '/home/itoledo/Work/calscripts/rfc_2014b_cat.txt',
    skiprows=119,
    skipinitialspace=True,
    sep=' ',
    names=['Category', 'IVS', 'name', 'ra_h', 'ra_m', 'ra_s', 'dec_d',
           'dec_m', 'dec_s', 'ra_err', 'dec_err', 'ra_dec_corr', 'numobs'],
    usecols=[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12], dtype=dt)

atca = pd.io.parsers.read_table(
    '/home/itoledo/Work/calscripts/ATCA_newCandidates_sorted.txt',
    skiprows=24, sep='\t',
    names=['ATCA_name', 'RA', 'errRA', 'DEC', 'errDEC', 'f20', 'ef20', 'f93',
           'B3est', 'Flags', 'Flags2', 'ALMAname'],
    usecols=[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11])

