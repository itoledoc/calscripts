__author__ = 'itoledo'

import cx_Oracle
from numpy import dtype
import pandas as pd
pd.options.display.width = 200
pd.options.display.max_columns = 50
pd.options.display.max_rows = 20

def calc_ra(h, m, s):
    ra = h * 15. + m * 15. / 60 + s * 15. / 3600.
    return ra


def calc_dec(d, m, s):
    d_int = pd.np.int16(d)
    if d.startswith('-'):
        dec = -1 * d_int - m / 60. - s / 3600.
    else:
        dec = d_int + m / 60. + s / 3600.
    return dec


def distance(ra1, ra2, dec1, dec2):
    ra1 = pd.np.radians(ra1)
    ra2 = pd.np.radians(ra2)
    dec1 = pd.np.radians(dec1)
    dec2 = pd.np.radians(dec2)

    cos_teta = pd.np.sin(dec1) * pd.np.sin(dec2) + \
        pd.np.cos(dec1) * pd.np.cos(dec2) * pd.np.cos(ra1 - ra2)
    teta = pd.np.degrees(pd.np.arccos(cos_teta))
    return teta * 3600.


def match(ra, dec, name, sources_cat):
    sel = sources_cat[(sources_cat.DEC > dec - 0.5) &
                      (sources_cat.DEC < dec + 0.5)]
    ids = sel.apply(
        lambda r: distance(r['RA'], ra, r['DEC'], dec), axis=1)
    try:
        print ids.idxmin()[0], ids.min()
        idx = ids.idxmin()[0]
        idm = ids.min()
    except ValueError:
        idx = 999999
        idm = 999999

    return pd.Series([idx, idm, name],
                     index=['match_alma', 'distance', 'name'])


conx_string = 'almasu/alma4dba@ALMA_ONLINE.OSF.CL'

connection = cx_Oracle.connect(conx_string)
cursor = connection.cursor()

sql_measurements = 'SELECT * FROM sourcecatalogue.measurements'
cursor.execute(sql_measurements)
measurements = pd.DataFrame(
    cursor.fetchall(),
    columns=[rec[0] for rec in cursor.description]).set_index('MEASUREMENT_ID')
mgroup = measurements.groupby('SOURCE_ID')
sources = mgroup.apply(
    lambda t: t[t.DATE_CREATED == t.DATE_CREATED.max()])

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

vlbi.RA = vlbi.apply(lambda r: calc_ra(r['ra_h'], r['ra_m'], r['ra_s']), axis=1)
vlbi.DEC = vlbi.apply(lambda r: calc_dec(r['dec_d'], r['dec_m'], r['dec_s']),
                      axis=1)

cross = atca.apply(lambda r: match(r['RA'], r['DEC'], r['ATCA_name'], sources),
                   axis=1)

atca2 = pd.merge(atca, cross, left_index=True, right_index=True)