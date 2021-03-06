__author__ = 'itoledo'

import cx_Oracle
from numpy import dtype
import pandas as pd
import ephem
pd.options.display.width = 200
pd.options.display.max_columns = 50
pd.options.display.max_rows = 20


def calc_ra(h, m, s):
    ra = h * 15. + m * 15. / 60 + s * 15. / 3600.
    return ra


def create_obj(ra, dec):
    obj = ephem.FixedBody()
    obj._ra = pd.np.radians(ra)
    obj._dec = pd.np.radians(dec)
    obj.compute()
    return obj


def calc_dec(d, m, s):
    d_int = pd.np.int16(d)
    if d.startswith('-'):
        dec = d_int - m / 60. - s / 3600.
    else:
        dec = d_int + m / 60. + s / 3600.
    return dec


def distance(obj1, obj2):

    return ephem.separation(obj1, obj2)


def match_alma(obj, dec, name, sources_cat):
    sel = sources_cat[(sources_cat.DEC > dec - 0.5) &
                      (sources_cat.DEC < dec + 0.5)]
    ids = sel.apply(
        lambda r: distance(obj, r['obj']), axis=1)
    try:
        print name, ids.idxmin(), ids.min()
        idx = ids.idxmin()
        idm = ids.min()
    except ValueError or IndexError:
        idx = 999999
        idm = 999999

    return pd.Series([idx, idm, name],
                     index=['match_alma', 'distance', 'name'])


def closest_alma(obj, dec, name, sources_cat):
    sel = sources_cat[(sources_cat.DEC > dec - 0.5) &
                      (sources_cat.DEC < dec + 0.5)]
    ids = sel.apply(
        lambda r: distance(obj, r['obj']), axis=1)
    try:
        ids.sort()
        print name, ids.index[1], pd.np.degrees(ids.iloc[1]) * 3600.
        idx = ids.index[1]
        idm = pd.np.degrees(ids.iloc[1]) * 3600.
    except IndexError:
        idx = 999999
        idm = 999999

    return pd.Series([idx, idm, name],
                     index=['match_alma', 'distance', 'name'])


def match_vlbi(ra, dec, name, sources_cat):
    sel = sources_cat[(sources_cat.DEC > dec - 1) &
                      (sources_cat.DEC < dec + 1)]
    ids = sel.apply(
        lambda r: distance(r['RA'], ra, r['DEC'], dec), axis=1)
    try:
        print ids.idxmin(), ids.min()
        idx = ids.idxmin()
        idm = ids.min()
    except ValueError:
        idx = 999999
        idm = 999999

    return pd.Series([idx, idm, name],
                     index=['match_vlbi', 'distance', 'name'])


def match_grid(ra, dec, name, sources_cat):
    sel = sources_cat[(sources_cat.DEC > dec - 20) &
                      (sources_cat.DEC < dec + 25)]
    ids = sel.apply(
        lambda r: distance(r['RA'], ra, r['DEC'], dec), axis=1)
    try:
        print ids.idxmin(), ids.min()
        idx = ids.idxmin()
        idm = ids.min()
    except ValueError:
        idx = 999999
        idm = 999999

    return pd.Series([idx, idm, name],
                     index=['match_grid', 'distance', 'name'])


def match_weak(ra, dec, name, sources_cat):
    sel = sources_cat[(sources_cat.RA_vlbi > dec - 2) &
                      (sources_cat.DEC_vlbi < dec + 2)]
    ids = sel.apply(
        lambda r: distance(r['RA_vlbi'], ra, r['DEC_vlbi'], dec), axis=1)
    try:
        print ids.idxmin(), ids.min()
        idx = ids.idxmin()
        idm = ids.min()
    except ValueError:
        idx = 999999
        idm = 999999

    return pd.Series([idx, idm, name],
                     index=['match_weak', 'distance', 'name'])


def split_ra(lista):
    hour = float(lista[0]) * 15.
    minu = float(lista[1]) * 15. / 60.
    sec = float(lista[2]) * 15. / 3600.
    return hour + minu + sec


def split_dec(lista):
    if float(lista[0]) == 0 and lista[0].startswith('-'):
        signo = -1.
    elif float(lista[0]) < 0:
        signo = -1
    else:
        signo = 1

    deg = float(lista[0])
    minu = signo * float(lista[1]) / 60.
    sec = signo * float(lista[2]) / 3600.
    return deg + minu + sec


conx_string = 'almasu/alma4dba@ALMA_ONLINE.OSF.CL'

connection = cx_Oracle.connect(conx_string)
cursor = connection.cursor()

sql_measurements = 'SELECT * FROM sourcecatalogue.measurements'
cursor.execute(sql_measurements)
measurements = pd.DataFrame(
    cursor.fetchall(),
    columns=[rec[0] for rec in cursor.description])
sources = measurements.sort(
    'DATE_OBSERVED', ascending=False).groupby(
        'SOURCE_ID').first()

sql_sourcename = 'SELECT * FROM sourcecatalogue.source_name'
cursor.execute(sql_sourcename)
source_name = pd.DataFrame(
    cursor.fetchall(), columns=[rec[0] for rec in cursor.description])

sql_names = 'SELECT * FROM sourcecatalogue.names'
cursor.execute(sql_names)
names = pd.DataFrame(
    cursor.fetchall(),
    columns=[rec[0] for rec in cursor.description])
cursor.close()
connection.close()

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
    '/home/itoledo/Work/calscripts/ATC.txt',
    skiprows=24, sep='\t',
    names=['ATCA_name', 'RA', 'errRA', 'DEC', 'errDEC', 'f20', 'ef20', 'f93',
           'B3est', 'Flags', 'Flags2', 'ALMAname'],
    usecols=[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11])

grids = pd.io.parsers.read_table(
    '/home/itoledo/Work/calscripts/grid_sources_20140830.csv', sep=',',
    names=['RA', 'RA_err', 'DEC', 'DEC_err', 'names'])

vlbi['RA'] = vlbi.apply(
    lambda r: calc_ra(r['ra_h'], r['ra_m'], r['ra_s']), axis=1)
vlbi['DEC'] = vlbi.apply(
    lambda r: calc_dec(r['dec_d'], r['dec_m'], r['dec_s']), axis=1)

cross_alma = atca.apply(
    lambda r: match_alma(r['RA'], r['DEC'], r['ATCA_name'], sources), axis=1)

cross_vlbi = atca.apply(
    lambda r: match_vlbi(r['RA'], r['DEC'], r['ATCA_name'], vlbi), axis=1)

cross_grid = atca.apply(
    lambda r: match_grid(r['RA'], r['DEC'], r['ATCA_name'], grids), axis=1)

atca2 = pd.merge(atca, cross_alma, left_index=True, right_index=True)
atca3 = pd.merge(atca2, cross_vlbi, left_index=True, right_index=True,
                 suffixes=['_alma', '_vlbi'])
atca4 = pd.merge(atca3, cross_grid, left_index=True, right_index=True,
                 suffixes=['_alma', '_grid'])

atca4 = atca4[[u'ATCA_name', u'RA', u'errRA', u'DEC', u'errDEC', u'f20',
               u'ef20', u'f93', u'B3est', u'Flags', u'Flags2', u'ALMAname',
               u'match_alma', u'distance_alma', u'match_vlbi', u'distance_vlbi',
               u'match_grid', u'distance']]
atca4.columns = pd.Index([u'ATCA_name', u'RA', u'errRA', u'DEC', u'errDEC',
                          u'f20', u'ef20', u'f93', u'B3est', u'Flags',
                          u'Flags2', u'ALMAname', u'match_alma',
                          u'distance_alma', u'match_vlbi', u'distance_vlbi',
                          u'match_grid', u'distance_grid'], dtype='object')

atca_select = atca4[
    (atca4.distance_grid > 1.9) &
    (atca4.Flags == 'g') & (atca4.Flags2 == 'nnn') & (atca4.distance_vlbi < 5)]

atca_sel_A = pd.merge(
    atca_select, sources, left_on='match_alma', right_index=True, how='left',
    suffixes=['_atca', '_alma'])

atca_sel_A.columns = pd.Index(
    [u'ATCA_name', u'RA_atca', u'errRA_atca', u'DEC_atca', u'errDEC_atca',
     u'f20', u'ef20', u'f93', u'B3est', u'Flags', u'Flags2', u'ALMAname',
     u'match_alma', u'distance_alma', u'match_vlbi', u'distance_vlbi',
     u'match_grid', u'distance_grid', u'MEASUREMENT_ID', u'CATALOGUE_ID',
     u'RA_alma', u'RA_UNCERTAINTY_alma', u'DEC_alma', u'DEC_UNCERTAINTY_alma',
     u'FREQUENCY', u'FLUX', u'FLUX_UNCERTAINTY', u'DEGREE',
     u'DEGREE_UNCERTAINTY', u'ANGLE', u'ANGLE_UNCERTAINTY', u'EXTENSION',
     u'FLUXRATIO', u'ORIGIN', u'DATE_OBSERVED', u'DATE_CREATED', u'VALID',
     u'UVMIN', u'UVMAX'], dtype='object')

atca_sel_AV = pd.merge(
    atca_sel_A, vlbi, left_on='match_vlbi', right_index=True, how='left')
atca_sel_AV.columns = pd.Index(
    [u'ATCA_name', u'RA_atca', u'errRA_atca', u'DEC_atca', u'errDEC_atca',
     u'f20', u'ef20', u'f93', u'B3est', u'Flags', u'Flags2', u'ALMAname',
     u'match_alma', u'distance_alma', u'match_vlbi', u'distance_vlbi',
     u'match_grid', u'distance_grid', u'MEASUREMENT_ID', u'CATALOGUE_ID',
     u'RA_alma', u'RA_UNCERTAINTY_alma', u'DEC_alma', u'DEC_UNCERTAINTY_alma',
     u'FREQUENCY', u'FLUX', u'FLUX_UNCERTAINTY', u'DEGREE',
     u'DEGREE_UNCERTAINTY', u'ANGLE', u'ANGLE_UNCERTAINTY', u'EXTENSION',
     u'FLUXRATIO', u'ORIGIN', u'DATE_OBSERVED', u'DATE_CREATED', u'VALID',
     u'UVMIN', u'UVMAX', u'Category', u'IVS', u'name', u'ra_h', u'ra_m',
     u'ra_s', u'dec_d', u'dec_m', u'dec_s', u'ra_err', u'dec_err',
     u'ra_dec_corr', u'numobs', u'RA_vlbi', u'DEC_vlbi'], dtype='object')

atca_sel_AVG = pd.merge(
    atca_sel_AV, grids, left_on='match_grid', right_index=True, how='left')

atca_sel_AVG = atca_sel_AVG[
    [u'ATCA_name', u'RA_atca', u'errRA_atca', u'DEC_atca', u'errDEC_atca',
     u'f20', u'ef20', u'f93', u'B3est', u'Flags', u'Flags2', u'ALMAname',
     u'match_alma', u'distance_alma', u'distance_vlbi', u'distance_grid',
     u'CATALOGUE_ID', u'RA_alma', u'RA_UNCERTAINTY_alma', u'DEC_alma',
     u'DEC_UNCERTAINTY_alma', u'FREQUENCY', u'FLUX', u'FLUX_UNCERTAINTY',
     u'DATE_OBSERVED', u'VALID', u'IVS', u'name', u'RA_vlbi', u'DEC_vlbi',
     u'names']]

atca_sel_AVG.columns = pd.Index(
    [u'ATCA_name', u'RA_atca', u'errRA_atca', u'DEC_atca', u'errDEC_atca',
     u'f20', u'ef20', u'f93', u'B3est', u'Flags', u'Flags2', u'ALMAname',
     u'SOURCE_ID', u'distance_alma', u'distance_vlbi', u'distance_grid',
     u'CATALOGUE_ID', u'RA_alma', u'RA_UNCERTAINTY_alma', u'DEC_alma',
     u'DEC_UNCERTAINTY_alma', u'FREQUENCY', u'FLUX', u'FLUX_UNCERTAINTY',
     u'DATE_OBSERVED', u'VALID', u'IVS', u'name', u'RA_vlbi', u'DEC_vlbi',
     u'Grid_close'], dtype='object')


weak = pd.read_table('/home/itoledo/Work/calscripts/WeakSourceList2.csv',
                     sep=',', header=0)

vla = pd.read_table(
    'Work/calscripts/vla_candidates2.csv', sep=' ', skipinitialspace=True
)[['vla_name', 'p_err', 'ra', 'dec', 'est_flux']]

vla['RA'] = vla.apply(lambda r: split_ra(r['ra'].split(':')), axis=1)
vla['DEC'] = vla.apply(lambda r: split_dec(r['dec'].split(':')), axis=1)

cross_weak = vla.apply(
    lambda r: match_weak(r['RA'], r['DEC'], r['vla_name'], weak), axis=1)

vla_weak = pd.merge(vla, cross_weak, left_on='vla_name', right_on='name')

cross_vla_vlbi = vla.apply(
    lambda r: match_vlbi(r['RA'], r['DEC'], r['vla_name'], vlbi), axis=1)

cross_vla_alma = vla.apply(
    lambda r: match_alma(
        r['RA'], r['DEC'], r['vla_name'], sources[sources.CATALOGUE_ID == 5]),
    axis=1)
