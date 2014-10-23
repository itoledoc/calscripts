__author__ = 'itoledo'
import datetime
from xmlrpclib import ServerProxy
from pylab import *
import getpass
import time
import sys, math
import socket
import re # for matchNames
import os
import fileinput
from types import NoneType


sys.path.append('/users/aod/AIV/science/analysis_scripts')
import tmUtils as tm

NULL_AS_FLOAT = 1.7976931348623157e+308
NULL_AS_FLOAT_STRING = '1.7976931348623157E308'
NULL_AS_STRING = 'null'


class CalibratorCatalogUpdate:
    def __init__(self, name=None, tunnel=False,
                 server='http://sourcecat.osf.alma.cl/sourcecat/xmlrpc'):

        self.username = getpass.getuser()

        if (tunnel):
            self.s = ServerProxy('http://localhost:8080/sourcecat/xmlrpc')

        else:
            self.s = ServerProxy(server)
            #self.s = ServerProxy('http://terpsichore.sco.alma.cl:8080/sourcecat/xmlrpc')

        self.connectionFailed = False  # added by T. Hunter

        try:
            self.catalogList=self.s.sourcecat.listCatalogues()

        except:
            print "Connection failed."
            if (tunnel):
                print "Before calling this function with tunnel=True, be sure to set up an ssh tunnel like so:"
                print "    ssh -N -f -L 8080:pomona.osf.alma.cl:8080 %s@tatio.aiv.alma.cl" % os.getenv("USER")
                print "(or ssh -N -f -L 8080:pomona.osf.alma.cl:8080 %s@login.alma.cl)" % os.getenv("USER")
                print "Alternatively, to access the webpage manually, open a SOCKS v5 proxy tunnel by:"
                print "(1) Use FoxyProxy to set pattern http://pomona.osf.alma.cl:8080/* to localhost port 8080"
                print "(2) ssh -D 8080 tatio.aiv.alma.cl"
                print "(3) surf to http://pomona.osf.alma.cl:8080/sourcecatweb/sourcecat"
            self.connectionFailed = True  # added by T. Hunter
            return

        self.catalogues=[]
        self.basepath = '/mnt/jaosco/data/Calsurvey'
        # for testing
        #self.basepath = '/users/dbarkats/cal_survey_tmp'

        for i in range(size(self.catalogList)):
            self.catalogues.append(self.catalogList[i]['catalogue_id'])

        self.typesList = self.s.sourcecat.listTypes()
        self.types = []

        for i in range(size(self.typesList)):
            self.types.append(self.typesList[i]['type_id'])
        if tunnel==False:
            self.hostname= socket.gethostname()
            if ('casa' not in self.hostname and 'scops' not in self.hostname and 'alma.cl' in self.hostname):
                print "### WARNING: This script is only intended to be run from scops0X/casa0X.sco.alma.cl. ### \n### Unless you are doing specific testing, only run this from scops0X/casa0X.### \n"
        #    sys.exit()


    def addName(self, filename):
        f = open(filename, 'r')
        lines = f.readlines()
        for line in lines[1:]:
            _line = line.split(',')
            sourceId = int(_line[1])
            sourceName = str(_line[3])
            print sourceId, sourceName

            self.s.sourcecat.addSourceName(sourceId, sourceName)

    def addnewMeasurement(self, filename, dryrun=True):
        """
        Single usage function to add new sources from
        southern_extra_with_RADEC.txt
        provided by Ed Fomalont. File with all info
        is located in osf-red:/data/cal_survey/southern_extra_with_RADEC.txt

        re-used Dec 2013 to add a  at20g catalog sources which also had a VLBI
        position and above 0.1 Jy.

        """
        count = 0

        f = open(filename, 'r')
        lines = f.readlines()
        for line in lines[1:]:

            if line.startswith('#'):
                continue

            _line = line.split(',')
            sourceId = int(_line[1].strip())
            sourceName = _line[2].strip()

            ra_decimal = float(_line[3])   # in deg

            ra_uncertainty = float(_line[4])
            dec_decimal = float(_line[5])

            dec_uncertainty = float(_line[6])

            # Put frequency, flux, and flux uncertainty from this new
            # measurement
            frequency = float(_line[7])
            flux = float(_line[8])
            flux_uncertainty = NULL_AS_FLOAT
            degree = NULL_AS_FLOAT
            degree_uncertainty = NULL_AS_FLOAT
            angle = NULL_AS_FLOAT
            angle_uncertainty = NULL_AS_FLOAT

            uvmin = NULL_AS_FLOAT
            uvmax = NULL_AS_FLOAT

            # will eventually disappear
            fluxratio = float(1.0)

            date_observed = tm.get_datetime_from_isodatetime('2014-04-26')
            origin = 'rfc_2014b_cat.txt'
            catalogue_id = long(41)

            print(
                sourceName, sourceId, ra_decimal, ra_uncertainty, dec_decimal,
                dec_uncertainty, frequency, flux, flux_uncertainty, degree,
                degree_uncertainty, angle, angle_uncertainty,
                fluxratio, uvmin, uvmax, date_observed, origin, catalogue_id)

            if dryrun == True:
                checkAdd = 'n'
                count = count + 1

            else:
                checkAdd = 'y'

            if checkAdd == 'y':

                measurementId = self.s.sourcecat.addMeasurement(
                    sourceId, ra_decimal, ra_uncertainty,
                    dec_decimal, dec_uncertainty,
                    frequency, flux, flux_uncertainty,
                    degree, degree_uncertainty, angle, angle_uncertainty,
                    fluxratio, uvmin, uvmax, date_observed, origin,
                    catalogue_id)

                count += 1
                if measurementId != 0:
                    print "Making this new Measurement %i  on source %i Valid" %(measurementId, sourceId)
                    setValid = self.s.sourcecat.setMeasurementState(measurementId, True)
                    if setValid == True:
                        print "Measurement %i  on source %i is now  Valid" %(measurementId, sourceId)
                else:
                    print "Sorry adding this last measurement to source %i %s failed" %(sourceId, name)
            else:
                continue

        print " \n You have added %i measurements to the calibrator catalog" % count
        return


    def matchName(self, sourceName, Id = None, verbose = 0):
        """
        Name matching function to check that the name provided (sourceName)
        is the same as the one in the catalog (realName)
        Given a sourceName,  it returns the realName in the catalog. As long as the realName
        and the sourceName are the same (within the starting J), it does not complain.
        Also, returns the official name of this source: JXXXX-XXXX

        """
        realName = None
        sourceId = None
        officialName = None
        nameList = []

        # remove any star at the end of the sourceName
        sourceName = sourceName.strip('*')

        if Id == None:
            # get source Id from source Name
            sourceId = self.getSourceIdFromSourceName('%%%s%%'%sourceName)
        else:
            sourceId = Id
        if sourceId != None:
            Names = self.getSourceNameFromId(sourceId)
            for name in Names:
                rp = name['source_name']
                nameList.append(rp)
                if sourceName.lower() in rp.lower():
                    realName = rp

            # find official name
            #print nameList
            for name in nameList:

                ab = re.match("J[0-9]{4}.[0-9]{4}",name)
                #print name, ab
                if ab:
                    officialName = ab.group()
                    break
                else:
                    officialName = None

            if (verbose):
                print "Name given: %s, SourceID:%d, Matched name: %s, Official Name = %s"%(sourceName, sourceId, realName, officialName)
            # if realName.startswith('J') and realName.replace('J','') == sourceName:
            return sourceId, realName, officialName
            #elif sourceName != realName:
                 #usename= raw_input("Name does not match catalog name, Use matched name (y) or exit any other key)?")
            #    print "WARNING: realName:%s in catalog does NOT match sourceName given: %s " %(realName, sourceName)
            #    return sourceId, realName
            #else:
            #    return sourceId, realName
        else:
            # print "WARNING: No SourceID found for this sourceName :%s" %sourceName
            return sourceId, realName, officialName

    def getSourceIdFromSourceName(self,sourceName) :
        """
        Searches for all sources given the name
        Returns sourceId
        Note that this search only finds sources which have a valid measurement

        """
        measurements=self.wrapSearch(name=sourceName, limit = 1, sourceBandLimit = 1)
        ids=[]
        for i in range(size(measurements)):
            ids.append(measurements[i]['source_id'])

        sourceId=unique(ids)
        if size(sourceId) == 0:
            # print "Sorry, could not find any sources with name %s.Try to add wildcard %% before or after source name" %sourceName
            return
        else:
            return int(sourceId[0])


    def getSourceNameFromId(self,sourceId):
        """
        Returns  the source names given the sourceId
        Note that this search returns even if the source has no measurements

        """
        source = self.s.sourcecat.getSourcePlus(sourceId,False)
        sourceNames = source['names']

        return sourceNames

    def wrapSearch(self,sourceBandLimit = 1, limit = 10,catalogues = None,types = None,name = '',ra = -1.0,
                   dec = -1.0, radius = -1.0,ranges = [],fLower = -1.0, fUpper = -1.0,
                   fluxMin = -1.0, fluxMax = -1.0, degreeMin = -1.0, degreeMax = -1.0,
                   angleMin = -361.0, angleMax = -361.0, sortBy = 'source_id', asc = True,
                   searchOnDate=False, dateCriteria = 0, date ='',onlyValid = True,
                   uvmin = -1.0, uvmax = -1.0):
        """
        This is the basic search. It is a wrapper around the  catalog's
        searching function
        OnlyValid = True means we find only valid sources. Not invalid ones.
        date has format '2013-01-01'
        limit is a limit on the number of sources
        sourceBandLimit is the number of measurements per source
        """

        if catalogues == None:
            catalogues=self.catalogues
        elif isinstance(catalogues,list) == False :
            print "Catalogues must be a list of integers ([1,2,3]. Try again. Available catalogues are:"
            print self.catalogList
            sys.exit()

        if types == None:
            types=self.types
        elif isinstance(catalogues,list) == False :
            print "Types must be a list of integers ([1,2,3]. Try again. Available types are:"
            print self.typesList
            sys.exit()

        # pre 9.1.3
        #measurements = self.s.sourcecat.searchMeasurements(limit,catalogues,types,name,ra, dec,
        #                                                            radius,ranges,fLower, fUpper, fluxMin,
        #                                                            fluxMax, degreeMin,degreeMax, angleMin,
        #                                                            angleMax ,sortBy ,asc,
        #                                                            searchOnDate,dateCriteria,date)
        try:
            #print "Searching using searchMeasurements103 with a source limit %d and measurements/source limit = %d"%(limit, sourceBandLimit)
            measurements = self.s.sourcecat.searchMeasurements103(sourceBandLimit,limit,catalogues,types,name,ra, dec,
                                                                  radius,ranges,fLower, fUpper, fluxMin,
                                                                  fluxMax, degreeMin,degreeMax, angleMin,
                                                                  angleMax ,sortBy ,asc,
                                                                  searchOnDate,dateCriteria,date,
                                                                  onlyValid,uvmin,uvmax)
        except:
            #print "Searching using searchMeasurements913"
            measurements = self.s.sourcecat.searchMeasurements913(limit,catalogues,types,name,ra, dec,
                                                                  radius,ranges,fLower, fUpper, fluxMin,
                                                                  fluxMax, degreeMin,degreeMax, angleMin,
                                                                  angleMax ,sortBy ,asc,
                                                                  searchOnDate,dateCriteria,date,
                                                                  onlyValid,uvmin,uvmax)

        measurements = checkForPseudoNullsInMeasurements(measurements)
        return measurements

def checkForPseudoNullsInMeasurements(measurements):
    for m in measurements:
        m['ra_uncertainty'] = convertPseudoNullToNone(m['ra_uncertainty'])
        m['dec_uncertainty'] = convertPseudoNullToNone(m['dec_uncertainty'])
        m['flux_uncertainty'] = convertPseudoNullToNone(m['flux_uncertainty'])
        m['degree'] = convertPseudoNullToNone(m['degree'])
        m['degree_uncertainty'] = convertPseudoNullToNone(m['degree_uncertainty'])
        m['angle'] = convertPseudoNullToNone(m['angle'])
        m['angle_uncertainty'] = convertPseudoNullToNone(m['angle_uncertainty'])
        m['origin'] = convertPseudoNullToNone(m['origin'])
        m['fluxratio'] = convertPseudoNullToNone(m['fluxratio'])
        m['uvmin'] = convertPseudoNullToNone(m['uvmin'])
        m['uvmax'] = convertPseudoNullToNone(m['uvmax'])
    return measurements

def convertPseudoNullToNone(value):
    if (value == NULL_AS_STRING)  or (value == '') or (value == NULL_AS_FLOAT) or (value == NULL_AS_FLOAT_STRING) \
           or (value == '0') or (value =='0.0') or (value == 0.0):
        return None
    return value