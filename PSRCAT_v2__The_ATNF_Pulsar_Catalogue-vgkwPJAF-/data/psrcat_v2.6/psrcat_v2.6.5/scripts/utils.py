import sqlite3
import pandas as pd
import numpy as np
import math
from datetime import datetime

# Copyright: CSIRO 2024
# Author: Agastya Kapur: agastya.kapur@csiro.au, George Hobbs: George.Hobbs@csiro.au, Lawrence Toomey, Andrew Zic 


# Connect to database, default is "psrcat2.db"
def connect_db(db="psrcat2.db"):
    con = sqlite3.connect(db)
    return con


# GETTING DATA OUT OF DB

# Get current version ID
def getVersionID(con,version=None):
    cur = con.cursor()
    if version is None:
        versionIDtemp = cur.execute("SELECT version_id FROM version ORDER BY version_id DESC LIMIT 1")
    else:
        versionIDtemp = cur.execute("SELECT version_id FROM version WHERE version LIKE ?",(version,))
    versionIDinDB = versionIDtemp.fetchone()
    if versionIDinDB:
        versionID = versionIDinDB[0]
        return versionID
    return None

# Get Pulsar ID from Jname 
def getPulsarIDfromJName(con,jname):
    cur = con.cursor()
    pulsarIDtemp = cur.execute("SELECT DISTINCT pulsar_id FROM pulsar WHERE jname LIKE ?",(jname,))
    pulsarIDinDB = pulsarIDtemp.fetchone()
    if pulsarIDinDB:
        pulsarID = pulsarIDinDB[0]
        return pulsarID 
    return None

def getPulsarIDfromName(con,name):
    cur = con.cursor()
    pulsarIDtemp = cur.execute("SELECT DISTINCT pulsar.pulsar_id FROM pulsar LEFT JOIN name on pulsar.pulsar_id=name.pulsar_id WHERE (jname LIKE ? OR name LIKE ?)",(name,name))
    pulsarIDinDB = pulsarIDtemp.fetchone()
    if pulsarIDinDB:
        pulsarID = pulsarIDinDB[0]
        return pulsarID
    return None

# Get Survey ID from Survey Short Label
def getSurveyIDfromLabel(con,label):
    cur = con.cursor()
    surveyIDtemp = cur.execute("SELECT DISTINCT survey_id FROM survey WHERE shortLabel LIKE ?",(label,))
    surveyIDinDB = surveyIDtemp.fetchone()
    if surveyIDinDB:
        surveyID = surveyIDinDB[0]
        return surveyID
    return None

# Get Survey ID from Pulsar ID
# Think about getting discovery survey, This gives only the discovery survey ID.
def getSurveyIDfromPulsar(con,pulsar_id=None,jname=None):
    if(pulsar_id==None and jname==None):
        return None
    cur = con.cursor()
    if(jname is not None):
        pulsar_id=getPulsarIDfromJname(con=con,jname=jname)
    surveyIDtemp = cur.execute("SELECT DISTINCT survey_id FROM survey WHERE pulsar_id LIKE ?",(pulsar_id,))
    surveyIDinDB = surveyIDtemp.fetchone()
    if surveyIDinDB:
        surveyID = surveyIDinDB[0]
        return surveyID
    return None

# Get observing system ID using systemLabel, centralFrequency, bandwidth, telescope and approximate.
def getObservingSystemID(con,systemLabel,centralFrequency,bandwidth,telescope,approximate):
    cur = con.cursor()
    observingSystemIDtemp = cur.execute("SELECT DISTINCT observingSystem_id FROM observingSystem WHERE (systemLabel LIKE ? AND centralFrequency LIKE ? AND bandwidth LIKE ? AND telescope LIKE ? AND approximate LIKE ?)",(systemLabel,centralFrequency,bandwidth,telescope,approximate))
    observingSystemIDinDB = observingSystemIDtemp.fetchone()
    if observingSystemIDinDB:
        observingSystemID = observingSystemIDinDB[0]
        return observingSystemID
    return None

# Get parameterTypeID from v2 label. i.e. F,P,DM etc. Will return None if F0,P0 etc. are entered.
def getParameterTypeID(con,label):
    cur = con.cursor()
    parameterTypeIDtemp = cur.execute("SELECT DISTINCT parameterType_id FROM parameterType WHERE label LIKE ?",(label,))
    parameterTypeIDinDB = parameterTypeIDtemp.fetchone()
    if parameterTypeIDinDB:
        parameterTypeID = parameterTypeIDinDB[0]
        return parameterTypeID
    return None

# Get ancillaryID for parameterID, value, desc
def getAncillaryID(con,parameter_id,value,description):
    cur = con.cursor()
    ancillaryIDtemp = cur.execute("SELECT DISTINCT ancillary_id FROM ancillary WHERE parameter_id LIKE ? AND value LIKE ? AND description LIKE ?",(parameter_id,value,description))
    ancillaryIDinDB = ancillaryIDtemp.fetchone()
    if ancillaryIDinDB:
        ancillaryID = ancillaryIDinDB[0]
        return ancillaryID
    return None

# Get fitparametersID for units,ephem,clock and citation.
def getFitParametersID(con,units,ephemeris,clock,citation_id):
    cur = con.cursor()
    #print(units,ephemeris,clock)
    fitParametersIDtemp = cur.execute("SELECT DISTINCT fitParameters_id FROM fitParameters WHERE units LIKE ? AND ephemeris LIKE ? AND clock LIKE ? AND citation_id LIKE ?",(units,ephemeris,clock,citation_id))
    fitParametersIDinDB = fitParametersIDtemp.fetchone()
    #print(fitParametersIDinDB)
    if fitParametersIDinDB:
        fitParametersID = fitParametersIDinDB[0]
        return fitParametersID
    return None

# Check if parameter exists in database already. 
def getParameterUniqCheck(con,pulsar_id,parameterType_id,citation_id,value,timeDerivative,companionNumber,referenceTime):
    cur = con.cursor()
    parameterIDtemp = cur.execute("SELECT DISTINCT parameter_id FROM parameter WHERE (pulsar_id LIKE ? AND parameterType_id LIKE ? AND citation_id LIKE ? AND value LIKE ? AND timeDerivative LIKE ? AND companionNumber LIKE ? AND referenceTime LIKE ?)",(pulsar_id,parameterType_id,citation_id,value,timeDerivative,companionNumber,referenceTime))
    parameterIDinDB = parameterIDtemp.fetchone()
    if parameterIDinDB:
        parameterID = parameterIDinDB[0]
        return parameterID
    return None
# Getting a timing model

# In the works right now
def getTimingModel(con,jname):
    return None

# Get associationTypeID based on associationType label
def getAssociationTypeID(con,label):
    cur = con.cursor()
    label = label.strip()
    associationTypeIDtemp = cur.execute("SELECT DISTINCT associationType_id FROM associationType WHERE label LIKE ?",(label,))
    associationTypeIDinDB = associationTypeIDtemp.fetchone()
    if associationTypeIDinDB:
        associationTypeID = associationTypeIDinDB[0]
        return associationTypeID
    return None

# Get sourceTypeOptionID based on sourceTypeOptions label
def getSourceTypeOptionsID(con,label):
    cur = con.cursor()
    label=label.strip()
    sourceTypeOptionsIDtemp = cur.execute("SELECT DISTINCT sourceTypeOptions_id FROM sourceTypeOptions WHERE label LIKE ?",(label,))
    sourceTypeOptionsIDinDB = sourceTypeOptionsIDtemp.fetchone()
    if sourceTypeOptionsIDinDB:
        sourceTypeOptionsID = sourceTypeOptionsIDinDB[0]
        return sourceTypeOptionsID
    return None

# Get binaryTypeOptionsID based on binaryTypeOptions label 
def getBinaryTypeOptionsID(con,label):
    cur = con.cursor()
    label=label.strip()
    binaryTypeOptionsIDtemp = cur.execute("SELECT DISTINCT binaryTypeOptions_id FROM binaryTypeOptions WHERE label LIKE ?",(label,))
    binaryTypeOptionsIDinDB = binaryTypeOptionsIDtemp.fetchone()
    if binaryTypeOptionsIDinDB:
        binaryTypeOptionsID = binaryTypeOptionsIDinDB[0]
        return binaryTypeOptionsID
    return None

# Get associationID for pulsar
def getAssociation(con,pulsar_id,associationType_id,citation_id,name):
    cur = con.cursor()
    associationIDtemp = cur.execute("SELECT DISTINCT association_id FROM association WHERE pulsar_id LIKE ? AND associationType_id LIKE ? and citation_id LIKE ? AND name LIKE ?",(pulsar_id,associationType_id,citation_id,name))
    associationIDinDB = associationIDtemp.fetchone()
    if associationIDinDB:
        associationID = associationIDinDB[0]
        return associationID
    return None

# Get sourceTypeID for pulsar
def getSourceType(con,pulsar_id,sourceTypeOptions_id,citation_id):
    cur = con.cursor()
    sourceTypeIDtemp = cur.execute("SELECT DISTINCT sourceType_id FROM sourceType WHERE pulsar_id LIKE ? and sourceTypeOptions_id LIKE ? and citation_id LIKE ?",(pulsar_id,sourceTypeOptions_id,citation_id))
    sourceTypeIDinDB = sourceTypeIDtemp.fetchone()
    if sourceTypeIDinDB:
        sourceTypeID = sourceTypeIDinDB[0]
        return sourceTypeID
    return None

# Get binaryTypeID for pulsar
def getBinaryType(con,pulsar_id,binaryTypeOptions_id,citation_id):
    cur = con.cursor()
    binaryTypeIDtemp = cur.execute("SELECT DISTINCT binaryType_id FROM binaryType WHERE pulsar_id LIKE ? and binaryTypeOptions_id LIKE ? and citation_id LIKE ?",(pulsar_id,binaryTypeOptions_id,citation_id))
    binaryTypeIDinDB = binaryTypeIDtemp.fetchone()
    if binaryTypeIDinDB:
        binaryTypeID = binaryTypeIDinDB[0]
        return binaryTypeID
    return None

def getCitationIDFromV1Label(con,v1label):
    cur = con.cursor()
    citationIDtemp = cur.execute("SELECT DISTINCT citation_id FROM citation WHERE v1label LIKE ?",(v1label,))
    citationIDinDB = citationIDtemp.fetchone()
    if citationIDinDB:
        citationID = citationIDinDB[0]
        return citationID
    return None
# ADDING

# Add a linked set. Return linkedSetID



def addLinkedSet(con,citation_id,description,commit=True):
    cur = con.cursor()
    try:
        cur.execute("INSERT INTO linkedSet(linkedSet_id,citation_id,description) VALUES (NULL,?,?);",(citation_id,description))
        if(commit==True):
            con.commit()
            linkedSetID = cur.lastrowid
            return linkedSetID
        else:
            return None
    except sqlite3.IntegrityError:
        con.rollback()

def addPulsarJname(con,jname,survey_id,citation_id,commit=True):
    # to do
    pulsarIDinDB = getPulsarIDfromJName(con,jname)
    if pulsarIDinDB:
        return pulsarIDinDB
    else:
        cur = con.cursor()
        try:
            cur.execute("INSERT OR IGNORE INTO pulsar (pulsar_id,jname,survey_id,citation_id,entryDate) VALUES (NULL,?,?,?,DATETIME('NOW'))",(jname,survey_id,citation_id))
            if(commit==True):
                con.commit()
                pulsarID = cur.lastrowid
                print(f"Added pulsar: id:{pulsarID} name:{jname} sur_id:{survey_id} cit_id:{citation_id}")
                return pulsarID
            else:
                return None
        except sqlite3.IntegrityError:
            con.rollback() 

# Adding Parameter to DB

def addParameter(con,pulsar_id,citation_id,linkedSet_id,fitParameters_id,observingSystem_id,parameterType_id,timeDerivative,companionNumber,value,uncertainty,referenceTime,commit=True):
    cur = con.cursor()
    parameter_id = getParameterUniqCheck(con,pulsar_id,parameterType_id,citation_id,value,timeDerivative,companionNumber,referenceTime)
    if not parameter_id:
        try:
            cur.execute("INSERT INTO parameter (parameter_id,pulsar_id,citation_id,linkedSet_id,fitParameters_id,observingSystem_id,parameterType_id,timeDerivative,companionNumber,value,uncertainty,referenceTime,entryDate) VALUES (NULL,?,?,?,?,?,?,?,?,?,?,?,DATETIME('now'))",(pulsar_id,citation_id,linkedSet_id,fitParameters_id,observingSystem_id,parameterType_id,timeDerivative,companionNumber,value,uncertainty,referenceTime))
            if(commit==True):
                con.commit()
                parameterID = cur.lastrowid
                return parameterID
            else:
                return None
        except sqlite3.IntegrityError:
            con.rollback()
    else:
        return None
# Add Survey

def addSurvey(con,label,shortLabel,telescope,receiver,commit=True):
    surveyIDinDB = getSurveyIDfromLabel(con,label)
    if surveyIDinDB:
        return surveyIDinDB
    else:
        cur = con.cursor()
        try:
            cur.execute("INSERT OR IGNORE INTO survey (survey_id,label,shortLabel,telescope,receiver,entryDate) VALUES (NULL,?,?,?,?,DATETIME('now'))",(label,shortLabel,telescope,receiver))
            if(commit==True):
                con.commit()
                surveyID = cur.lastrowid
                return surveyID
            else:
                return None
        except sqlite3.IntegrityError:
            con.rollback()
    
def addSurveyToPulsar(con,survey_id,pulsar_id,commit=True):
    cur = con.cursor()
    try:
        cur.execute("INSERT INTO surveyToPulsar (surveyToPulsar_id,survey_id,pulsar_id) VALUES (NULL,?,?)",(survey_id,pulsar_id))
        if(commit==True):
            con.commit()
            surveyToPulsarID = cur.lastrowid
            return surveyToPulsarID
        else:
            return None
    except sqlite3.IntegrityError:
        con.rollback()

# Add citation

def addCitation(con,v1label,label,title,author,journal,year,month,volume,number,pages,doi,url,version_id,commit=True):
    citationIDinDB = getCitationIDFromV1Label(con,v1label)
    if citationIDinDB:
        cur = con.cursor()
        try:
            cur.execute("UPDATE citation SET label=?,title=?,author=?,journal=?,year=?,month=?,volume=?,number=?,pages=?,doi=?,url=? WHERE citation_id=?",(label,title,author,journal,year,month,volume,number,pages,doi,url,citationIDinDB))
            if(commit==True):
                con.commit()
            return citationIDinDB
        except sqlite3.IntegrityError:
            con.rollback()
    else:
        cur = con.cursor()
        try:
            cur.execute("INSERT INTO citation (citation_id,v1label,label,title,author,journal,year,month,volume,number,pages,doi,url,version_id) VALUES (NULL,?,?,?,?,?,?,?,?,?,?,?,?,?)",(v1label,label,title,author,journal,year,month,volume,number,pages,doi,url,version_id))
            if(commit==True):
                con.commit()
                citationID = cur.lastrowid
                return citationID
            else:
                return None
        except sqlite3.IntegrityError:
            con.rollback()
    return None
        
# Add ParameterType

def addParameterType(con,label,unit,description,timingFlag,dataType,commit=True):
    #TODO
    return None

# Add Observing System
def addObservingSystem(con,systemLabel,centralFrequency,bandwidth,telescope,approximate,commit=True):
    observingSystemIDinDB = getObservingSystemID(con,systemLabel,centralFrequency,bandwidth,telescope,approximate)
    if observingSystemIDinDB:
        return observingSystemIDinDB
    else:
        cur = con.cursor()
        try:
            cur.execute("INSERT INTO observingSystem (observingSystem_id,systemLabel,centralFrequency,bandwidth,telescope,approximate,entryDate) VALUES (NULL,?,?,?,?,?,DATETIME('now'))",(systemLabel,centralFrequency,bandwidth,telescope,approximate))
            if(commit==True):
                con.commit()
                observingSystemID = cur.lastrowid
                return observingSystemID
            else:
                return None
        except sqlite3.IntegrityError:
            con.rollback()
    return None
# Set parameter values

def addAncillary(con,parameter_id,value,description,commit=True):
    ancillaryIDinDB = getAncillaryID(con,parameter_id,value,description)
    if ancillaryIDinDB:
        return ancillaryIDinDB
    else:
        cur = con.cursor()
        try:
            cur.execute("INSERT INTO ancillary (ancillary_id,parameter_id,value,description) VALUES (NULL,?,?,?)",(parameter_id,value,description))
            if(commit==True):
                con.commit()
                ancillaryID = cur.lastrowid
                return ancillaryID
            else:
                return None
        except sqlite3.IntegrityError:
            con.rollback()

def addFitParameters(con,units,ephemeris,clock,citation_id,commit=True):
    fitParametersIDinDB = getFitParametersID(con,units,ephemeris,clock,citation_id)
    if fitParametersIDinDB:
        return fitParametersIDinDB
    else:
        cur = con.cursor()
        try:
            cur.execute("INSERT INTO fitParameters (fitParameters_id,units,ephemeris,clock,citation_id) VALUES (NULL,?,?,?,?)",(units,ephemeris,clock,citation_id))
            if(commit==True):
                con.commit()
                fitParameterID = cur.lastrowid
                return fitParameterID
            else:
                return None
        except sqlite3.IntegrityError:
            con.rollback()
    return None


def addDistance(con,pulsar_id,citation_id,value,uncertainty,label,commit=True):
    # Not bothering checking for duplicate values because of UNIQUE constraint in table
    print("Functionality is now depricated")
    print("Distances are now in the parameter table")
    return None
    if(label == "DIST_DM"):
        label = "DIST_YMW16"
    elif(label == "DIST_DM1"):
        label = "DIST_NE2001"
    if(label == "DIST_YMW16" or label == "DIST_NE2001"):
        uncertainty = None
    cur = con.cursor()
    try:
        cur.execute("INSERT INTO distance (distance_id,pulsar_id,citation_id,value,uncertainty,label) VALUES (NULL,?,?,?,?,?)",(pulsar_id,citation_id,value,uncertainty,label))
        if(commit==True):
            con.commit()
            distanceID = cur.lastrowid
            return distanceID
        else:
            return None
    except sqlite3.IntegrityError:
        con.rollback()
    return None
def addAssociation(con,pulsar_id,name,citation_id,confidence,associationType_id=None,label=None,commit=True):
    if(associationType_id is None and label is None):
        return None
    cur = con.cursor()
    if label:
        associationType_id = getAssociationTypeID(con,label)
    association_id = getAssociation(con,pulsar_id,associationType_id,citation_id,name)
    if not association_id:
        try:
            cur.execute("INSERT INTO association (association_id,pulsar_id,associationType_id,citation_id,confidence,name) VALUES (NULL,?,?,?,?,?)",(pulsar_id,associationType_id,citation_id,confidence,name))
            if(commit==True):
                con.commit()
                associationID = cur.lastrowid
                return associationID
            else:
                return None
        except sqlite3.IntegrityError:
            con.rollback()
    else:
        return None
    return None

def addBinaryType(con,pulsar_id,citation_id,confidence,binaryTypeOptions_id=None,label=None,commit=True):
    if(binaryTypeOptions_id is None and label is None):
        return None
    cur = con.cursor()
    if label:
        binaryTypeOptions_id = getBinaryTypeOptionsID(con,label)
    binaryType_id = getBinaryType(con,pulsar_id,binaryTypeOptions_id,citation_id)
    if not binaryType_id:
        try:
            cur.execute("INSERT INTO binaryType (binaryType_id,pulsar_id,binaryTypeOptions_id,confidence,citation_id) VALUES (NULL,?,?,?,?)",(pulsar_id,binaryTypeOptions_id,confidence,citation_id))
            if(commit==True):
                con.commit()
                binaryTypeID = cur.lastrowid
                return binaryTypeID
            else:
                return None
        except sqlite3.IntegrityError:
            con.rollback()
    else:
        return None    
    return None

def addSourceType(con,pulsar_id,citation_id,confidence,sourceTypeOptions_id=None,label=None,commit=True):
    if(sourceTypeOptions_id is None and label is None):
        return None
    cur = con.cursor()
    if label:
        sourceTypeOptions_id = getSourceTypeOptionsID(con,label)
    sourceType_id = getSourceType(con,pulsar_id,sourceTypeOptions_id,citation_id)
    if not sourceType_id:
        try:
            cur.execute("INSERT INTO sourceType (sourceType_id,pulsar_id,sourceTypeOptions_id,confidence,citation_id) VALUES (NULL,?,?,?,?)",(pulsar_id,sourceTypeOptions_id,confidence,citation_id))
            if(commit==True):
                con.commit()
                sourceTypeID = cur.lastrowid
                return sourceTypeID
            else:
                return None
        except sqlite3.IntegrityError:
            con.rollback()
    else:
        return None
    return None
def addDerived(con,parameter_id,method,methodVersion,commit=True):
    cur = con.cursor()
    try:
        cur.execute("INSERT INTO derived (derived_id,parameter_id,method,methodVersion) VALUES (NULL,?,?,?)",(parameter_id,method,methodVersion))
        if(commit==True):
            con.commit()
            deriveID = cur.lastrowid
            return deriveID
        else:
            return None
    except sqlite3.IntegrityError:
        con.rollback()
    return None
    

def addDerivedFromParameter(con,derived_id,parameter_id,commit=True):
    cur = con.cursor()
    try:
        cur.execute("INSERT INTO derivedFromParameter (derivedFromParameter_id,derived_id,parameter_id) VALUES (NULL,?,?)",(derived_id,parameter_id))
        if(commit==True):
            con.commit()
            derivedFromParameterID = cur.lastrowid
            return derivedFromParameterID
        else:
            return None
    except sqlite3.IntegrityError:
        con.rollback()
    return None

def addAssociationType():
    return None

def addBinaryTypeOptions():
    return None

def addSourceTypeOptions():
    return None

def addTag(con,tagLabel,tagString,commit=True):
    cur = con.cursor()
    tagIDtemp = cur.execute("SELECT DISTINCT tag_id FROM tag WHERE tagLabel LIKE ? and tagString LIKE ?",(tagLabel,tagString))
    tagIDinDB = tagIDtemp.fetchone()
    if tagIDinDB:
        tagID = tagIDinDB[0]
        return tagID
    cur.execute("INSERT INTO tag (tag_id,tagLabel,tagString,entryDate) VALUES (NULL,?,?,DATETIME('now'))",(tagLabel,tagString))
    if(commit==True):
        con.commit()
        sourceTypeID = cur.lastrowid
        return sourceTypeID
    else:
        return None
    return None

def addTagToPulsar(con,pulsar_id=None,jname=None,tag_id=None,tagLabel=None,tagString=None,commit=True):
    if(pulsar_id is None and jname is None):
        return None
    if(tag_id is None and tagLabel is None and tagString is None):
        return None
    if not pulsar_id:
        pulsar_id = getPulsarIDfromJName(con,jname)
    if pulsar_id is None:
        return None
    if not tag_id:
        tag_id = addTag(con,tagLabel,tagString)
    if tag_id is None:
        return None
    cur = con.cursor()
    tagToPulsarIDtemp = cur.execute("SELECT DISTINCT tagToPulsar_id FROM tagToPulsar WHERE pulsar_id LIKE ? and tag_id LIKE ?",(pulsar_id,tag_id))
    tagToPulsarIDinDB = tagToPulsarIDtemp.fetchone()
    if tagToPulsarIDinDB:
        tagToPulsarID = tagToPulsarIDinDB[0]
        return tagToPulsarID
    
    cur.execute("INSERT into tagToPulsar (tagToPulsar_id,pulsar_id,tag_id) VALUES (NULL,?,?)",(pulsar_id,tag_id))
    if(commit==True):
        con.commit()
        tagToPulsarID = cur.lastrowid
        return tagToPulsarID
    else:
        return None
    return None

def addTagToCitation(con,citation_id,tag_id=None,tagLabel=None,tagString=None,commit=True):
    if(tag_id is None and tagLabel is None and tagString is None):
        return None
    if not tag_id:
        tag_id = addTag(con,tagLabel,tagString)
    if tag_id is None:
        return None
    cur = con.cursor()
    tagToCitationIDtemp = cur.execute("SELECT DISTINCT tagToCitation_id FROM tagToCitation WHERE citation_id LIKE ? and tag_id LIKE ?",(citation_id,tag_id))
    tagToCitationIDinDB = tagToCitationIDtemp.fetchone()
    if tagToCitationIDinDB:
        tagToCitationID = tagToCitationIDinDB[0]
        return tagToCitationID
    cur.execute("INSERT into tagToCitation (tagToCitation_id,citation_id,tag_id) VALUES (NULL,?,?)",(citation_id,tag_id))
    if(commit==True):
        con.commit()
        tagToCitationID = cur.lastrowid
        return tagToCitationID
    else:
        return None
    return None

def addTagToLinkedSet(con,linkedSet_id,tag_id=None,tagLabel=None,tagString=None,commit=True):
    if(tag_id is None and tagLabel is None and tagString is None):
        return None
    if not tag_id:
        tag_id = addTag(con,tagLabel,tagString)
    if tag_id is None:
        return None
    cur = con.cursor()
    tagToLinkedSetIDtemp = cur.execute("SELECT DISTINCT tagToLinkedSet_id FROM tagToLinkedSet WHERE linkedSet_id LIKE ? and tag_id LIKE ?",(linkedSet_id,tag_id))
    tagToLinkedSetIDinDB = tagToLinkedSetIDtemp.fetchone()
    if tagToLinkedSetIDinDB:
        tagToLinkedSetID = tagToLinkedSetIDinDB[0]
        return tagToLinkedSetID
    cur.execute("INSERT into tagToLinkedSet (tagToLinkedSet_id,linkedSet_id,tag_id) VALUES (NULL,?,?)",(linkedSet_id,tag_id))
    if(commit==True):
        con.commit()
        tagToLinkedSetID = cur.lastrowid
        return tagToLinkedSetID
    else:
        return None
    return None

def addTagToParameter(con,parameter_id,tag_id=None,tagLabel=None,tagString=None,commit=True):
    if(tag_id is None and tagLabel is None and tagString is None):
        return None
    if not tag_id:
        tag_id = addTag(con,tagLabel,tagString)
    if tag_id is None:
        return None
    cur = con.cursor()
    tagToParameterIDtemp = cur.execute("SELECT DISTINCT tagToParameter_id FROM tagToParameter WHERE parameter_id LIKE ? and tag_id LIKE ?",(parameter_id,tag_id))
    tagToParameterIDinDB = tagToParameterIDtemp.fetchone()
    if tagToParameterIDinDB:
        tagToParameterID = tagToParameterIDinDB[0]
        return tagToParameterID
    cur.execute("INSERT into tagToParameter (tagToParameter_id,parameter_id,tag_id) VALUES (NULL,?,?)",(parameter_id,tag_id))
    if(commit==True):
        con.commit()
        tagToParameterID = cur.lastrowid
        return tagToParameterID
    else:
        return None
    return None


def parseGeneralParameters(label,value,uncertainty,observingSystemID=None,widthFraction=None):
    
    con = connect_db()
        
    parList = pd.read_csv("lookupParameters",sep=';')
    
    labeltemp = parList[parList['oldlabel'].str.strip()==label]['newlabel'].to_numpy()
    #print(label)
    if(len(labeltemp)==0):
        havelabel = 0
    else:
        havelabel = 1
        timeDerivative = int(parList[parList['oldlabel'].str.strip()==label]['timeDerivative'].to_numpy()[0])
        companionNumber = int(parList[parList['oldlabel'].str.strip()==label]['companionNumber'].to_numpy()[0])
        newlabel = parList[parList['oldlabel'].str.strip()==label]['newlabel'].to_numpy()[0]
        derived = parList[parList['oldlabel'].str.strip()==label]['derived'].to_numpy()[0]
        label=newlabel
        # The parameters that are ALWAYS derived. e.g. dmdist,galactic coords, age,bsurf etc.. get ignored
        #if(derived == 0):
        #    print(f"HERE0 WITH {label}")
        if(derived == 1):
            havelabel = 0
        #    print(f'HERE1 WITH {label}') 
    if value is not None and isinstance(value,str):
        value = value.strip()
    if uncertainty is not None and isinstance(uncertainty,str):
        uncertainty = uncertainty.strip()

    if(value == '-' or value == '' or value is None):
        return None,None,None,None,None,None
    
    if(havelabel==1):
        parameterTypeID = getParameterTypeID(con,label)
        return label,value,uncertainty,parameterTypeID,timeDerivative,companionNumber
    else:
        return label,value,uncertainty,None,None,None
    
    con.close()


def errScale(valstr, errstr):

#Based on routine from Paul Harrison

    temp = 1
    i = 0

    #print(f"In here with {valstr} {errstr}")

    if errstr[0] == "-" or "." in errstr:
        #error is nonsense.
        return 1.0
    
    #strcpy(ovalstr, valstr)
    ovalstr = valstr[:]
    
    #check for decimal places
    if '.' not in ovalstr and not ("E" in ovalstr or "e" in ovalstr):
        valstr = None
        #print(f"Valstr = {valstr}")
    elif '.' not in ovalstr:
        valstr = ovalstr
    else:
        valstr = ovalstr[ovalstr.index('.'):]
        
    #print("HERE WITH ",valstr)
    if valstr != None:
        
        #print(f"Now valstr = {valstr}")

        i=1
 
        while i < len(valstr) and valstr[i].isdigit():
            i += 1
            
        #print(f"Found i = {i}")
        
        temp = 1
        #if valstr[i] in ['E', 'e']:  

        if valstr[min(i, len(valstr)-1)] in ['E', 'e']:
            temp = float(valstr[i+1:])
            #print(f"Getting {valstr[i:]} {temp}")
            #extract original exponent
            temp = math.pow(10.0, temp)
        else:
            temp=1

        #print(f"A temp = {temp:g}")

        if "." in valstr:
            #subtract i - 1 orders of magnitude
            temp *= pow(10.0, -(i-1))
        #print(f"B temp = {temp:g}")
        
        if ( temp > 1 ) and (len(errstr) > 1) and (errstr[0] != "-"):
            temp *= pow(10.0, (len(errstr) - 1))
        #print(f"C temp = {temp:g}")
        return temp
    else:
        return 1

def getValueErr(valStr,errStr,scale):

    try:
        float(valStr)
    except ValueError:
        try:
            float(errStr)
            return valStr,errStr
        except ValueError:
            return valStr,None

    logScale = int(np.log10(scale))
    if (np.log10(scale) != logScale):
        return None,None
    
    if errStr != "-" and errStr != 'None' and errStr is not None:
        scaleError = errScale(valStr,errStr)
        finalError = (float(errStr))*scaleError*scale

    if 'E' in valStr:
        expStr = valStr[valStr.index('E'):]
        valStr = valStr.split('E')[0]
        exponent = int(expStr[1:])
        logScale = logScale + exponent
        
    if 'e' in valStr:
        expStr = valStr[valStr.index('e'):]
        valStr = valStr.split('e')[0]
        exponent = int(expStr[1:])
        logScale = logScale + exponent
        
    if(str(logScale) == "0"):
        finalValueStr = valStr
    else:
        finalValueStr = valStr+"e"+str(logScale)

    if errStr != "-" and errStr != 'None' and errStr is not None:
        return finalValueStr,'{:.2g}'.format(finalError)
        #print(f"{finalValueStr} {finalError:.2g}")
    else:
        return finalValueStr,None


# Helpful calculations

def jyear_to_mjd(jyear):
    # Date in format 2010.96 for e.g.
    mjd = ((jyear-2000)*365.25)+(2451525-2400000.5)
    return mjd

def year_to_mjd(year):
    ## Doesn't work yet. Idk why
    # Date in format 2020:08:21
    date = str(year).split(':')
    if(len(date)==1):
        dat = date[0]
        y = dat[0:4]
        m = dat[4:6]
        d = dat[6:8]
    elif(len(date)==3):
        y = date[0]
        m = date[1]
        d = date[2]
    
    
    fdate = datetime(int(y),int(m),int(d))
    doy = fdate.timetuple().tm_yday

    jyr = float(y)+float(doy/365)
    print(jyr)
    mjd = jyear_to_mjd(jyr)
    print(mjd)
