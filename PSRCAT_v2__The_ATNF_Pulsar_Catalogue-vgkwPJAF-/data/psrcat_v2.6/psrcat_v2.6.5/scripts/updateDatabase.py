import sys
import re
import sqlite3
import csv
import derive
import utils
import pandas as pd
import numpy as np

# Copyright: CSIRO 2024
# Author: Agastya Kapur: Agastya.Kapur@csiro.au, George Hobbs: George.Hobbs@csiro.au, Lawrence Toomey

def splitParamValue(line,delim=','): 
    splitted = line.split(delim)
    value = None
    if(len(splitted) == 1):
        print("PARAM input but has no value")
        label=(line.split(delim)[0]).strip()
    elif(len(splitted) == 2 or len(splitted) == 3):
        label=(line.split(delim)[0]).strip()
        value=(line.split(delim)[1]).strip()
    else:
        print("Something went wrong")
    return value

def splitParFile(line,delim=' '):
    splitted = line.split(delim)
    value = None
    if(len(splitted) == 1):
        print("PARAM input but has no value")
        label=(line.split(delim)[0]).strip()
    elif(len(splitted) == 2 or len(splitted) == 4):
        label=(line.split(delim)[0]).strip()
        value=(line.split(delim)[1]).strip()
    else:
        print("Something went wrong")
    return value
    
def handleRAJ(value,error):
    rajval = value.split(':')
    if error is None:
        return None
    if(len(rajval) == 3):  #Error in seconds
        hh = float(rajval[0])
        mm = float(rajval[1])
        ss = float(rajval[2])
        if(hh>=24 or hh<0 or mm>=60 or mm<0 or ss>=60 or ss<0):
            print("WARNING: RAJ STRING NOT FORMATTED PROPERLY")
        scaleby = rajval[2]
        tempval,tempunc = utils.getValueErr(str(scaleby),str(error),1)
        tempunc = float(tempunc)*15/3600
        tempval,uncertainty = utils.getValueErr(str(tempval),str(tempunc),1)
        return uncertainty
    elif(len(rajval) == 2):  #Error in minutes
        hh = float(rajval[0])
        mm = float(rajval[1])
        ss = 0
        if(hh>=24 or hh<0 or mm>=60 or mm<0 or ss>=60 or ss<0):
            print("WARNING: RAJ STRING NOT FORMATTED PROPERLY")
        scaleby = rajval[1]
        tempval,tempunc = utils.getValueErr(str(scaleby),str(error),1)
        tempunc = float(tempunc)*15/60
        tempval,uncertainty = utils.getValueErr(str(tempval),str(tempunc),1)
        return uncertainty
    elif(len(rajval) == 1):  #Error in hours
        hh = float(rajval[0])
        mm = 0
        ss = 0
        if(hh>=24 or hh<0 or mm>=60 or mm<0 or ss>=60 or ss<0):
            print("WARNING: RAJ STRING NOT FORMATTED PROPERLY")
        scaleby = rajval[0]
        tempval,tempunc = utils.getValueErr(str(scaleby),str(error),1)
        tempunc = float(tempunc)*15
        tempval,uncertainty = utils.getValueErr(str(tempval),str(tempunc),1)
        return uncertainty
    else:
        print("WARNING: RAJ STRING NOT FORMATTED PROPERLY")


def handleDECJ(value,error):
    decjval = value.split(':')
    if error is None:
        return None
    if(len(decjval) == 3):  #Error in seconds
        dd = float(decjval[0])
        mm = float(decjval[1])
        ss = float(decjval[2])
        if(dd>90 or dd<-90 or mm>=60 or mm<0 or ss>=60 or ss<0):
            print("WARNING: DECJ STRING NOT FORMATTED PROPERLY")
        scaleby = decjval[2]
        tempval,tempunc = utils.getValueErr(str(scaleby),str(error),1)
        tempunc = float(tempunc)/3600
        tempval,uncertainty = utils.getValueErr(str(tempval),str(tempunc),1)
        return uncertainty
    elif(len(decjval) == 2):  #Error in minutes
        dd = float(decjval[0])
        mm = float(decjval[1])
        ss = 0
        if(dd>90 or dd<-90 or mm>=60 or mm<0 or ss>=60 or ss<0):
            print("WARNING: DECJ STRING NOT FORMATTED PROPERLY")
        scaleby = decjval[1]
        tempval,tempunc = utils.getValueErr(str(scaleby),str(error),1)
        tempunc = float(tempunc)/60
        tempval,uncertainty = utils.getValueErr(str(tempval),str(tempunc),1)
        return uncertainty
    elif(len(decjval) == 1):  #Error in hours
        dd = float(decjval[0])
        mm = 0
        ss = 0
        if(dd>90 or dd<-90 or mm>=60 or mm<0 or ss>=60 or ss<0):
            print("WARNING: DECJ STRING NOT FORMATTED PROPERLY")
        scaleby = decjval[0]
        tempval,tempunc = utils.getValueErr(str(scaleby),str(error),1)
        tempunc = float(tempunc)
        tempval,uncertainty = utils.getValueErr(str(tempval),str(tempunc),1)
        return uncertainty
    else:
        print("WARNING: DECJ DECJ STRING NOT FORMATTED PROPERLY")



def addRows(con,infile,citation_id,linkedSetDesc=None,delim=',',commit=True):
     
    filein = open(infile,"r")
    delim = delim
    commit = commit
    psrj=None
    pepoch=None
    p0=None
    f0=None
    raj=None
    decj=None
    elong=None
    elat=None
    dm=None
    posepoch=None
    dmepoch=None
    survey_label=None
    units= 'unknown'
    ephem= 'unknown'
    clock= 'unknown'
    #TO IMPLEMENT
    systemlabel=''
    bandwidth=None
    telescope=None
    
    for line in filein:
        #PSRJ"
        if(re.search("PSRJ",line)):
            splitted = line.split(delim)
            psrj = splitted[1].strip()
    
        if(re.search("SURVEY",line)):
            splitted=line.split(delim)
            survey_label = splitted[1].strip()
        #PEPOCH check
        elif(re.search("PEPOCH",line)):
            pepoch = splitParamValue(line,delim)
        #DMEPOCH check
        elif(re.search("DMEPOCH",line)):
            dmepoch = splitParamValue(line,delim)
        #POSEPOCH check
        elif(re.search("POSEPOCH",line)):
            posepoch = splitParamValue(line,delim)
        #DM check
        elif(re.search("DM",line)):
            dm = splitParamValue(line,delim)
        #P0 check
        elif(re.search("P0",line)):
            p0 = splitParamValue(line,delim)
        #F0 check
        elif(re.search("F0",line)):
            f0 = splitParamValue(line,delim)    
        #RAJ check
        elif(re.search("RAJ",line)):
            raj = splitParamValue(line,delim)
        #DECJ check
        elif(re.search("DECJ",line)):
            decj = splitParamValue(line,delim)
        #ELONG check
        elif(re.search("ELONG",line)):
            elong = splitParamValue(line,delim)
        #ELAT check
        elif(re.search("ELAT",line)):
            elat = splitParamValue(line,delim)
        #UNITS check
        elif(re.search("UNITS",line)):
            units = splitParamValue(line,delim)
        #EPHEM check
        elif(re.search("EPHEM",line)):
            ephem = splitParamValue(line,delim)
        #CLOCK check
        elif(re.search("CLOCK",line)):
            clock = splitParamValue(line,delim)
    
    pulsar_id = utils.getPulsarIDfromName(con,psrj)
    
    if not survey_label and not pulsar_id:
        survey_label= 'misc'
     
    if(psrj is None):
        print("ERROR: No pulsar ?!?!?")
        exit()
    if((raj is None and elong is None) or (decj is None and elat is None) or pepoch is None or (p0 is None and f0 is None) or dm is None):
        linkedSet_id = None
    else:
        linkedSet_id = utils.addLinkedSet(con,citation_id,linkedSetDesc,commit=commit)

    # Add pulsar

    
    
    survey_id= utils.getSurveyIDfromLabel(con,survey_label)
    if(pulsar_id is None and psrj[0] == 'J'):
        pulsar_id = utils.addPulsarJname(con,psrj,survey_id,citation_id,commit=commit)
           
    if not pulsar_id:
        print("ERROR: NO PULSAR_ID ADDED.")
        exit()

    if(units == 'unknown' and clock == 'unknown' and ephem == 'unknown'):
        fitParameters_id = None
    else:
        fitParameters_id = utils.addFitParameters(con,units,ephem,clock,citation_id,commit=commit)
    if survey_id:
        surveyToPulsar_id = utils.addSurveyToPulsar(con,survey_id,pulsar_id,commit=commit)
    
    filein = open(infile,"r")

    parameter_id = None
    for line in filein:
        #print(line)
        referenceTime=pepoch 
        splitted = line.split(delim)
        label=splitted[0]
        if parameter_id is not None:
            if(label == "TAG_PARAMETER"):
                splitted = csv.reader([line],skipinitialspace=True,delimiter=delim).__next__()
                tagLabel = splitted[1]
                tagString = splitted[2]
                tagToParameter_id = utils.addTagToParameter(con,parameter_id,tagLabel=tagLabel,tagString=tagString,commit=commit)
                print("Added TagToParameter: TagToPar_id:%s par_id:%s tagL:%s tagS:%s" % (tagToParameter_id,parameter_id,tagLabel,tagString))
                continue
        if(label=="NOTES"):
            splitted = csv.reader([line],skipinitialspace=True,delimiter=delim).__next__()
            notes = splitted[1]
            print("NOTES: %s" % (notes))
            continue
        elif(label=="TAG_PAPER"):
            splitted = csv.reader([line],skipinitialspace=True,delimiter=delim).__next__()
            tagLabel = splitted[1]
            tagString = splitted[2]
            if citation_id is None:
                print("WARNING: Added tag to citation even though no citation is there")
            tagToCitation_id = utils.addTagToCitation(con,citation_id,tagLabel=tagLabel,tagString=tagString,commit=commit)
            print("Added TagToCitation: TagToC_id:%s cit_id:%s tagL:%s tagS:%s" % (tagToCitation_id,citation_id,tagLabel,tagString))
            continue
        elif(label=="TAG_PULSAR"):
            splitted = csv.reader([line],skipinitialspace=True,delimiter=delim).__next__()
            tagLabel = splitted[1]
            tagString = splitted[2]
            tagToPulsar_id = utils.addTagToPulsar(con,pulsar_id=pulsar_id,tagLabel=tagLabel,tagString=tagString,commit=commit)
            print("Added TagToPulsar: TagToPul_id:%s pul_id:%s tagL:%s tagS:%s" % (tagToPulsar_id,pulsar_id,tagLabel,tagString))
            continue
        elif(label=="TAG_LINKEDSET"):
            splitted = csv.reader([line],skipinitialspace=True,delimiter=delim).__next__()
            tagLabel = splitted[1]
            tagString = splitted[2]
            if linkedSet_id is None:
                print("WARNING: Added tag to linked set even though no linked set created")
                continue
            tagToLinkedSet_id = utils.addTagToLinkedSet(con,linkedSet_id,tagLabel=tagLabel,tagString=tagString,commit=commit)
            print("Added TagToLinkedSet: TagToLink:%s link_id:%s tagL:%s tagS:%s" % (tagToLinkedSet_id,linkedSet_id,tagLabel,tagString))            
            continue
        elif(label=="ASSOC"):
            if(3<=len(splitted)<=4):
                assocLabel = splitted[1]
                assocName = splitted[2].strip()
                if(assocName=='SMC' or assocName=='LMC'):
                    citation_id=None
                confidence = 1
                if(len(splitted)==4):
                    confidence = splitted[3].strip()
                    if(confidence == 2):
                        confidence = 2
                    else:
                        confidence = 1
                confidence = int(confidence)
                association_id = utils.addAssociation(con,pulsar_id,assocName,citation_id,confidence,label=assocLabel,commit=commit)
                print("Added assoc: assoc_id:%s cit_id:%s assocN:%s assocL:%s" % (association_id,citation_id,assocName,assocLabel))
                continue
            else:
                continue
        elif(label=="TYPE"):
            if(2<=len(splitted)<=3):
                sourceType = splitted[1]
                confidence = 1
                if(len(splitted)==3):
                    confidence = splitted[2].strip()
                    if(confidence == 2):
                        confidence = 2
                    else:
                        confidence = 1
                confidence = int(confidence)
                sourceType_id = utils.addSourceType(con,pulsar_id,citation_id,confidence,label=sourceType,commit=commit)
                print("Added type: type_id:%s cit_id:%s type:%s" % (sourceType_id,citation_id,sourceType))
                continue
            else:
                continue
        elif(label=="BINCOMP"):
            if(2<=len(splitted)<=3):
                binaryType = splitted[1]
                confidence = 1
                if(len(splitted)==3):
                    confidence = splitted[2].strip()
                    if(confidence == 2):
                        confidence = 2
                    else:
                        confidence = 1
                confidence = int(confidence)
                binaryType_id = utils.addBinaryType(con,pulsar_id,citation_id,confidence,label=binaryType,commit=commit)
                print("Added binary: bin_id:%s cit_id:%s bin:%s" % (binaryType_id,citation_id,binaryType))
                continue
            else:
                continue
        elif(label=="FLUX" or label == "W10" or label == "W50" or label == "FRACL" or label == "FRACV" or label == "FRACAV"):
            if(8<=len(splitted)<=9):
                fitParameters_id = None
                uncertainty = None
                label=splitted[0]
                value=splitted[1]
                uncertainty=splitted[2]
                if(uncertainty == ''):
                    if('(' in str(value)):
                        oldval = value 
                        value = oldval.split('(')[0] #Split for bracket error
                        place_err = oldval.split('(')[1]
                        split_err = place_err.split(')')
                        if(split_err[1] == ''):
                            place_err = split_err[0]
                            value,uncertainty = utils.getValueErr(str(value),str(place_err),float(1))
                        else:
                            place_err = split_err[0]
                            scaleVal = '1'+str(split_err[1])
                            value,uncertainty = utils.getValueErr(str(value),str(place_err),float(scaleVal))
                referenceTime=splitted[3]
                centralFrequency=float(splitted[4])
                bandwidth=float(splitted[5])
                systemLabel=splitted[6]
                telescope=splitted[7].strip()
                approximate = 0
                if(len(splitted)==9):
                    approximate = splitted[8].strip()
                    if(approximate == 1):
                        approximate = 1
                    else:
                        approximate = 0
                if(label == "W10"):
                    label = "WIDTH"
                    widthFraction = "10.0"
                    widthDescription = "Percentage of peak height"
                elif(label == "W50"):
                    label = "WIDTH"
                    widthFraction = "50.0"
                    widthDescription = "Percentage of peak height"
                observingSystem_id = utils.addObservingSystem(con,systemLabel,centralFrequency,bandwidth,telescope,approximate=approximate,commit=commit)
            else:
                print("Not enough or too many parameters entered for a flux value")
                print("Input as ----------------")
                print("FLUX,value,error,epoch,centreFreq,bandwidth,systemLabel,telescope,approximate")
                print("Approximate is optional. If approximate = 1, this is because the receiver frequency is not given precisely")
                continue

        
        elif(label=="DM"):
            if dmepoch:
                referenceTime=dmepoch
        elif(label=="RAJ" or label=="DECJ" or label=="PMRA" or label=="PMDEC" or label=="ELAT" or label=="ELONG"or label=="PMELONG" or label=="PMELAT"):
            if posepoch:
                referenceTime=posepoch
                
        if(len(splitted)==2):
            observingSystem_id=None
            label=splitted[0].strip()
            value=splitted[1].strip()
            if('(' in str(value)):
                oldval = value 
                value = oldval.split('(')[0] #Split for bracket error
                place_err = oldval.split('(')[1]

                # Checking if last digit uncertainty is an integer
                err_int_check = place_err.split(')')[0]
                try:
                    int(err_int_check)
                except ValueError:
                    if err_int_check is None:
                        pass
                    else:
                        if(label in ["BINARY","CLOCK","EPHEM","UNITS"]):
                            pass
                        else:
                            print(f"ERROR: label:{label} uncertainty:{err_int_check} is not an int. EXITING")
                            exit()
 
                try:
                    float(value)
                except ValueError:
                    if(label in ["RAJ","DECJ","BINARY","CLOCK","EPHEM","UNITS"]):
                        pass
                    else:
                        print(f"WARNING: label:{label} value:{value} is not a float")
                print(label)
                if(label == "RAJ"):
                    place_err = place_err.strip(')')
                    uncertainty = handleRAJ(value,place_err)
                elif(label == "DECJ"):
                    place_err = place_err.strip(')')
                    uncertainty = handleDECJ(value,place_err)
                else:
                    split_err = place_err.split(')')
                    if(split_err[1] == ''):
                        place_err = split_err[0]
                        value,uncertainty = utils.getValueErr(str(value),str(place_err),float(1))
                    else:
                        place_err = split_err[0]
                        scaleVal = '1'+str(split_err[1])
                        value,uncertainty = utils.getValueErr(str(value),str(place_err),float(scaleVal))
            else:  
                uncertainty=None
        elif(len(splitted)==3):
            observingSystem_id=None
            label=splitted[0].strip()
            value=splitted[1].strip()
            uncertainty=splitted[2].strip()
            if('(' in str(value)):
                oldval = value
                value = oldval.split('(')[0]
            try:
                float(value)
            except ValueError:
                if(label in ["RAJ","DECJ","BINARY","CLOCK","EPHEM","UNITS","GROUP"]):
                    pass
                else:
                    print(f"WARNING: label:{label} value:{value} is not a float")
            if(label == "RAJ"):
                uncertainty = handleRAJ(value,uncertainty)
            elif(label == "DECJ"):
                uncertainty = handleDECJ(value,uncertainty)
            else:
                if uncertainty is None:
                    value,uncertainty = utils.getValueErr(str(value),str('-'),float(1))
                else:
                    value,uncertainty = utils.getValueErr(str(value),str(uncertainty),float(1))
        psrcat1label = label
        label,value,uncertainty,parameterType_id,timeDerivative,companionNumber = utils.parseGeneralParameters(label,value,uncertainty)
        if(label is None or value is None or parameterType_id is None):
            continue 
        else:
            parameter_id = utils.addParameter(con,pulsar_id,citation_id,linkedSet_id,fitParameters_id,observingSystem_id,parameterType_id,timeDerivative,companionNumber,value,uncertainty,referenceTime,commit=commit)
            if(label == "WIDTH"):
                ancillary_id = utils.addAncillary(con,parameter_id,widthFraction,widthDescription,commit=commit)
            if(label == "FLUX" or label == "FRACL" or label == "FRACV" or label == "FRACAV"):
                print("Added parameter: p_id:%s cit_id:%s label:%s val:%s err:%s obs_id:%s mjd:%s\n" % (parameter_id,citation_id,psrcat1label,value,uncertainty,observingSystem_id,referenceTime))
            elif(label == "WIDTH"):
                print("Added parameter: p_id:%s cit_id:%s label:%s val:%s err:%s obs_id:%s mjd:%s anc_id:%s\n" % (parameter_id,citation_id,psrcat1label,value,uncertainty,observingSystem_id,referenceTime,ancillary_id))
            else:
                print("Added parameter: p_id:%s cit_id:%s label:%s value:%s err:%s mjd:%s\n" % (parameter_id,citation_id,psrcat1label,value,uncertainty,referenceTime))
def addParFile(con,infile,citation_id,linkedSetDesc,delim=' ',commit=True):
        
    filein = open(infile,"r")
    commit = commit
    
    psrj=None
    pepoch=None
    p0=None
    f0=None    
    raj=None
    decj=None    
    elong=None    
    elat=None    
    dm=None    
    posepoch=None    
    dmepoch=None    
    survey_label=None
    units=None
    ephem=None
    clock=None
    
    for line in filein:

    
        #PSRJ"
        if(re.search("PSRJ",line) or re.search("PSRB",line)):
            splitted = line.split(delim)
            psrj = splitted[1].strip()
    
        if(re.search("SURVEY",line)):
            splitted=line.split(delim)
            survey_label = splitted[1].strip()
        #PEPOCH check
        elif(re.search("PEPOCH",line)):
            pepoch = splitParFile(line)
        #DMEPOCH check
        elif(re.search("DMEPOCH",line)):
            dmepoch = splitParFile(line)
        #POSEPOCH check
        elif(re.search("POSEPOCH",line)):
            posepoch = splitParFile(line)
        #DM check
        elif(re.search("DM",line)):
            dm = splitParFile (line)
        #P0 check
        elif(re.search("P0",line)):
            p0 = splitParFile(line)
        #F0 check
        elif(re.search("F0",line)):
            f0 = splitParFile(line)    
        #RAJ check
        elif(re.search("RAJ",line)):
            raj = splitParFile(line)
        #DECJ check
        elif(re.search("DECJ",line)):
            decj = splitParFile(line)
        #ELONG check
        elif(re.search("ELONG",line)):
            elong = splitParFile(line)
        #ELAT check
        elif(re.search("ELAT",line)):
            elat = splitParFile(line)
        #UNITS check
        elif(re.search("UNITS",line)):
            units = splitParFile(line)
        #EPHEM check
        elif(re.search("EPHEM",line)):
            ephem = splitParFile(line)
        #CLOCK check
        elif(re.search("CLOCK",line) or re.search("CLK",line)):
            clock = splitParFile(line)

    if(survey_label is None):
        survey_label='misc'
    if(units is None):
        units='unknown'
    if(ephem is None):
        ephem='unknown'
    if(clock is None):
        clock='unknown'
    #print(psrj)
    #print(raj)
    #print(decj)
    #print(elong)
    #print(elat)
    #print(pepoch)
    #print(p0)
    #print(f0)
    #print(dm)
    
    pulsar_id = utils.getPulsarIDfromName(con,psrj)

    if not survey_label and not pulsar_id:
        survey_label= 'misc'

    if(psrj is None):
        print("ERROR: No pulsar ?!?!?")
        exit()
    if((raj is None and elong is None) or (decj is None and elat is None) or pepoch is None or (p0 is None and f0 is None) or dm is None):
        linkedSet_id = None
    else:
        linkedSet_id = utils.addLinkedSet(con,citation_id,linkedSetDesc,commit=commit)

    # Add pulsar



    survey_id= utils.getSurveyIDfromLabel(con,survey_label)
    if(pulsar_id is None and psrj[0] == 'J'):
        pulsar_id = utils.addPulsarJname(con,psrj,survey_id,citation_id,commit=commit)

    if not pulsar_id:
        print("ERROR: NO PULSAR_ID ADDED.")
        exit()

    if(units == 'unknown' and clock == 'unknown' and ephem == 'unknown'):
        fitParameters_id = None
    else:
        fitParameters_id = utils.addFitParameters(con,units,ephem,clock,citation_id,commit=commit)
    if survey_id:
        surveyToPulsar_id = utils.addSurveyToPulsar(con,survey_id,pulsar_id,commit=commit)


    parList = pd.read_csv("lookupParameters",sep=';')
    filein = open(infile,"r")
    
    for line in filein:
        referenceTime=pepoch 
        splitted = line.split(delim)
        label=splitted[0]
        timingFlagtemp = parList[parList['oldlabel'].str.match(label)]['timingFlag'].to_numpy()

        if(len(timingFlagtemp)==0):
            label = 0
        else:
            timingFlag = timingFlagtemp[0]
            
        
        if(label==0):
            continue

        uncertainty = None
        if(timingFlag==1):
            #split string accordinly.
            label = splitted[0].strip()
            value = splitted[1].strip()
            uncertainty = splitted[3].strip()
            if (uncertainty == ""):
                uncertainty = splitted[2].strip()
        elif(timingFlag==0):
            label = splitted[0].strip()
            value = splitted[1].strip()           
        
        if(label=="DM"):
            if dmepoch:
                referenceTime=dmepoch
        if(label=="RAJ" or label=="DECJ" or label=="PMRA" or label=="PMDEC" or label=="ELAT" or label=="ELONG" or label=="PMELONG" or label=="PMELAT"):
            if posepoch:
                referenceTime=posepoch

        observingSystem_id=None
        psrcat1label = label
        if(label == "RAJ"):
            if uncertainty is not None and uncertainty != "":
                uncertainty = handleRAJ(value,uncertainty)
        elif(label == "DECJ"):
            if uncertainty is not None and uncertainty != "":
                uncertainty = handleDECJ(value,uncertainty)
        else:
            if uncertainty is None or uncertainty == "":
                value,uncertainty = utils.getValueErr(str(value),str('-'),float(1))
            else:
                #print('cheeya')
                value,uncertainty = utils.getValueErr(str(value),str(uncertainty),float(1))

        label,value,uncertainty,parameterType_id,timeDerivative,companionNumber = utils.parseGeneralParameters(label,value,uncertainty)
        if(value is None or label is None or parameterType_id is None):
            continue
        else:
            parameter_id = utils.addParameter(con,pulsar_id,citation_id,linkedSet_id,fitParameters_id,observingSystem_id,parameterType_id,timeDerivative,companionNumber,value,uncertainty,referenceTime,commit=commit)
            print("Added parameter: p_id:%s cit_id:%s label:%s value:%s err:%s mjd:%s\n" % (parameter_id,citation_id,psrcat1label,value,uncertainty,referenceTime))
            
def addTabular(con,infile,citation_id,delim=',',commit=True):
    # Number of lines to skip. Start from 1 because of "GROUP" 
    numextra = 1
    commit = commit
    #HEADER INPUTS
    _survey_label = None
    _notes = None
    _telescope = None
    _systemLabel = ''
    _sourceType = None
    _units = 'unknown'
    _clock = 'unknown'
    _ephemeris = 'unknown'
    _discovery = None
    _linkedSet = None
    _assoc = None
    ###
    survey_label = None
    notes = None
    telescope = None
    systemLabel = ''
    units = 'unknown'
    clock = 'unknown'
    ephemeris = 'unknown'
    uncertainty = None
    sourceType = None
    discovery = None
    linkedSet = None
    assoc = None
    
    filein = open(infile,"r")
    # Decide how many lines to skip
    for line in filein:
        if(re.search("_SURVEY",line)):
            splitted = line.split(delim)
            _survey_label = splitted[1].strip()
            numextra+=1
        elif(re.search("_TAG_PAPER",line)):
            splitted = csv.reader([line],skipinitialspace=True,delimiter=delim).__next__()
            tagLabel = splitted[1]
            tagString = splitted[2]
            if citation_id is None:
                print("WARNING: Added tag to citation even though no citation is there")
            tagToCitation_id = utils.addTagToCitation(con,citation_id,tagLabel=tagLabel,tagString=tagString,commit=commit)
            print("Added TagToCitation: TagToC_id:%s cit_id:%s tagL:%s tagS:%s" % (tagToCitation_id,citation_id,tagLabel,tagString))
            numextra+=1
        elif(re.search("_NOTES",line)):
            splitted = line.split(delim)
            _notes = splitted[1].strip()
            print("NOTES: %s" % (_notes))
            numextra+=1    
        elif(re.search("_TELESCOPE",line)):
            splitted = line.split(delim)
            _telescope = splitted[1].strip()
            numextra +=1
        elif(re.search("_SYSTEMLABEL",line)):
            splitted = line.split(delim)
            _systemLabel = splitted[1].strip()
            numextra+=1
        elif(re.search("_UNITS",line)):
            splitted = line.split(delim)
            _units = splitted[1].strip()
            numextra +=1
        elif(re.search("_CLOCK",line)):
            splitted = line.split(delim)
            _clock = splitted[1].strip()
            numextra +=1
        elif(re.search("_EPHEM",line)):
            splitted = line.split(delim)
            _ephemeris = splitted[1].strip()
            numextra +=1
        elif(re.search("_TYPE",line)):
            splitted = csv.reader([line],skipinitialspace=True,delimiter=delim).__next__()
            _sourceType = splitted[1].strip()
            numextra +=1
        elif(re.search("_DISCOVERY",line)):
            splitted = line.split(delim)
            _discovery = splitted[1].strip()
            numextra +=1
        elif(re.search("_LINKEDSET",line)):
            splitted = line.split(delim)
            _linkedSet = splitted[1].strip()
            numextra +=1
        elif(re.search("_ASSOC",line)):
            splitted = csv.reader([line],skipinitialspace=True,delimiter=delim).__next__()
            _assoc = splitted[1].strip()
            #print(_assoc)
            numextra +=1
        elif(re.search("PSRJ",line)):
            break

        else:
            if(re.search("GROUP",line)):
                pass
            else:
                print(f"line: {line} has an unknown parameter")
#            numextra += 1
    
    pd_infile = pd.read_csv(infile,sep=delim,skiprows=numextra)
    print(pd_infile.to_string())
    headers = list(pd_infile)

    for index,row in pd_infile.iterrows():

        psrj = row['PSRJ']

        if(pd.isna(psrj)):
            print("NO PULSAR NAME IN THIS ROW")
            continue
        
        pulsar_id = utils.getPulsarIDfromName(con,psrj)
        if pulsar_id:
            if(discovery=='1'):
                print(f"\n WARNING: GEORGE SAID TO RING ALARM BELLS. {psrj} EXISTS IN THE DB ALREADY\n")
                continue
            else:
                if(discovery!='0'):
                    print(f"WARNING: PULSAR {psrj} EXISTS IN THE THE DATABASE. If this is not a discovery, set DISCOVERY,0 or just ignore this message.")

        
        if("SURVEY" in headers):
            if(pd.isna(row['SURVEY'])):
                if _survey_label:
                    survey_label = _survey_label
            else:
                survey_label = row['SURVEY']
        else:
            survey_label = _survey_label

        if not survey_label and not pulsar_id:
            survey_label = 'misc'

        if("UNITS" in headers):
            if(pd.isna(row['UNITS'])):
                if _units:
                    units = '_units'
            else:
                units = row['UNITS']
        else:
            units = _units

        if("CLOCK" in headers):
            if(pd.isna(row['CLOCK'])):
                if _clock:
                    clock = _clock
            else:
                clock = row['CLOCK']
        else:
            clock = _clock
        if("EPHEM" in headers):
            if(pd.isna(row['EPHEM'])):
                if _ephemeris:
                    ephemeris = _ephemeris
            else:
                ephemeris = row['EPHEM']
        else:
            ephemeris = _ephemeris
            
        if("TELESCOPE" in headers):
            if(pd.isna(row['TELESCOPE'])):
                if _telescope:
                    telescope = _telescope
                else:
                    telescope = None
            else:
                telescope = row['TELESCOPE']
        else:
            telescope = _telescope
                
        if("SYSTEMLABEL" in headers):
            if(pd.isna(row['SYSTEMLABEL'])):
                if _systemLabel:
                    systemLabel = _systemLabel
                else:
                    systemLabel = ''
            else:
                systemLabel = row['SYSTEMLABEL']
        else:
            systemLabel = _systemLabel

        if("TYPE" in headers):
            if(pd.isna(row['TYPE'])):
                if _sourceType:
                    sourceType = _sourceType
                else:
                    sourceType = None
            else:
                sourceType = row['TYPE']
        else:
            sourceType = _sourceType

        if("LINKEDSET" in headers):
            if(pd.isna(row['LINKEDSET'])):
                if _linkedSet:
                    linkedSet = _linkedSet
                else:
                    linkedSet = None
            else:
                linkedSet = row['LINKEDSET']
        else:
            linkedSet = _linkedSet

        if("DISCOVERY" in headers):
            if(pd.isna(row['DISCOVERY'])):
                if _discovery:
                    discovery = _discovery
                else:
                    discovery = None
            else:
                discovery = row['DISCOVERY']
        else:
            discovery = _discovery

        if("ASSOC" in headers):
            if(pd.isna(row['ASSOC'])):
                if _assoc:
                    assoc = _assoc
                else:
                    assoc = None
            else:
                assoc = row['ASSOC']
        else:
            assoc = _assoc

            
        survey_id = utils.getSurveyIDfromLabel(con,survey_label)
        if(pulsar_id is None and psrj[0] == 'J'):
            pulsar_id = utils.addPulsarJname(con,psrj,survey_id,citation_id,commit=commit)

        if survey_id:
            surveyToPulsar_id = utils.addSurveyToPulsar(con,survey_id,pulsar_id,commit=commit)
        if(units == 'unknown' and ephemeris == 'unknown' and clock == 'unknown'):
           fitParameters_id = None
        else:
           fitParameters_id = utils.addFitParameters(con,units,ephemeris,clock,citation_id,commit=commit)

        if sourceType:
            splitted = csv.reader([sourceType],skipinitialspace=True,delimiter=delim).__next__()
            sourceType = splitted[0]
            confidence = 1
            if(len(splitted)==2):
                confidence = splitted[1].strip()
                if(confidence==2):
                    confidence = 2
                else:
                    confidence = 1
            confidence = int(confidence)
            sourceType_id = utils.addSourceType(con,pulsar_id,citation_id,confidence,label=sourceType,commit=commit)
            print("Added type: type_id:%s cit_id:%s type:%s" % (sourceType_id,citation_id,sourceType))

        if assoc:
            splitted = csv.reader([assoc],skipinitialspace=True,delimiter=delim).__next__()
            assocLabel = splitted[0]
            assocName = splitted[1]
            confidence=1
            if(len(splitted)==3):
                confidence = splitted[2].strip()
                if(confidence==2):
                    confidence = 2
                else:
                    confidence = 1
            confidence = int(confidence)
            association_id = utils.addAssociation(con,pulsar_id,assocName,citation_id,confidence,label=assocLabel,commit=commit)
            print("Added assoc: assoc_id:%s cit_id:%s assocN:%s assocL:%s" % (association_id,citation_id,assocName,assocLabel))    
           
        referenceTime = None
        linkedSet_id = None
        
        if("PEPOCH" in headers):
            if(pd.isna(row['PEPOCH'])):
                referenceTime = None
            else:
                referenceTime = row['PEPOCH']
        #LinkedSet determination
        timingList = []
        originalList = []
        timingParameters = ['PSRJ','RAJ','DECJ','ELONG','ELAT','P0','F0','DM']
        linkedSetDesc = "Timing Model"
        
        for label in headers:
            formallabel = label.split('(')[0]
            timingList.append(formallabel)
            originalList.append(label)

        if(linkedSet=='1'):
            if 'GLEP' in headers:
                linkedSetDesc = 'Glitch parameters'
            elif('MAGINC' or 'IMPANG' or 'PASWING' in headers):
                linkedSetDesc = 'Pulsar Geometry'
            linkedSet_id = utils.addLinkedSet(con,citation_id,linkedSetDesc,commit=commit)
        else:
            if('PSRJ' and 'RAJ' and 'DECJ' and 'P0' and 'DM' in timingList):
                for timparam in originalList:
                    if(pd.isna(row[timparam])):
                        linkedSet_id = None
                        continue
                    else:
                        linkedSet_id = utils.addLinkedSet(con,citation_id,linkedSetDesc,commit=commit)
            elif('PSRJ' and 'RAJ' and 'DECJ' and 'F0' and 'DM' in timingList):
                for timparam in originalList:
                    if(pd.isna(row[timparam])):
                        linkedSet_id = None
                        continue
                    else:
                        linkedSet_id = utils.addLinkedSet(con,citation_id,linkedSetDesc,commit=commit)
            elif('PSRJ' and 'ELONG' and 'ELONG' and 'P0' and 'DM' in timingList):
                for timparam in originalList:
                    if(pd.isna(row[timparam])):
                        linkedSet_id = None
                        continue
                    else:
                        linkedSet_id = utils.addLinkedSet(con,citation_id,linkedSetDesc,commit=commit)
            elif('PSRJ' and 'ELONG' and 'ELONG' and 'F0' and 'DM' in timingList):
                for timparam in originalList:
                    if(pd.isna(row[timparam])):
                        linkedSet_id = None
                        continue
                    else:
                        linkedSet_id = utils.addLinkedSet(con,citation_id,linkedSetDesc,commit=commit)
        
        for label in headers:


            origlabel = label
            splitted = label.split('(')

            if(origlabel[-4:] in ["_MJD","_ERR"]):
                continue

            if(origlabel[0:4] == "TAG_"):
                continue
            
            if(len(splitted)==1):
                label = splitted[0]
                scaleValue = 1
            elif(len(splitted)==2):  
                label=splitted[0]
                scaleValue=float(splitted[1].strip(')'))
            label_err = str(label)+"_ERR"

            uncertainty = None
            if(label_err in headers):
                if(pd.isna(row[label_err])):
                    uncertainty = None
                else:
                    uncertainty = row[label_err]
            else:
                uncertainty = None

            if(pd.isna(row[origlabel])): #Add other null criteria with or if needed here
                continue
            else:
                if('(' in str(row[origlabel])):
                    oldval = str(row[origlabel])
                    value = oldval.split('(')[0]
                    place_err = oldval.split('(')[1]

                    err_int_check = place_err.split(')')[0]
                    try:
                        int(err_int_check)
                    except ValueError:
                        if err_int_check is None:
                            pass
                        if(label in ["BINARY","CLOCK","EPHEM","UNITS"]):
                            pass
                        else:
                            print(f"ERROR: label:{label} uncertainty:{err_int_check} is not an int. EXITING")
                            exit()

 
                    
                    except ValueError:
                        if(label in ["RAJ","DECJ","BINARY","CLOCK","EPHEM","UNITS"]):
                            pass
                        else:
                            print(f"WARNING: label:{origlabel} value:{value} is not a float")
                    if(label == "RAJ"):
                        place_err = place_err.strip(')')
                        uncertainty = handleRAJ(value,place_err)         
                    elif(label == "DECJ"):
                        place_err = place_err.strip(')')
                        uncertainty = handleDECJ(value,place_err)
                    else:
                        split_err = place_err.split(')')
                        if(split_err[1] == ''):
                            place_err = split_err[0]
                            value,uncertainty = utils.getValueErr(str(value),str(place_err),float(scaleValue))
                        else:
                            place_err = split_err[0]
                            scaleVal = '1'+str(split_err[1])
                            #value,uncertainty = utils.getValueErr(str(value),str(place_err),float(scaleVal))
                            #value,uncertainty = utils.getValueErr(str(value),str(uncertainty),float(scaleValue))
                            value,uncertainty = utils.getValueErr(str(value),None,float(scaleValue))
                            value,uncertainty = utils.getValueErr(str(value),str(place_err),float(scaleVal))
                else:  
                    value = row[origlabel]
                    try:
                        if uncertainty is None:
                            value,uncertainty = utils.getValueErr(str(value),str('-'),float(scaleValue))
                        else:
                            if(label == "RAJ"):
                                uncertainty = handleRAJ(value,uncertainty)
                            elif(label == "DECJ"):
                                uncertainty = handleDECJ(value,uncertainty)
                            else:
                                value,uncertainty = utils.getValueErr(str(value),str(uncertainty),float(scaleValue))
                    except ValueError:
                        if(label in ["RAJ","DECJ","BINARY","CLOCK","EPHEM","UNITS"]):
                            pass
                        else:
                            print(f"WARNING: label:{origlabel} value:{value} is not a float")

           


            if(label == "TAG_PULSAR"):
                splitted = csv.reader([row[origlabel]],skipinitialspace=True,delimiter=delim).__next__()
                tagLabel = splitted[0]
                tagString = splitted[1]
                tagToPulsar_id = utils.addTagToPulsar(con,pulsar_id=pulsar_id,tagLabel=tagLabel,tagString=tagString,commit=commit)
                print("Added TagToPulsar: TagToPul_id:%s pul_id:%s tagL:%s tagS:%s" % (tagToPulsar_id,pulsar_id,tagLabel,tagString))
                continue

            if(label == "ASSOC"):
                splitted = csv.reader([row[origlabel]],skipinitialspace=True,delimiter=delim).__next__()
                assocLabel = splitted[0]
                assocName = splitted[1]
                if(assocName=='SMC' or assocName=='LMC'):
                    citation_id=None

                confidence=1
                if(len(splitted)==3):
                    confidence = splitted[2].strip()
                    if(confidence==2):
                        confidence = 2
                    else:
                        confidence = 1
                confidence = int(confidence)
                association_id = utils.addAssociation(con,pulsar_id,assocName,citation_id,confidence,label=assocLabel,commit=commit)
                print("Added assoc: assoc_id:%s cit_id:%s assocN:%s assocL:%s" % (association_id,citation_id,assocName,assocLabel))
                continue

            if(label == "TYPE"):
                splitted = csv.reader([row[origlabel]],skipinitialspace=True,delimiter=delim).__next__()
                sourceType = splitted[1]
                confidence = 1
                if(len(splitted=2)):
                    confidence = splitted[2].strip()
                    if(confidence==2):
                        confidence = 2
                    else:
                        confidence = 1
                confidence = int(confidence)
                sourceType_id = utils.addSourceType(con,pulsar_id,citation_id,confidence,label=sourceType,commit=commit)
                print("Added type: type_id:%s cit_id:%s type:%s" % (sourceType_id,citation_id,sourceType))
                continue

            if(label == "BINCOMP"):
                splitted = csv.reader([row[origlabel]],skipinitialspace=True,delimiter=delim).__next__()
                binaryType = splitted[1]
                confidence = 1
                if(len(splitted=2)):
                    confidence = splitted[2].strip()
                    if(confidence==2):
                        confidence = 2
                    else:
                        confidence = 1
                confidence = int(confidence)
                binaryType_id = utils.addBinaryType(con,pulsar_id,citation_id,confidence,label=binaryType,commit=commit)
                print("Added binary: bin_id:%s cit_id:%s bin:%s" % (binaryType_id,citation_id,binaryType))
                continue
                
            if(label == "DM"):
                if("DMEPOCH" in headers):
                    if(pd.isna(row['DMEPOCH'])):
                        referenceTime = referenceTime
                    else:
                        referenceTime = row['DMEPOCH']
            elif(label=="RAJ" or label=="DECJ" or label=="PMRA" or label=="PMDEC" or label=="ELAT" or label=="ELONG"or label=="PMELONG" or label=="PMELAT"):
                if("POSEPOCH" in headers):
                    if(pd.isna(row['POSEPOCH'])):
                        referenceTime = referenceTime
                    else:
                        referenceTime = row['POSEPOCH']

            label_mjd = str(label)+"_MJD"

            if(label_mjd in headers):
                if(pd.isna(row[label_mjd])):
                    referenceTime = None
                else:
                    referenceTime = row[label_mjd]

            observingSystem_id = None
            
            if(origlabel[0:4] == 'FLUX' or origlabel[0:5] == 'FRACL' or origlabel[0:5] == 'FRACV' or origlabel[0:6] == 'FRACAV'):
                if(origlabel[0:4] == 'FLUX'):
                   label = 'FLUX'
                elif(origlabel[0:5] == 'FRACL'):
                   label = 'FRACL'
                elif(origlabel[0:5] == 'FRACV'):
                   label = 'FRACV'
                elif(origlabel[0:6] == 'FRACAV'):
                   label = 'FRACAV'
                   
                freqbw = origlabel.split('[')[1].split(']')[0].split(delim)
                #linkedSet_id = 0
                approximate = 0
                if(len(freqbw)==1):
                    centralFrequency = freqbw[0]
                    bandwidth = None
                elif(len(freqbw)==2):
                    centralFrequency = freqbw[0]
                    bandwidth = freqbw[1]
                elif(len(freqbw)==3):
                    centralFrequency = freqbw[0]
                    bandwidth = freqbw[1]
                    approximate = freqbw[2]
                    if(approximate==1):
                        approximate = 1
                    else:
                        approximate = 0
                else:
                    print(f"{label} MEASUREMENT INCOMPLETE: Need centralFrequency and/or bandwidth for measurement")
                    continue
                approximate = int(approximate)
                centralFrequency = float(centralFrequency)
                if bandwidth is not None:
                    bandwidth = float(bandwidth)
                observingSystem_id = utils.addObservingSystem(con,systemLabel,centralFrequency,bandwidth,telescope,approximate=approximate,commit=commit)
            elif(origlabel[0:3] == "W10" or origlabel[0:3] == "W50"):
                label = origlabel[0:3]
                if(label == "W10"):
                    label = "WIDTH"
                    widthFraction = "10.0"
                    widthDescription = "Percentage of peak height"
                elif(label == "W50"):
                    label = "WIDTH"
                    widthFraction = "50.0"
                    widthDescription = "Percentage of peak height"
                else:
                    print("Unknown width label")
                    continue
                #widthFraction = int(origlabel[1:3])
                freqbw = origlabel.split('[')[1].split(']')[0].split(delim)
                approximate=0
                if(len(freqbw)==1):
                    centralFrequency = float(freqbw[0])
                    bandwidth = None
                elif(len(freqbw)==2):
                    centralFrequency = float(freqbw[0])
                    bandwidth = float(freqbw[1])
                    
                elif(len(freqbw)==3):
                    centralFrequency = float(freqbw[0])
                    bandwidth = float(freqbw[1])
                    approximate = float(freqbw[2])
                    if(approximate==1):
                        approximate = 1
                    else:
                        approximate = 0
                else:
                    print("WIDTH MEASUREMENT ERROR: Need centralFrequency and/or bandwidth for width measurement")
                    continue
                approximate = int(approximate)
                centralFrequency = float(centralFrequency)
                bandwidth = float(bandwidth)
                observingSystem_id = utils.addObservingSystem(con,systemLabel,centralFrequency,bandwidth,telescope,approximate=approximate,commit=commit)
            psrcat1label = label

            label,value,uncertainty,parameterType_id,timeDerivative,companionNumber = utils.parseGeneralParameters(label,value,uncertainty)
            #print(label,value,parameterType_id)
            if(value is None or parameterType_id is None):
                continue
            else:
                parameter_id = utils.addParameter(con,pulsar_id,citation_id,linkedSet_id,fitParameters_id,observingSystem_id,parameterType_id,timeDerivative,companionNumber,value,uncertainty,referenceTime,commit=commit)

                tag_label = "TAG_"+str(label)
                if(tag_label in headers):
                    if(pd.isna(row[tag_label])):
                        pass
                    else:
                        splitted = csv.reader([row[tag_label]],skipinitialspace=True,delimiter=delim).__next__()
                        tagLabel = splitted[0]
                        if(tagLabel == "asymmetricUncertainty"):
                            tagString = splitted[1]+','+splitted[2]
                        else:
                            tagString = splitted[1]
                        tagToParameter_id = utils.addTagToParameter(con,parameter_id,tagLabel=tagLabel,tagString=tagString,commit=commit)
                        print("Added TagToParameter: TagToPar_id:%s par_id:%s tagL:%s tagS:%s" % (tagToParameter_id,parameter_id,tagLabel,tagString))

                if(label == "WIDTH"):
                    ancillary_id = utils.addAncillary(con,parameter_id,widthFraction,widthDescription,commit=commit)
                if(label == "FLUX" or label == "FRACL" or label == "FRACV" or label == "FRACAV"):
                    print("Added parameter: p_id:%s cit_id:%s label:%s val:%s err:%s obs_id:%s mjd:%s\n" % (parameter_id,citation_id,psrcat1label,value,uncertainty,observingSystem_id,referenceTime))
                elif(label == "WIDTH"):
                    print("Added parameter: p_id:%s cit_id:%s label:%s val:%s err:%s obs_id:%s mjd:%s anc_id:%s\n" % (parameter_id,citation_id,psrcat1label,value,uncertainty,observingSystem_id,referenceTime,ancillary_id))
                else:
                    print("Added parameter: p_id:%s cit_id:%s label:%s value:%s err:%s mjd:%s\n" % (parameter_id,citation_id,psrcat1label,value,uncertainty,referenceTime))


