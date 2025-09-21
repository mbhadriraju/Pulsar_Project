import utils
import derive
import sqlite3
import pandas as pd
import numpy as np
import pygedm
from functools import reduce

# Copyright: CSIRO 2024
# Author: Agastya Kapur: Agastya.Kapur@csiro.au, George Hobbs: George.Hobbs@csiro.au

con = utils.connect_db()

ymw_citation_id = utils.getCitationIDFromV1Label(con,'ymw17')
ne_citation_id = utils.getCitationIDFromV1Label(con,'cl02')

cur = con.cursor()

# Remove derived parameters from the parameter table
cur.execute("DELETE FROM parameter where parameter_id IN (SELECT parameter_id FROM derived)")

# Clear derived and derived to parameter table
cur.execute("DELETE FROM derived")
cur.execute("DELETE FROM derivedFromParameter")

# Clear distance table
cur.execute("DELETE FROM distance WHERE label IN ('DIST_YMW16','DIST_NE2001')")

# Commit deletions
con.commit()
# Get DIST

def getDist(distA,distA_id,distYMW,distYMW_id,PX,PX_err,PX_id,distAMN,distAMN_id,distAMX,distAMX_id):
    if pd.notna(distA):
        return distA,distA_id
    elif pd.notna(PX) and pd.notna(PX_err) and float(PX)>3*float(PX_err):
        dist_px = derive.parallax_to_dist(float(PX))
        return dist_px,PX_id           
    elif (pd.notna(distAMN) and pd.notna(distAMX)):
        if pd.notna(distYMW):
            if(distYMW>distAMN and distYMW<distAMX):
                return distYMW,distYMW_id
            else:
                return (float(distAMN)+float(distAMX))/2,distAMN_id
        else:
            return (float(distAMN)+float(distAMX))/2,distAMN_id
    elif pd.notna(distYMW):
        return distYMW,distYMW_id
    else:
        return None,None

def getDist1(distA,distA_id,distNE,distNE_id,PX,PX_err,PX_id,distAMN,distAMN_id,distAMX,distAMX_id):
    if pd.notna(distA):
        return distA,distA_id
    elif pd.notna(PX) and pd.notna(PX_err) and float(PX)>3*float(PX_err):
        dist_px = derive.parallax_to_dist(float(PX))
        return dist_px,PX_id
    elif (pd.notna(distAMN) and pd.notna(distAMX)):
        if pd.notna(distNE):
            if(distNE>distAMN and distNE<distAMX):
                return distNE,distNE_id
            else:
                return (float(distAMN)+float(distAMX))/2,distAMN_id
        else:
            return (float(distAMN)+float(distAMX))/2,distAMN_id 
    elif pd.notna(distNE):
        return distNE,distNE_id
    else:
        return None,None

def inputDerived(con,pulsar_id,citation_id,observingSystem_id,label,timeDerivative,companionNumber,value,uncertainty,referenceTime,method,methodVersion,derivedFrom):
    parameterType_id = utils.getParameterTypeID(con,label)
    parameter_id = utils.addParameter(con,pulsar_id,citation_id,None,None,observingSystem_id,parameterType_id,timeDerivative,companionNumber,value,uncertainty,referenceTime)
    derived_id = utils.addDerived(con,parameter_id,method,methodVersion)
    for derparam in derivedFrom:
        derivedFromParameter_id = utils.addDerivedFromParameter(con,derived_id,derparam)
    return parameter_id

def deriveDMandPosition(dmdist_merged,con):
    for index, row in dmdist_merged.iterrows():
        
        pulsar_id = row['pulsar_id']
        DM = float(row['DM'])
        DM_id = row['DM_id']
        DM_mjd = row['DM_MJD']
        RAJ = row['RAJ']
        RAJ_id = row['RAJ_id']
        RAJ_mjd = row['RAJ_MJD']
        DECJ = row['DECJ']
        DECJ_id = row['DECJ_id']
        DECJ_mjd = row['DECJ_MJD']
        ELONG = row['ELONG']
        ELONG_id = row['ELONG_id']
        ELONG_mjd = row['ELONG_MJD']
        ELAT = row['ELAT']
        ELAT_id = row['ELAT_id']
        ELAT_mjd = row['ELAT_MJD']
        GL = None
        GB = None
        RAJD = None
        DECJD = None

        # Case to check just one pulsar
        #if(pulsar_id!=3798):
        #    continue

        
        #if(pulsar_id%300 == 0):
        print(f"Currently at pulsar_id: {pulsar_id}")


        # Check which to use
        if(pd.isna(RAJ) and pd.isna(ELONG)):
            continue

    
        # Going with more recent for now
        if(pd.notna(RAJ) and pd.notna(DECJ) and pd.notna(ELONG) and pd.notna(ELAT)):
            if(RAJ_id > ELONG_id):
                useRAJ = True
            else:
                useRAJ = False
        elif(pd.notna(RAJ) and pd.notna(DECJ)):
            useRAJ = True
        elif(pd.notna(ELONG) and pd.notna(ELAT)):
            useRAJ = False

        # Derive positions here.

        if useRAJ:
            if(RAJ_mjd != DECJ_mjd):
                print("Not set up properly. Need review")
            RAJD = derive.hhmmss_to_degrees(RAJ)
            DECJD = derive.ddmmss_to_degrees(DECJ)
            galactic = derive.equatorial_to_galactic(RAJD,DECJD)
            GL = float(galactic.l.value)
            GB = float(galactic.b.value)
            ecliptic = derive.equatorial_to_ecliptic(RAJD,DECJD)
            ELONG = float(ecliptic.lon.value)
            ELAT = float(ecliptic.lat.value)


            #inputDerived(con,pulsar_id,citation_id,observingSystem_id,label,timeDerivative,companionNumber,value,uncertainty,referenceTime,method,methodVersion,derivedFrom)
 
            # Input RAJD,DECJD,ELONG,ELAT,GL,GB

            # Input RAJD
            # if RAJD is not None:
            #     RAJD_id = inputDerived(con,pulsar_id,None,None,'RAJD',0,0,RAJD,None,RAJ_mjd,raj_to_rajd_method,methodVersion,[RAJ_id])
            
            # # Input DECJD
            # if DECJD is not None:
            #     DECJD_id = inputDerived(con,pulsar_id,None,None,'DECJD',0,0,DECJD,None,RAJ_mjd,decj_to_decjd_method,methodVersion,[DECJ_id])

            # # Input ELONG
            # if ELONG is not None:
            #     ELONG_id = inputDerived(con,pulsar_id,None,None,'ELONG',0,0,ELONG,None,RAJ_mjd,equatorial_to_ecliptic_method,methodVersion,[RAJD_id,DECJD_id])
            
            # # Input ELAT
            # if ELAT is not None:
            #     ELAT_id = inputDerived(con,pulsar_id,None,None,'ELAT',0,0,ELAT,None,RAJ_mjd,equatorial_to_ecliptic_method,methodVersion,[RAJD_id,DECJD_id])

            # # Input GL
            # if GL is not None:
            #     GL_id = inputDerived(con,pulsar_id,None,None,'GL',0,0,GL,None,RAJ_mjd,equatorial_to_galactic_method,methodVersion,[RAJD_id,DECJD_id])
            
            # # Input GB
            # if GB is not None:
            #     GB_id = inputDerived(con,pulsar_id,None,None,'GB',0,0,GB,None,RAJ_mjd,equatorial_to_galactic_method,methodVersion,[RAJD_id,DECJD_id])
            
        else:
            if(ELONG_mjd != ELAT_mjd):
                print("Not set up properly. Need review")

            galactic = derive.ecliptic_to_galactic(ELONG,ELAT)
            GL = float(galactic.l.value)
            GB = float(galactic.b.value)
            equatorial = derive.ecliptic_to_equatorial(ELONG,ELAT)
            RAJD = float(equatorial.ra.value)
            DECJD = float(equatorial.dec.value)
            RAJDECJ = equatorial.to_string('hmsdms',sep=':').split(' ')
            RAJ = RAJDECJ[0]
            DECJ = RAJDECJ[1]

            # Input RAJ,DECJ,RAJD,DECJD,GL,GB
            # Input RAJD
            # if RAJD is not None:
            #     RAJD_id = inputDerived(con,pulsar_id,None,None,'RAJD',0,0,RAJD,None,ELONG_mjd,ecliptic_to_equatorial_method,methodVersion,[ELONG_id,ELAT_id])

            # # Input DECJD
            # if DECJD is not None:
            #     DECJD_id = inputDerived(con,pulsar_id,None,None,'DECJD',0,0,DECJD,None,ELONG_mjd,ecliptic_to_equatorial_method,methodVersion,[ELONG_id,ELAT_id])

            # # Input RAJ
            # if RAJ is not None:
            #     RAJ_id = inputDerived(con,pulsar_id,None,None,'RAJ',0,0,RAJ,None,ELONG_mjd,ecliptic_to_equatorial_method,methodVersion,[ELONG_id,ELAT_id])

            # # Input DECJ
            # if DECJ is not None:
            #     DECJ_id = inputDerived(con,pulsar_id,None,None,'DECJ',0,0,DECJ,None,ELONG_mjd,ecliptic_to_equatorial_method,methodVersion,[ELONG_id,ELAT_id])

            # # Input GL
            # if GL is not None:
            #     GL_id = inputDerived(con,pulsar_id,None,None,'GL',0,0,GL,None,ELONG_mjd,ecliptic_to_galactic_method,methodVersion,[ELONG_id,ELAT_id])

            # # Input GB
            # if GB is not None:
            #     GB_id = inputDerived(con,pulsar_id,None,None,'GB',0,0,GB,None,ELONG_mjd,ecliptic_to_galactic_method,methodVersion,[ELONG_id,ELAT_id])

        if(pd.isna(DM)):
            continue
        if GL is not None and GB is not None:
            if(pd.notna(row['ASSOC'])):
                dist_ymw,ymw_tau = derive.dm_to_dist(GL,GB,DM,method='ymw16',mode='MC')
            else:
                dist_ymw,ymw_tau = derive.dm_to_dist(GL,GB,DM,method='ymw16')
            dist_ne,ne_tau = derive.dm_to_dist(GL,GB,DM,method='ne2001')

            dist_ymw16 = "{:.3f}".format(dist_ymw.value/1000) #Convert to kpc with 3dp
            dist_ne2001 = "{:.3f}".format(dist_ne.value/1000) #Convert to kpc

            #inputDerived(con,pulsar_id,citation_id,observingSystem_id,label,timeDerivative,companionNumber,value,uncertainty,referenceTime,method,methodVersion,derivedFrom
            if dist_ymw16 is not None:
                ymw_id = inputDerived(con,pulsar_id,ymw_citation_id,None,'DIST_YMW16',0,0,dist_ymw16,None,None,distYMW_method,methodVersion,[DM_id])
            if dist_ne is not None:
                ne_id = inputDerived(con,pulsar_id,ne_citation_id,None,'DIST_NE2001',0,0,dist_ne2001,None,None,distNE_method,methodVersion,[DM_id])
        #if(pulsar_id==20):
        #    break
def deriveSpinParams(spin_merged,con):
    for index, row in spin_merged.iterrows():
        pulsar_id = row['pulsar_id']
        P0 = row['P0']
        F0 = row['F0']
        P0_err = row['P0_ERR']
        F0_err = row['F0_ERR']
        P0_id = row['P0_id']
        F0_id = row['F0_id']

        # Used to test for a single pulse
        # if(pulsar_id!=1899):
        #    continue

        #print(pulsar_id)
        
        if(pulsar_id%300 == 0):
            print(f"Currenty at pulsar_id: {pulsar_id}") 
        # Setting NULLs for next case
        P1 = None
        P1_err = None
        P1_id = None
        F1 = None
        F1_err = None
        F1_id = None

        DIST_A = row['DIST_A']
        DIST_A_id = row['DIST_A_id']
        DIST_YMW16 = row['DIST_YMW16']
        DIST_YMW16_id = row['DIST_YMW16_id']
        DIST_NE2001 = row['DIST_NE2001']
        DIST_NE2001_id = row['DIST_NE2001_id']
        DIST_AMN = row['DIST_AMN']
        DIST_AMN_id = row['DIST_AMN_id']
        DIST_AMX = row['DIST_AMX']
        DIST_AMX_id = row['DIST_AMX_id']
        PX = row['PX']
        PX_err = row['PX_ERR']
        PX_id = row['PX_id']

        DIST,deriveFromDIST_id = getDist(DIST_A,DIST_A_id,DIST_YMW16,DIST_YMW16_id,PX,PX_err,PX_id,DIST_AMN,DIST_AMN_id,DIST_AMX,DIST_AMX_id)
        DIST1,deriveFromDIST1_id = getDist1(DIST_A,DIST_A_id,DIST_NE2001,DIST_NE2001_id,PX,PX_err,PX_id,DIST_AMN,DIST_AMN_id,DIST_AMX,DIST_AMX_id)
        #print(f" here with {pulsar_id}")
        #print(DIST_A,DIST_YMW16,PX)
        if DIST1 is not None:
            # Input DIST1
            DIST1 = "%-6.3f" % float(DIST1)
            DIST1_id = inputDerived(con,pulsar_id,None,None,'DIST1',0,0,DIST1,None,None,dist1_method,methodVersion,[deriveFromDIST1_id])

        if DIST is not None:
            # Input DIST
            DIST = "%-6.3f" % float(DIST)
            DIST_id = inputDerived(con,pulsar_id,None,None,'DIST',0,0,DIST,None,None,dist_method,methodVersion,[deriveFromDIST_id])

        #print(DIST)
        # Both F0 and P0 are NULL
        if(pd.isna(F0) and pd.isna(P0)):
            continue

        if(pd.notna(F0) and pd.notna(P0)):
            # Always derive from F0 ?? Pick which parameter is better ?? ( more decimals ? more recent ? )
            # Going with more recent for now
            if(P0_id < F0_id):
                useF = True
            elif(P0_id > F0_id):
                useF = False
        elif(pd.notna(F0)):
            useF = True
        elif(pd.notna(P0)):
            useF = False

        if useF:
            # Deriving Ps from Fs
            #F0_err = row['F0_ERR']
            F0_mjd = row['F0_MJD']
            #if(pd.isna(row['F0_citation_id'])):
            #    continue
            #F0_citation_id = int(row['F0_citation_id'])
            P0,P0_err = derive.f0_to_p0(F0,F0_err)
            # Insert P0,P0_ERR into the database
            if P0 is not None:
                P0_id = inputDerived(con,pulsar_id,None,None,'P',0,0,P0,P0_err,F0_mjd,f0_to_p0_method,methodVersion,[F0_id])

            F1temp = cur.execute("SELECT value,uncertainty,max(parameter_id) FROM viewParameter WHERE (label LIKE 'F' AND referenceTime LIKE ? AND timeDerivative=1 AND pulsar_id LIKE ? AND value IS NOT NULL)",(F0_mjd,pulsar_id))
            F1dat = F1temp.fetchone()
            F1 = F1dat[0]
            F1_err = F1dat[1]
            F1_id = F1dat[2]

            if(F1 is not None and F1_id is not None):
                #print(f"{pulsar_id} here with {F0},{F1}")
                P1,P1_err = derive.f1_to_p1(F0,F1,F1_err)
                # Insert P1,P1_ERR into the database
                if P1 is not None:
                    P1_id = inputDerived(con,pulsar_id,None,None,'P',1,0,P1,P1_err,F0_mjd,f1_to_p1_method,methodVersion,[F0_id,F1_id])                

        else:
            # Deriving Fs Prom Ps
            #P0_err = row['P0_ERR']
            P0_mjd = row['P0_MJD']
            #if(pd.isna(row['P0_citation_id'])):
            #    continue
            #P0_citation_id = int(row['P0_citation_id'])
            
            F0,F0_err = derive.p0_to_f0(P0,P0_err)
            # Insert F0,F0_err into the database
            if F0 is not None:
                F0_id = inputDerived(con,pulsar_id,None,None,'F',0,0,F0,F0_err,P0_mjd,p0_to_f0_method,methodVersion,[P0_id])

            P1temp = cur.execute("SELECT value,uncertainty,max(parameter_id) FROM viewParameter WHERE (label LIKE 'P' AND referenceTime LIKE ? AND timeDerivative=1 AND pulsar_id LIKE ? AND value IS NOT NULL)",(P0_mjd,pulsar_id))
            P1dat = P1temp.fetchone()
            P1 = P1dat[0]
            P1_err = P1dat[1]
            P1_id = P1dat[2]

            if(P1 is not None and P1_id is not None):
                F1,F1_err = derive.p1_to_f1(P0,P1,P1_err)
                # Input F1,F1_err into the DB
                if F1 is not None:
                    F1_id = inputDerived(con,pulsar_id,None,None,'F',1,0,F1,F1_err,P0_mjd,p1_to_f1_method,methodVersion,[P0_id,P1_id])

        #print(pulsar_id,P0,P1)
        if P0 is not None and P1 is not None:

            #print(f" here with {pulsar_id}")
            P0 = float(P0)
            P1 = float(P1)

            #derive age
            AGE = derive.derive_age(P0,P1)
            if AGE is not None:
                #AGE = float(AGE)
                AGE_id = inputDerived(con,pulsar_id,None,None,'AGE',0,0,AGE,None,None,age_method,methodVersion,[P0_id,P1_id])
            #derive bsurf
            BSURF = derive.derive_bsurf(P0,P1)
            if BSURF is not None:
                #BSURF = float(BSURF)
                BSURF_id = inputDerived(con,pulsar_id,None,None,'BSURF',0,0,BSURF,None,None,bsurf_method,methodVersion,[P0_id,P1_id])
            #derive B_lc
            B_LC = derive.derive_b_lc(P0,P1)
            if B_LC is not None:
                #B_LC = float(B_LC)
                B_LC_id = inputDerived(con,pulsar_id,None,None,'B_LC',0,0,B_LC,None,None,b_lc_method,methodVersion,[P0_id,P1_id])
            #derive Edot
            EDOT = derive.derive_edot(P0,P1)
            if EDOT is not None:
                #EDOT = float(EDOT)
                EDOT_id = inputDerived(con,pulsar_id,None,None,'EDOT',0,0,EDOT,None,None,edot_method,methodVersion,[P0_id,P1_id])

            if DIST is not None:
                DIST = float(DIST)
                # Derive and input EDOTD2
                EDOTD2 = derive.derive_edotd2(P0,P1,DIST)
    
                if EDOTD2 is not None:
                    EDOTD2 = float(EDOTD2)
                    EDOTD2_id = inputDerived(con,pulsar_id,None,None,'EDOTD2',0,0,EDOTD2,None,None,edotd2_method,methodVersion,[P0_id,P1_id,DIST_id])

                PMRA = row['PMRA']
                PMRA_id = row['PMRA_id']
                PMRA_err = row['PMRA_ERR']
                PMDEC = row['PMDEC']
                PMDEC_id = row['PMDEC_id']
                PMDEC_err = row['PMDEC_ERR']
                PMELONG = row['PMELONG']
                PMELONG_id = row['PMELONG_id']
                PMELONG_err = row['PMELONG_ERR']
                PMELAT = row['PMELAT']
                PMELAT_id = row['PMELAT_id']
                PMELAT_err = row['PMELAT_ERR']

                if(pd.notna(PMRA) and pd.notna(PMDEC) and pd.notna(PMELONG) and pd.notna(PMELAT)):
                    if(PMRA_id > PMELONG_id):
                        usePMRA = True
                    else:
                        usePMRA = False
                elif(pd.notna(PMRA) and pd.notna(PMDEC)):
                    usePMRA = True
                elif(pd.notna(PMELONG) and pd.notna(PMELAT)):
                    usePMRA = False
                else:
                    continue
                pmx = None
                pmy = None
                pmx_id = None
                pmy_id = None
                pmx_err = None
                pmy_err = None
                
                if usePMRA:
                    #print('j1')
                    #print(f"using PMRA for {pulsar_id}") 
                    pmx = PMRA
                    pmy = PMDEC
                    pmx_id = PMRA_id
                    pmy_id = PMDEC_id
                    if(pd.isna(PMRA_err)):
                        pmx_err = None
                    else:
                        pmx_err = float(PMRA_err)
                    if(pd.isna(PMDEC_err)):
                        pmy_err = None
                    else:
                        pmy_err = float(PMDEC_err)
                    #print(pmx_err,pmy_err)
                elif not usePMRA:
                    #print('j2')
                    #print(f"using PMELAT for {pulsar_id}")
                    pmx = PMELONG
                    pmy = PMELAT
                    pmx_id = PMELONG_id
                    pmy_id = PMELAT_id
                    if(pd.isna(PMELONG_err)):
                        pmx_err = None
                    else:
                        pmx_err = float(PMELONG_err)
                    if(pd.isna(PMELAT_err)):
                        pmy_err = None
                    else:
                        pmy_err = float(PMELAT_err)

                #print(pmx_err,pmy_err)
                if(pmx is not None and pmy is not None):
                    pmx = float(pmx)
                    pmy = float(pmy)
                    PMTOT,PMTOT_err = derive.pmtot(pmx,pmy,pmx_err,pmy_err)
                    if PMTOT_err is not None:
                        PMTOT_err = float(PMTOT_err)

                    if PMTOT is not None:
                        PMTOT = float(PMTOT)
                        PMTOT_id = inputDerived(con,pulsar_id,None,None,'PMTOT',0,0,PMTOT,PMTOT_err,None,pmtot_method,methodVersion,[pmx_id,pmy_id])

                        VTRANS = derive.vtrans(PMTOT,DIST)
                        if VTRANS is not None:
                            VTRANS = float(VTRANS)
                            VTRANS_id = inputDerived(con,pulsar_id,None,None,'VTRANS',0,0,VTRANS,None,None,vtrans_method,methodVersion,[PMTOT_id,DIST_id])

                            P1_I = derive.p1_I(P0,P1,DIST,VTRANS)
                            if P1_I is not None:
                                P1_I = float(P1_I)
    
                                if(P1_I > 0):
                                    #print(pulsar_id,P0,P1,DIST,VTRANS)
                                    #print(P1_I)
                                    P1_I_id = inputDerived(con,pulsar_id,None,None,'P1_I',0,0,P1_I,None,None,p1_i_method,methodVersion,[P0_id,P1_id,DIST_id,VTRANS_id])
                                    
                                    AGE_I = derive.age_i(P0,P1_I)
                                    if AGE_I is not None:
                                        AGE_I = float(AGE_I)
                                        AGE_I_id = inputDerived(con,pulsar_id,None,None,'AGE_I',0,0,AGE_I,None,None,age_i_method,methodVersion,[P0_id,P1_I_id])

                                    BSURF_I = derive.bsurf_i(P0,P1_I)
                                    if BSURF_I is not None:
                                        BSURF_I = float(BSURF_I)
                                        BSURF_I_id = inputDerived(con,pulsar_id,None,None,'BSURF_I',0,0,BSURF_I,None,None,bsurf_i_method,methodVersion,[P0_id,P1_I_id])

                                    EDOT_I = derive.edot_i(P0,P1_I)
                                    if EDOT_I is not None:
                                        EDOT_I = float(EDOT_I)
                                        EDOT_I_id = inputDerived(con,pulsar_id,None,None,'EDOT_I',0,0,EDOT_I,None,None,edot_i_method,methodVersion,[P0_id,P1_I_id])


                        
        #if(pulsar_id==20):
        #    break
# Derivation methods


methodVersion = "1.0"

p0_to_f0_method = "f0=1/p0, f0_err=p0_err*f0*f0"
f0_to_p0_method = "p0=1/f0, p0_err=f0_err*p0*p0"
p1_to_f1_method = "f1=-1*f0*f0*p1, f1_err=((f0*f0*p1_err)**2+(2*f0*f0*p1*p1_err)**2)**(0.5)"
f1_to_p1_method = "p1=-1*p0*p0*f1, p1_err=((p0*p0*f1_err)**2+(2*p0*p0*f1*f1_err)**2)**(0.5)"
age_method =  "age=p0/2.0/p1/60.0/60.0/24.0/365.25"
bsurf_method = "bsurf=3.2e19*(p0*p1)**0.5"
b_lc_method = "b_lc=c*(p1**0.5)*p0**(-5.0/2.0)"
edot_method = "4.0*(pi**2)*1.0e45*p1/(p0**3)"
distYMW_method = "pygedm.dm_to_dist"
distNE_method = "pygedm.dm_to_dist"
dist_method = "Priority order: 1. DIST_A, 2. 1/Parallax, 3. If DIST_YMW16 is between DISTAMN/AMX, then DIST_YMW16, else average of the distance limits. 4. DIST_YMW16"
dist1_method = "Priority order: 1. DIST_A, 2. 1/Parallax, 3. If DIST_NE2001 is between DISTAMN/AMX, then DIST_NE2001, else average of the distance limits.  4. DIST_NE2001"
raj_to_rajd_method = "15*hh+15*mm/60+15*ss/3600"
decj_to_decjd_method = "+ve dec: dd+mm/60+ss/3600. -ve dec:dd-mm/60-ss/3600"
equatorial_to_ecliptic_method = "astropy transform from icrs to geocentricmeanecliptic"
ecliptic_to_equatorial_method = "astropy transform from geocentricmeanecliptic to icrs"
equatorial_to_galactic_method = "astropy transform from icrs to galactic"
ecliptic_to_galactic_method = "astropy transform from geocentricmeanecliptic to icrs"
edotd2_method = "4.0*(pi**2)*1.0e45*p1/(p0**3)/(dist**2)"
pmtot_method = "pmtot=(pmx**2+pmy**2)**0.5, pmtot_err=((pmx*pmx_err)**2+(pmy*pmy_err)**2)**0.5/(pmx**2+pmy**2) [pmx = RAJ/ELONG and pmy = DECJ/ELAT]"
vtrans_method = "vtrans = pmtot/1000.0/3600.0/180.0*np.pi/365.25/86400.0*3.086e16*dist"
p1_i_method = "p1_1=(p1/1.0e15-(vtrans**2)*1.0e10*p0/(dist*3.086e6)/2.9979e10)*1.0e-15"
age_i_method = "(p0/2.0/p1_i)/60.0/60.0/24.0/365.25"
bsurf_i_method = "bsurf_i=3.2e19*(p0*p1_i)**0.5"
edot_i_method = "edot_i=4.0/1.0e-15*(np.pi**2)*p1_i/p0**3*1.0e30"


dm_query = "SELECT DISTINCT viewParameter.pulsar_id AS pulsar_id,value as 'DM',uncertainty as 'DM_ERR',max(parameter_id) AS DM_id,referenceTime as DM_MJD FROM viewParameter WHERE (viewParameter.label='DM' and timeDerivative=0 AND value IS NOT NULL) GROUP BY viewParameter.pulsar_id"

raj_query = "SELECT DISTINCT viewParameter.pulsar_id AS pulsar_id,value as 'RAJ',uncertainty as 'RAJ_ERR',max(parameter_id) AS RAJ_id,referenceTime AS RAJ_MJD FROM viewParameter WHERE (viewParameter.label='RAJ' and timeDerivative=0 AND value IS NOT NULL) GROUP BY viewParameter.pulsar_id"

decj_query = "SELECT DISTINCT viewParameter.pulsar_id AS pulsar_id,value as 'DECJ',uncertainty as 'DECJ_ERR',max(parameter_id) AS DECJ_id,referenceTime AS DECJ_MJD FROM viewParameter WHERE (viewParameter.label='DECJ' and timeDerivative=0 AND value IS NOT NULL) GROUP BY viewParameter.pulsar_id"

elong_query = "SELECT DISTINCT viewParameter.pulsar_id AS pulsar_id,value as 'ELONG',uncertainty as 'ELONG_ERR',max(parameter_id) AS ELONG_id,referenceTime AS ELONG_MJD FROM viewParameter WHERE (viewParameter.label='ELONG' and timeDerivative=0 AND value IS NOT NULL) GROUP BY viewParameter.pulsar_id"

elat_query = "SELECT DISTINCT viewParameter.pulsar_id AS pulsar_id,value as 'ELAT',uncertainty as 'ELAT_ERR',max(parameter_id) AS ELAT_id,referenceTime AS ELAT_MJD FROM viewParameter WHERE (viewParameter.label='ELAT' and timeDerivative=0 AND value IS NOT NULL) GROUP BY viewParameter.pulsar_id"

assoc_query = "SELECT DISTINCT association.pulsar_id AS pulsar_id,name as 'ASSOC' FROM association WHERE name IN ('SMC','LMC') GROUP BY association.pulsar_id"

p0_query = "SELECT DISTINCT viewParameter.pulsar_id AS pulsar_id,viewParameter.[parameter.citation_id] AS P0_citation_id,fitParameters_id AS P0_fitParameters_id,observingSystem_id AS P0_observingSystem_id,timeDerivative AS P0_timeDerivative,companionNumber AS P0_companionNumber,value as 'P0',uncertainty as 'P0_ERR',referenceTime AS P0_MJD,max(parameter_id) AS P0_id FROM viewParameter WHERE (viewParameter.label='P' and timeDerivative=0 AND value IS NOT NULL) GROUP BY viewParameter.pulsar_id ORDER BY viewParameter.pulsar_id ASC"

f0_query = "SELECT DISTINCT viewParameter.pulsar_id AS pulsar_id,viewParameter.[parameter.citation_id] AS F0_citation_id,fitParameters_id AS F0_fitParameters_id,observingSystem_id AS F0_observingSystem_id,timeDerivative AS F0_timeDerivative,companionNumber AS F0_companionNumber,value as 'F0',uncertainty as 'F0_ERR',referenceTime AS F0_MJD,max(parameter_id) AS F0_id FROM viewParameter WHERE (viewParameter.label='F' and timeDerivative=0 AND value IS NOT NULL) GROUP BY viewParameter.pulsar_id ORDER BY viewParameter.pulsar_id ASC"

#flux_query = "SELECT viewParameter.pulsar_id AS pulsar_id,value as 'FLUX',max(parameter_id) AS FLUX_parameter_id FROM viewParameter WHERE (viewParameter.label='FLUX' and timeDerivative=0 AND value IS NOT NULL) GROUP BY viewParameter.pulsar_id"

distA_query = "SELECT DISTINCT viewParameter.pulsar_id AS pulsar_id,max(parameter_id) AS DIST_A_id,value AS DIST_A FROM viewParameter WHERE (label LIKE 'DIST_A' AND value IS NOT NULL) GROUP BY pulsar_id ORDER BY viewParameter.pulsar_id ASC"

distYMW_query = "SELECT DISTINCT viewParameter.pulsar_id AS pulsar_id,max(parameter_id) AS DIST_YMW16_id,value AS DIST_YMW16 FROM viewParameter WHERE (label LIKE 'DIST_YMW16' AND value IS NOT NULL) GROUP BY pulsar_id ORDER BY viewParameter.pulsar_id ASC"

distNE_query = "SELECT DISTINCT viewParameter.pulsar_id AS pulsar_id,max(parameter_id) AS DIST_NE2001_id,value AS DIST_NE2001 FROM viewParameter WHERE (label LIKE 'DIST_NE2001' AND value IS NOT NULL) GROUP BY pulsar_id ORDER BY viewParameter.pulsar_id ASC"

distAMX_query = "SELECT DISTINCT viewParameter.pulsar_id AS pulsar_id,max(parameter_id) AS DIST_AMX_id,value AS DIST_AMX FROM viewParameter WHERE (label LIKE 'DIST_AMX' AND value IS NOT NULL) GROUP BY pulsar_id ORDER BY viewParameter.pulsar_id ASC"
distAMN_query = "SELECT DISTINCT viewParameter.pulsar_id AS pulsar_id,max(parameter_id) AS DIST_AMN_id,value AS DIST_AMN FROM viewParameter WHERE (label LIKE 'DIST_AMN' AND value IS NOT NULL) GROUP BY pulsar_id ORDER BY viewParameter.pulsar_id ASC"

px_query = "SELECT DISTINCT viewParameter.pulsar_id AS pulsar_id,viewParameter.value AS 'PX',max(parameter_id) AS PX_id,uncertainty AS PX_ERR FROM viewParameter WHERE (label LIKE 'PX' AND value IS NOT NULL) GROUP BY viewParameter.pulsar_id ORDER BY viewParameter.pulsar_id ASC"

pmra_query = "SELECT DISTINCT viewParameter.pulsar_id AS pulsar_id,max(parameter_id) AS PMRA_id,value AS PMRA,uncertainty AS PMRA_ERR FROM viewParameter WHERE (label LIKE 'PMRA' AND value IS NOT NULL)  GROUP BY pulsar_id ORDER BY viewParameter.pulsar_id ASC"

pmdec_query = "SELECT DISTINCT viewParameter.pulsar_id AS pulsar_id,max(parameter_id) AS PMDEC_id,value AS PMDEC,uncertainty AS PMDEC_ERR FROM viewParameter WHERE (label LIKE 'PMDEC' AND value IS NOT NULL) GROUP BY pulsar_id ORDER BY viewParameter.pulsar_id ASC"

pmelong_query = "SELECT DISTINCT viewParameter.pulsar_id AS pulsar_id,max(parameter_id) AS PMELONG_id,value AS PMELONG,uncertainty AS PMELONG_ERR FROM viewParameter WHERE (label LIKE 'PMELONG' AND value IS NOT NULL) GROUP BY pulsar_id ORDER BY viewParameter.pulsar_id ASC"

pmelat_query = "SELECT DISTINCT viewParameter.pulsar_id AS pulsar_id,max(parameter_id) AS PMELAT_id,value AS PMELAT,uncertainty AS PMELAT_ERR FROM viewParameter WHERE (label LIKE 'PMELAT' AND value IS NOT NULL) GROUP BY pulsar_id ORDER BY viewParameter.pulsar_id ASC"


# Used for DM,position derivation
dm = pd.read_sql_query(dm_query,con)
raj = pd.read_sql_query(raj_query,con)
decj = pd.read_sql_query(decj_query,con)
elong = pd.read_sql_query(elong_query,con)
elat = pd.read_sql_query(elat_query,con)
assoc = pd.read_sql_query(assoc_query,con)

#Merging dataframe code adapted from https://stackoverflow.com/questions/44327999/how-to-merge-multiple-dataframes
dmdist_data_frames = [dm,raj,decj,elong,elat,assoc]
dmdist_merged = reduce(lambda  left,right: pd.merge(left,right,on=['pulsar_id'],how='outer'), dmdist_data_frames)
dmdist_merged = dmdist_merged.sort_values('pulsar_id')

### Derive DIST_DMs
print("DERIVING DM AND POS NOW")
deriveDMandPosition(dmdist_merged,con)
print("DM AND POS DERIVED")

# Used for P0,P1,F0,F1,DIST and dependent parameters
p0 = pd.read_sql_query(p0_query,con)
f0 = pd.read_sql_query(f0_query,con)
distA = pd.read_sql_query(distA_query,con)
distYMW = pd.read_sql_query(distYMW_query,con)
distNE = pd.read_sql_query(distNE_query,con)
distAMN = pd.read_sql_query(distAMN_query,con)
distAMX = pd.read_sql_query(distAMX_query,con)
px = pd.read_sql_query(px_query,con)
pmra = pd.read_sql_query(pmra_query,con)
pmdec = pd.read_sql_query(pmdec_query,con)
pmelong = pd.read_sql_query(pmelong_query,con)
pmelat = pd.read_sql_query(pmelat_query,con)

#print(pmra)
#print(pmdec)
#print(distYMW)

### Derive Period/Frequencies

spin_data_frames = [p0,f0,distA,distYMW,distNE,distAMN,distAMX,px,pmra,pmdec,pmelong,pmelat]
spin_merged = reduce(lambda  left,right: pd.merge(left,right,on=['pulsar_id'],how='outer'), spin_data_frames)
spin_merged = spin_merged.sort_values('pulsar_id')

print("DERIVING SPIN PARAMS NOW")
#deriveSpinParams(spin_merged,con)
print("FINISHED DERIVING SPIN PARAMS")

        




