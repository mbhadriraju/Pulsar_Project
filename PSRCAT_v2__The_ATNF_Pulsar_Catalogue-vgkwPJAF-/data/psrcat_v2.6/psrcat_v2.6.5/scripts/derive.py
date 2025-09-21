import numpy as np
import sqlite3
import pandas as pd
import pygedm
import utils 
import math
from astropy.coordinates import SkyCoord 
from astropy import units as u

# Copyright: CSIRO 2024
# Author: Agastya Kapur: Agastya.Kapur@csiro.au, George Hobbs: George.Hobbs@csiro.au, Lawrence Toomey

# Inspired by psrcat1

#constants
c=299792458.0 #speed of light in m/s
psr_inertia=float(1.0e45)
mass=1.35 #solar masses
au=149597870 #1 au in kilometres
pc=30.857e12 #1pc in kilometres
mass_psr = 1.35 #Typical mass of pulsar in solar masses

#con = sqlite3.connect("psrcat2.db")
#cur = con.cursor()

#f0->p0
def f0_to_p0(f0,f0_err=None):
    f0 = float(f0)
    p0=float(1/f0)
    if f0_err is None:
        p0_errScale = None
    else:
        f0_err = float(f0_err)
        p0_err = float(f0_err*p0*p0)
        p0,p0_errScale = utils.getValueErr(str(p0),str(p0_err),1.0)
    return p0,p0_errScale

#f1->p1
def f1_to_p1(f0,f1,f1_err=None):
    f0 = float(f0)
    f1 = float(f1)
    p0,p0_err = f0_to_p0(f0)
    p1 = float(-1.0*p0*p0*f1)
    if f1_err is None:
        p1_errScale=None
    else:
        f1_err = float(f1_err)
        p1_err=float(((p0*p0*f1_err)**2+(2*p0*p0*f1*f1_err)**2)**(0.5))
        p1,p1_errScale = utils.getValueErr(str(p1),str(p1_err),1.0)
    return p1,p1_errScale
#f2->p2

#p0->f0
#f0->p0
def p0_to_f0(p0,p0_err=None):
    p0=float(p0)
    f0=float(1/p0)
    if p0_err is None:
        f0_errScale = None
    else:
        p0_err = float(p0_err)
        f0_err = float(p0_err*f0*f0)
        f0,f0_errScale = utils.getValueErr(str(f0),str(f0_err),1.0)
    return f0,f0_errScale

#p1->f1
def p1_to_f1(p0,p1,p1_err=None):
    p0 = float(p0)
    p1 = float(p1)
    f0,f0_err = p0_to_f0(p0)
    f1 = float(-1.0*f0*f0*p1)
    if p1_err is None:
        f1_errScale=None
    else:
        p1_err = float(p1_err)
        f1_err=float(((f0*f0*p1_err)**2+(2*f0*f0*p1*p1_err)**2)**(0.5))
        f1,f1_errScale = utils.getValueErr(str(f1),str(f1_err),1.0)
    return f1,f1_errScale


#Positions
#hh_mm_ss->degrees
def hhmmss_to_degrees(position):
    splitted=position.split(':')
    if(len(splitted)==3):
        hh=int(splitted[0])
        mm=int(splitted[1])
        ss=float(splitted[2])
    elif(len(splitted)==2):
        hh=int(splitted[0])
        mm=int(float(splitted[1]))
        ss=0.0
    elif(len(splitted)==1):
        hh=int(splitted[0])
        mm=0
        ss=0.0
    else:
        return None
    if(hh>=24 or hh<0 or mm>=60 or mm<0 or ss>=60 or ss<0):
        return None
    return str(15*hh+15*mm/60+15*ss/3600)

#dd_mm_ss->degrees
def ddmmss_to_degrees(position):
    splitted=position.split(':')
    if(len(splitted)==3):
        ddstr = splitted[0]
        dd=int(splitted[0])
        mm=int(splitted[1])
        ss=float(splitted[2])
    elif(len(splitted)==2):
        ddstr = splitted[0]
        dd=int(splitted[0])
        mm=int(float(splitted[1]))
        ss=0
    elif(len(splitted)==1):
        ddstr = splitted[0]
        dd=int(splitted[0])
        mm=0
        ss=0
    else:
        return None
    if(dd>90 or dd<-90 or mm>=60 or mm<0 or ss>=60 or ss<0):
        return None
    
    if('-' in ddstr):
        return str(dd-mm/60-ss/3600)
    else:
        return str(dd+mm/60+ss/3600)

#EQUATORIAL DEGREES->STRING
def degrees_to_string(raj,decj):
    coord = SkyCoord(ra=float(raj)*u.degree,dec=float(decj)*u.degree,frame="icrs")
    return coord.to_string('hmsdms',sep=':')

#EQUATORIAL->ECLIPTIC
def equatorial_to_ecliptic(raj,decj):
    coord = SkyCoord(ra=float(raj)*u.degree,dec=float(decj)*u.degree,frame="icrs")
    eclipticcoord = coord.transform_to("geocentricmeanecliptic")
    return eclipticcoord

#ECLIPTIC->EQUATORIAL
def ecliptic_to_equatorial(elong,elat):
    coord = SkyCoord(lon=float(elong)*u.degree,lat=float(elat)*u.degree,frame="geocentricmeanecliptic")
    equatorialcoord=coord.transform_to("icrs")
    return equatorialcoord

#EQUATORIAL->GALACTIC
def equatorial_to_galactic(raj,decj):
    coord = SkyCoord(ra=float(raj)*u.degree,dec=float(decj)*u.degree,frame="icrs")
    galacticcoord = coord.transform_to("galactic")
    return galacticcoord

#ECLIPTIC->GALACTIC
def ecliptic_to_galactic(elong,elat):
    coord = SkyCoord(lon=float(elong)*u.degree,lat=float(elat)*u.degree,frame="geocentricmeanecliptic")
    galacticcoord = coord.transform_to("galactic")
    return galacticcoord


#GALACTIC->EQUATORIAL
def galactic_to_equatorial(gl,gb):
    coord = SkyCoord(l=float(gl)*u.degree,b=float(gb)*u.degree,frame="galactic")
    equatorialcoord = coord.transform_to("icrs")
    return equatorialcoord.to_string('hmsdms',sep=':')


#PMEQUATORIAL->PMECLIPTIC


#PMECLIPTIC->PMEQUATORIAL

#PMEQUATORIAL->PMGALACTIC
def pm_equatorial_to_galactic(raj,decj,pmra,pmdec):
    return None

#Distances

#Dist->PX
def dist_to_parallax(dist):
    parallax = 1.0/dist
    return parallax

#PX->Dist
def parallax_to_dist(parallax):
    dist = 1.0/parallax
    return dist

#Dist->XX
#Dist->YY
#Dist->ZZ

#DM->Dist_YMW16

def dm_to_dist(ra,dec,dm,method,mode='gal'):
    DM,tau_sc = pygedm.dm_to_dist(ra,dec,dm,method=method,mode=mode)
    return DM,tau_sc

#Derived

#R_Lum / R_Lum14
def r_lum(flux,dist,freq=None): #Observing system_id ?? Freq ?? Use separate hard coded functions ??
    if(freq==None):
        freq=1400
    r_lum=flux*(dist**2)
    return "%.2e" % r_lum

#AGE : Spin down age (years)
def derive_age(p0,p1):
    if(p1<=0):
        return None
    age=p0/2.0/p1/60.0/60.0/24.0/365.25
    return "%.2e" % age

#BSurf : Surface magnetic flux density (Gauss_
def derive_bsurf(p0,p1):
    if(p1<=0):
        return None
    bsurf=3.2e19*(p0*p1)**0.5
    return "%.2e" % bsurf

#B_LC : Surface magnetic field at light cylinder
def derive_b_lc(p0,p1):
    if(p1<=0):
        return None
    b_lc=c*(p1**0.5)*p0**(-5.0/2.0)
    return "%.2e" % b_lc

#Edot : Spin down energy loss rate (ergs/s)
def derive_edot(p0,p1):
    if(p1<=0):
        return None
    edot=4.0*(math.pi**2)*psr_inertia*p1/(p0**3)
    return "%.2e" % edot

#Edotd2 : Energy flux at the sun (ergs/kpc^2/s)
def derive_edotd2(p0,p1,dist):
    if(p1<=0):
        return None
    edotd2=4.0*(math.pi**2)*psr_inertia*p1/(p0**3)/(dist**2)
    return "%.2e" % edotd2

#PMTot : Total proper motion (mas/year)
def pmtot(pmra,pmdec,pmra_err=None,pmdec_err=None):
    if pmra_err is None or pmdec_err is None:
        pmtot_err = None
    else:
        pmtot_err=((pmra*pmra_err)**2+(pmdec*pmdec_err)**2)**0.5/(pmra**2+pmdec**2)
    pmtot=(pmra**2+pmdec**2)**0.5
    return pmtot,pmtot_err

#VTrans : Transverse velocity - based on DIST (km/s)
def vtrans(pmtot,dist):
    vtrans = pmtot/1000.0/3600.0/180.0*math.pi/365.25/86400.0*3.086e16*dist #
    return "%-6.2f" % vtrans

#P1_i : Period derivative corrected for Shklovskii (proper motion) effect
def p1_I(p0,p1,dist,vtrans):
    if(p1<=0):
        return None
    p1_1=(p1/1.0e-15-(vtrans**2)*1.0e10*p0/(dist*3.086e6)/2.9979e10)*1.0e-15
    return "%.2e" % p1_1

#Age_i : Spin down age from P1_i (yr)
def age_i(p0,p1_i):
    age_i=(p0/2.0/p1_i)/60.0/60.0/24.0/365.25 
    return "%.2e" % age_i

#Bsurf_i
def bsurf_i(p0,p1_i):
    bsurf_i=3.2e19*(p0*p1_i)**0.5
    return "%.2e" % bsurf_i

#Edot_i
def edot_i(p0,p1_i):
    edot_i=4.0/1.0e-15*(math.pi**2)*p1_i/p0**3*1.0e30
    return "%.2e" % edot_i


def pm_l():
    return None

def pm_b():
    return None

# Relativity mass stuff

# Have M2,MASSQ,OM,ECC,A1,PB

def massfn(a1,pb):
    day2sec = 86400.0
    gm = float(1.3271243999e26)
    pb_sec = pb*day2sec # PB convert from days to seconds
    asini = a1*c*100 # A1 * Speed of light in cm/s
    massfn = ((2*math.pi)**2)*(asini)**3/gm/(pb_sec)**2
    massfn = "%10.6f" % massfn
    
    minmass = m2(massfn,1.0,mass_psr)
    minmass = "%10.6f" % minmass

    medmass = m2(massfn,0.866025403,mass_psr)
    medmass = "%10.6f" % medmass

    uprmass = m2(massfn,0.438371146,mass_psr)
    uprmass = "%10.6f" % uprmass

    return massfn,
    
def m2(mf,si,m1):
    # mf = mass function
    # m1 = mrimary mass
    # si = sin(i) (i = inclination angle)

    # solves: (m1+m2)^2 = (m2*si)^3 / mf

    dx = 0.0
    eq = 0.0
    deq_dm2 = 0.0
    guess = m1
    for gi in range(0,10000):
        eq = (m1+guess)**2 - (sini*guess)**3 / mf
        deq_dm2 = 2.0*(m1+guess) - 3.0*(sini*guess)**2 / mf

        dx = eq/deq_dm2
        guess -= dx

        if(abs(dx) <= abs(guess)*1e-10):
            return guess
    # If maximum iterations exceeded
    return -1.0
