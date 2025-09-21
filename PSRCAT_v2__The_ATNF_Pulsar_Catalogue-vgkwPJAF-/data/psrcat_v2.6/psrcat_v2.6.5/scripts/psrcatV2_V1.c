
// Software to produce PSRCAT v1 catalogue file from PSRCAT V2 database

// Copyright: CSIRO 2024
// Author: George Hobbs: George.Hobbs@csiro.au, Agastya Kapur: Agastya.Kapur@csiro.au

// Compile with:
// /usr/bin/gcc -lm -o psrcatV2_V1 psrcatV2_V1.c sqliteRoutines.c -I/pulsar/psr/software/stable/src/util/anaconda/include -L/pulsar/psr/software/stable/src/util/anaconda/lib -lsqlite3 -lm

#include <stdio.h>
#include <string.h>
#include <math.h>
#include <stdlib.h>
#include "sqlite3.h"
#include <ctype.h>
#include <string.h>
#include <time.h>
#include "sqliteRoutines.h"

#define MAX_PSR         8000
#define MAX_SURVEY_LIST 32
#define MAX_TYPE_LIST   32
#define MAX_PARAMS_PSR  300 // Maximum number of measured parameters per pulsar
#define MAX_REF         64
#define MAX_STRLEN      1024

// If this updates then update 'replaceMeasurement' function
typedef struct measurementStruct {
  int parameterTypeID;
  int valueID;
  char label[64];
  char value[1024];
  int timeDeriv;
  char error[1024];
  char ref[1024];
  int    binaryNum; // Companion number
  double cfreq; // Centre frequency
  double measurementEpoch;  
  char   measurementEpochStr[128];
  double widthHeight;
} measurementStruct;
void replaceMeasurement(measurementStruct *m,int from,int to);
int dropMeasurement(measurementStruct *m,int drop,int nrow);

void convertLabelToV1(measurementStruct *m);

int loadMeasurements(int pulsarID,measurementStruct *measurement,sqlite3 *db,int selectDuplicate);
int nint_derived(double x);
int rnd8(double rval,double rerr,int ifac,char *cval,int *lv,char *cerr,int *le,char *msg);
void processRAJ(measurementStruct *measurement);
void processDECJ(measurementStruct *measurement);
double dms_turn(char *line);
double hms_turn(char *line);
int turn_hms(double turn, char *hms);
int turn_dms(double turn, char *dms);
double turn_deg(double turn);

int main(int argc,char *argv[])
{
  char version[1024]="2.0.1";
  char dbVer[1024]="psrcat2.0.1.db";
  int pulsarID[MAX_PSR];
  char cmd[1024];
  char bname[1024],jname[1024];
  int npsr;
  int rc;
  char *errMsg=0;
  sqlite3 *db;
  int i,p,j,k;
  int primary;
  char ref[1024];
  char **surveyList;
  char *typeList;
  int *typeID;
  int *sTypeID;
  int *parameterID;
  int refID[MAX_REF];
  char *assocList;
  int *assocID;
  int assoc_refID[MAX_REF];
  char assocType[64];
  char assocTypeAll[64];
  int nAssoc;
  int nRef;
  int nSurvey=0;
  int nType=0;
  int nSType=0;
  int nDistance=0;

  int selectDuplicate=0; // 0 = most recent, 1 = smallest error
  measurementStruct *measurement;
  int nMeasurement=0;
  int ierr;
  double rval,rerr;
  int lv,le;
  char cval[128],cerr[128],msg[128];
  char discoveryStr[1024];
  char epochCite[1024];
  int gotFreqs=0;
  double pepoch=0;
  char pepochStr[128];
  int confidence1=1;
  int confidence2=1;
  int confidence3=1;

  int firstBinary=1;
  int firstEphem=1;
  int firstClock=1;
  int firstUnit=1;
  int alreadyFoundGlitch=0;
  int noOutput=0;

  int haveDM1=0;

  if (argc == 4)
    {
      strcpy(version,argv[1]);
      strcpy(dbVer,argv[2]);
      sscanf(argv[3],"%d",&selectDuplicate);
    }
  else
    {
      printf("Run using: psrcatV2_V1 verNum dbFile selectType\n");
      printf("where selectType = 0 for more recently added into the database\n");
      printf("                 = 1 for most precise\n");
      exit(1);
    }
  // Allocate memory
  surveyList = (char **)malloc(sizeof(char *)*MAX_SURVEY_LIST);
  for (i=0;i<MAX_SURVEY_LIST;i++)
    surveyList[i] = (char *)malloc(sizeof(char)*64);
  typeList = (char *)malloc(sizeof(char)*64);
  assocList = (char *)malloc(sizeof(char)*64);
  typeID = (int *)malloc(sizeof(int)*64);
  sTypeID = (int *)malloc(sizeof(int)*64);
  parameterID = (int *)malloc(sizeof(int)*64);
  assocID = (int *)malloc(sizeof(int)*64);
  measurement = (measurementStruct *)malloc(sizeof(measurementStruct)*MAX_PARAMS_PSR);
  
  // Open the PSRCAT database
  rc = sqlite3_open(dbVer,&db);
  if (rc!=SQLITE_OK)
    {
      printf("Unable to open database file: %s\n",dbVer);
      sqlite3_free(errMsg);
      exit(1);
    }

  // Get a list of the pulsarIDs
  //  sprintf(cmd,"SELECT pulsar_id FROM pulsar ORDER BY pulsar_id;");
  sprintf(cmd,"SELECT pulsar_id FROM pulsar ORDER BY jname;");
  npsr = runSQL_returnIntArray(cmd,db,pulsarID);
  //  for (p=0;p<npsr;p++)
  //    printf("PULSAR: %d %d\n",p,pulsarID[p]);
  printf("#CATALOGUE %s\n",version);
  printf("#\n");
  printf("# Catalogue produced using psrcatV2_V1 from the version 2 database\n");
  printf("# Number of pulsars is %d\n",npsr);
  printf("#\n");
  for (p=0;p<npsr;p++)
  //  TEMP for testing specific pulsarID
  //    p=1692;
  //
  {
    alreadyFoundGlitch=0;
    firstBinary=1;
    firstEphem=1;
    firstClock=1;
    firstUnit=1;
    
    // Get PSRB and PSRJ names for the pulsar
      strcpy(bname,"");
      strcpy(jname,"");

      // Select the JNAME FIRST
      sprintf(cmd,"SELECT Jname FROM pulsar WHERE pulsar_id=%d",pulsarID[p]);
      runSQL_returnStr(cmd,db,jname);

      // Now see if a BNAME exists
      sprintf(cmd,"SELECT name FROM name WHERE pulsar_id=%d AND name LIKE 'B\%%'",pulsarID[p]);
      runSQL_returnStr(cmd,db,bname);
      //      printf("Have bname = %s (%s)\n",bname,cmd);
      
      // Get the relevant reference for the name
      sprintf(cmd,"SELECT citation.v1label FROM citation LEFT JOIN pulsar ON pulsar.citation_id=citation.citation_id WHERE pulsar_id=%d",pulsarID[p]);
      runSQL_returnStr(cmd,db,ref);
    
      if (strlen(bname)>1)
	{
	  printf("%-8s %-29s %s\n","PSRB",bname,ref);
	  printf("%-8s %s \n","PSRJ",jname);
	}
      else
	printf("%-8s %-29s %s \n","PSRJ",jname,ref);

      // Get all the parameters for this pulsar
      nMeasurement = loadMeasurements(pulsarID[p],measurement,db,selectDuplicate);
      //      printf("nMeasurement = %d\n",nMeasurement);
      gotFreqs=0;
      pepoch=0;
      strcpy(pepochStr,"NULL");
      for (i=0;i<nMeasurement;i++)
	{
	  convertLabelToV1(&measurement[i]);
	  if (strcmp(measurement[i].label,"DM1")==0)
	    {haveDM1=1;}
	}
      for (i=0;i<nMeasurement;i++)
	{
	  noOutput=0;

	  // Is this a binary?
	  if (firstBinary==1)
	    {
	    int binaryModelID;
	    char binaryModel[128];
	    int ret;
	    
	    sprintf(cmd,"SELECT modelLabel FROM binaryModel LEFT JOIN binaryValue ON binaryModel.binaryModelID=binaryValue.binaryModelID LEFT JOIN measuredValue ON measuredValue.measuredValueID=binaryValue.measuredValueID LEFT JOIN value ON value.valueID=measuredValue.valueID WHERE pulsarID=%d;",pulsarID[p]);
	    ret = runSQL_returnStr(cmd,db,binaryModel);
	    //	    printf("Ret = %d %s %d\n",ret,binaryModel,measurement[i].valueID);
	    if (ret == 1)
	      printf("%-8s %s\n","BINARY",binaryModel);
	    firstBinary=0;
	  }

	  if (firstEphem==1) // This is deprecated. We are not using this one.
	    {
	      char ephem[1024];
	      int ret;
	      sprintf(cmd,"SELECT ephemOption.ephem FROM ephemOption LEFT JOIN measuredValue ON measuredValue.ephemID=ephemOption.ephemOptionID LEFT JOIN value ON value.valueID=measuredValue.valueID WHERE pulsarID=%d;",pulsarID[p]);
	      ret = runSQL_returnStr(cmd,db,ephem);
	      //	      printf("ephem ret = %d %d\n",ret,pulsarID[p]);
	      if (ret==1)
		printf("%-8s %s\n","EPHEM",ephem);
	      firstEphem=0;		
	    }

	  if (firstUnit==1)
	    {
	      char units[1024];
	      int ret;
	      sprintf(cmd,"SELECT measurementUnit.unitStr FROM measurementUnit LEFT JOIN measuredValue ON measuredValue.measurementUnitID=measurementUnit.measurementUnitID LEFT JOIN value ON value.valueID=measuredValue.valueID WHERE pulsarID=%d;",pulsarID[p]);
	      ret = runSQL_returnStr(cmd,db,units);
	      if (ret==1)
		printf("%-8s %s\n","UNITS",units);
	      firstUnit=0;		
	    }

	  if (firstClock==1)
	    {
	      char clock[1024];
	      int ret;
	      sprintf(cmd,"SELECT clockOption.clock FROM clockOption LEFT JOIN measuredValue ON measuredValue.clock=clockOption.clockOptionID LEFT JOIN value ON value.valueID=measuredValue.valueID WHERE pulsarID=%d;",pulsarID[p]);
	      ret = runSQL_returnStr(cmd,db,clock);
	      //	      printf("ephem ret = %d %d\n",ret,pulsarID[p]);
	      if (ret==1)
		printf("%-8s %s\n","CLK",clock);
	      firstClock=0;		
	    }
	  // Check for RAJ
	  if (strcmp(measurement[i].label,"RAJ")==0)
	    processRAJ(&measurement[i]);
	  else if (strcmp(measurement[i].label,"DECJ")==0)
	    processDECJ(&measurement[i]);
	  else if (strcmp(measurement[i].label,"GLEP")==0 && alreadyFoundGlitch==0) // Check for glitch parameters
	    {
	      int nGlitch=0;
	      sprintf(cmd,"SELECT COUNT(value) FROM parameter,parameterType,pulsar WHERE parameter.parameterType_id=parameterType.parameterType_id AND parameter.pulsar_id=pulsar.pulsar_id AND pulsar.pulsar_id=%d AND label LIKE 'GLEP'",pulsarID[p]);
	      nGlitch = runSQL_returnInt(cmd,db);
	      printf("%s %d","NGLT",nGlitch);
	      alreadyFoundGlitch=1;
	    }
	  else if (strcmp(measurement[i].label,"GLEP")==0 || strcmp(measurement[i].label,"GLDF0_F0")==0 ||
		   strcmp(measurement[i].label,"GLDF1_F1")==0 || strcmp(measurement[i].label,"GLQ")==0 ||
		   strcmp(measurement[i].label,"GLTD")==0)  // Do not output glitch parameters
	    // Do nothing
	    {noOutput=1;}
	  else if (strcmp(measurement[i].error,"NULL")!=0)
	    {
	      char fmt[1024];
	      sscanf(measurement[i].value,"%lf",&rval);
	      sscanf(measurement[i].error,"%lf",&rerr);
	      if (fabs(rerr) < 1e-90)
		{
		  printf("# WARNING: HAVE 0 ERROR\n");
		  sprintf(cval,"%.5f",rval);
		  strcpy(cerr,"0");
		}
	      else
		rnd8(rval,rerr,1,cval,&lv,cerr,&le,msg);
	      printf("%-9s",measurement[i].label);
	      //	      printf("lv %d\n",lv);
	      if (lv < 25)
		sprintf(fmt,"\%%-25s");
	      else
		sprintf(fmt,"\%%-%ds",lv+3);
	      //	      printf("fmt = %s\n",fmt);
	      printf(fmt,cval);
	      printf("%-5s",cerr);
	      printf("%-5s",measurement[i].ref);
	    }
	  else
	    {
	      printf("%-9s",measurement[i].label);
	      printf("%-25s",measurement[i].value);
	      printf("%-5s","");
	      printf("%-5s",measurement[i].ref);
	    }
	  if (noOutput==0) printf("\n");

	  if (strcmp(measurement[i].label,"F0")==0 || strcmp(measurement[i].label,"P0")==0)
	    {
	      gotFreqs=1;
	      //	      pepoch = measurement[i].measurementEpoch;
	      strcpy(pepochStr,measurement[i].measurementEpochStr);
	      pepoch = 1;
	      strcpy(epochCite,measurement[i].ref);
	      gotFreqs=0;
	      if (pepoch >= 0 && strcmp(pepochStr,"NULL")!=0)
		printf("%-8s %-29s %-5s\n","PEPOCH",pepochStr,epochCite);

	    }	  
	  else if ((strcmp(measurement[i].label,"F1")!=0 && strcmp(measurement[i].label,"P1")!=0
		   && strcmp(measurement[i].label,"F2")!=0 && strcmp(measurement[i].label,"F3")!=0  &&
		    strcmp(measurement[i].label,"F4")!=0 && gotFreqs==1) || (gotFreqs==1 && i==nMeasurement-1) )
	    {
	      gotFreqs=0;
	      if (pepoch >= 0 && strcmp(pepochStr,"NULL")!=0)
		printf("%-8s %-29s %-5s\n","PEPOCH",pepochStr,epochCite);
		//		printf("%-8s %-29g %-5s\n","PEPOCH",pepoch,epochCite);
	    }
	  
	  // Look for POSEPOCHS
	  if (strcmp(measurement[i].label,"DECJ")==0 || strcmp(measurement[i].label,"ELAT")==0)
	    {
	      if (strcmp(measurement[i].measurementEpochStr,"NULL")!=0)
		printf("%-8s %-29s %-5s\n","POSEPOCH",measurement[i].measurementEpochStr,measurement[i].ref);
	    }
	  // Look for DMEPOCHS
	  if (strcmp(measurement[i].label,"DM")==0) // || strcmp(measurement[i].label,"DM1")==0)
	    {
	      if (strcmp(measurement[i].measurementEpochStr,"NULL") != 0)
		printf("%-8s %-29s %-5s\n","DMEPOCH",measurement[i].measurementEpochStr,measurement[i].ref);
	    }
	}



      // Distances
      
      sprintf(cmd,"SELECT DISTINCT parameter_id FROM viewParameter WHERE (pulsar_id=%d AND label in ('DIST_AMN','DIST_A','DIST_AMX','DIST_YMW16','DIST_NE2001'));",pulsarID[p]);
      nDistance = runSQL_returnIntArray(cmd,db,parameterID);
      if (nDistance > 0)
	{
	  char distanceLabel[1024];
	  char distanceValue[1024];
	  char errorValue[1024];
	  char citationLabel[1024];
	  for (i=0;i<nDistance;i++)
	    {
	      sprintf(cmd,"SELECT DISTINCT label FROM viewParameter WHERE parameter_id=%d",parameterID[i]);
	      runSQL_returnStr(cmd,db,distanceLabel);
	      sprintf(cmd,"SELECT DISTINCT value FROM viewParameter WHERE parameter_id=%d",parameterID[i]);
	      runSQL_returnStr(cmd,db,distanceValue);

	      sprintf(cmd,"SELECT DISTINCT ifnull(uncertainty,'NULL') FROM viewParameter WHERE parameter_id=%d",parameterID[i]);

	      runSQL_returnStr(cmd,db,errorValue);

	      sprintf(cmd,"SELECT DISTINCT v1label FROM citation LEFT JOIN viewParameter ON viewParameter.[parameter.citation_id]=citation.citation_id WHERE parameter_id=%d",parameterID[i]);
	      runSQL_returnStr(cmd,db,citationLabel);
	      if (strcmp(distanceLabel,"DIST_YMW16")==0) strcpy(distanceLabel,"DIST_DM");
	      else if (strcmp(distanceLabel,"DIST_NE2001")==0) strcpy(distanceLabel,"DIST_DM1");

	      if (strcmp(errorValue,"NULL")==0)
		printf("%-8s %-29s %s\n",distanceLabel,distanceValue,citationLabel);
	      else
		{
		  double rval,rerr;
		  sscanf(distanceValue,"%lf",&rval);
		  sscanf(errorValue,"%lf",&rerr);
		  if (fabs(rerr) < 1e-90)
		    {
		      printf("# WARNING: HAVE 0 ERROR\n");
		      sprintf(cval,"%.5f",rval);
		      strcpy(cerr,"0");
		    }
		  else
		    rnd8(rval,rerr,1,cval,&lv,cerr,&le,msg);
		  printf("%-9s",distanceLabel);
		  printf("%-25s",cval);
		  printf("%-5s",cerr);

		  printf("%s\n",citationLabel);
		}
		}
	}
      
      // Is this a collection of data with a CLK, EPHEM, UNIT?
      {
	int nCollection;
	int collectionID;
	sprintf(cmd,"SELECT COUNT(fitParameters_id) FROM parameter,parameterType WHERE parameter.parameterType_id=parameterType.parameterType_id AND pulsar_id=%d AND fitParameters_id IS NOT NULL",pulsarID[p]);
	//	printf("cmd = %s\n",cmd);
	nCollection = runSQL_returnInt(cmd,db);
	if (nCollection > 0)
	  {
	    char ephem[1024];
	    char unit[1024];
	    char clk[1024];
	    char collection_cite[1024];
	    //	    printf("GEORGE IN HERE\n");
	    sprintf(cmd,"SELECT fitParameters_id FROM parameter,parameterType WHERE parameter.parameterType_id=parameterType.parameterType_id AND pulsar_id=%d AND fitParameters_id IS NOT NULL ORDER BY fitParameters_id DESC LIMIT 1",pulsarID[p]);
	    collectionID = runSQL_returnInt(cmd,db);
	    //	    	    printf("cmd = %s\n",cmd);
	    //	    	    printf("result= %d\n",collectionID);
	   sprintf(cmd,"SELECT v1label FROM citation,fitParameters WHERE fitParameters_id=%d AND citation.citation_id=fitParameters.citation_id",collectionID);	   
	   runSQL_returnStr(cmd,db,collection_cite);
	   //	   printf("Got collection_cite = %s, %d\n",collection_cite,collectionID);
	   sprintf(cmd,"SELECT ephemeris FROM fitParameters WHERE fitParameters_id=%d",collectionID);	   
	   //	   printf("cmd = %s\n",cmd);
	   runSQL_returnStr(cmd,db,ephem);
	   if (strcmp(ephem,"unknown")!=0)
	     printf("EPHEM    %-30s%s\n",ephem,collection_cite);
	   sprintf(cmd,"SELECT clock FROM fitParameters WHERE fitParameters_id=%d",collectionID);	   
	   runSQL_returnStr(cmd,db,clk);
	   if (strcmp(clk,"unknown")!=0)
	     printf("CLK      %-30s%s\n",clk,collection_cite);

	   sprintf(cmd,"SELECT units FROM fitParameters WHERE fitParameters_id=%d",collectionID);	   
	   runSQL_returnStr(cmd,db,unit);
	   if (strcmp(unit,"unknown")!=0)
	     printf("UNITS    %-30s%s\n",unit,collection_cite);

	  }
      }

      // Types and surveys

      //      sprintf(cmd,"SELECT typeStr FROM sourceTypeSelect LEFT JOIN sourceType ON sourceType.sourceTypeSelectID=sourceTypeSelect.sourceTypeSelectID WHERE sourceType.pulsarID=%d;",pulsarID[p]);

      // Source type
      
      // AK adding to make sure that if there are 2 same TYPEs (especially RADIO), it is treated as only 1 radio
      sprintf(cmd,"SELECT DISTINCT sourceTypeOptions_id FROM sourceType  WHERE sourceType.pulsar_id=%d;",pulsarID[p]);
      nSType = runSQL_returnIntArray(cmd,db,sTypeID);
      //
      
      sprintf(cmd,"SELECT sourceType_id FROM sourceType  WHERE sourceType.pulsar_id=%d;",pulsarID[p]);
      //      printf("Running %s\n",cmd);
      nType = runSQL_returnIntArray(cmd,db,typeID);
      //      printf("nType = %d\n",nType);
      if (nSType>0)
	{
	  int isRadio=0;
	  int countIt=0;
	  if (nSType==1) // Don't print anything if only radio
	    {
	      sprintf(cmd,"SELECT DISTINCT label FROM sourceTypeOptions LEFT JOIN sourceType ON sourceType.sourceTypeOptions_id=sourceTypeOptions.sourceTypeOptions_id WHERE sourceType_id=%d",typeID[0]);
	      runSQL_returnStr(cmd,db,typeList);
	      if (strcmp(typeList,"RADIO")==0)
		isRadio=2; // Only radio!
	    }
	  if (isRadio==0)
	    printf("TYPE     ");
	  for (i=0;i<nType;i++)
	    {
	      //	      if (countIt>0 && i<nType-1) printf(",");
	      sprintf(cmd,"SELECT label FROM sourceTypeOptions LEFT JOIN sourceType ON sourceType.sourceTypeOptions_id=sourceTypeOptions.sourceTypeOptions_id WHERE sourceType_id=%d",typeID[i]);
	      //	      printf("cmd = %s\n",cmd);
	      runSQL_returnStr(cmd,db,typeList);
	      sprintf(cmd,"SELECT confidence FROM sourceType WHERE sourceType_id=%d",typeID[i]);
	      confidence1 = runSQL_returnInt(cmd,db);
	      //	      printf("CONFIDENCE = %d\n",confidence1);
//	      printf("Output = %s\n",typeList);
	      if (strcmp(typeList,"RADIO")==0)
		{
		  if (isRadio==0)
		    isRadio=1;
		}
	      else
		{
		  if (i>0) printf(",");
		  countIt++;
		  //	      printf("Returned >%s< %d\n",typeList,typeID[i]);
		  // Is there a reference?
		  sprintf(cmd,"SELECT COUNT(citation_id) FROM sourceType WHERE sourceType_id=%d",typeID[i]);
		  nRef = runSQL_returnInt(cmd,db);
		  //		  printf("nRef = %d\n",nRef);
		  if (nRef > 0)
		    {
		      sprintf(cmd,"SELECT citation_id FROM sourceType WHERE sourceType_id=%d",typeID[i]);
		      runSQL_returnIntArray(cmd,db,refID);
		      if (confidence1==2)
			printf("%s(?)[",typeList);
		      else
			printf("%s[",typeList);
		      for (j=0;j<nRef;j++)
			{		    
			  sprintf(cmd,"SELECT v1label FROM citation WHERE citation_id=%d\n",refID[j]);
			  runSQL_returnStr(cmd,db,ref);
		      //      sprintf(cmd,"SELECT paper.v1Ref FROM pulsarName LEFT JOIN paper ON paper.paperID=pulsarName.paperID WHERE pulsarID=%d AND primaryFlag=%d",pulsarID[p],primary);
			  printf("%s",ref);
			  if (j<nRef-1) printf(",");
			}
		      printf("]");
		    }
		  else
		    {
		      if (confidence1==2)
			printf("%s(?)",typeList);
		      else
			printf("%s",typeList);
		    }
		}
	    }
	  if (isRadio!=2)
	    printf("\n");
	}
  
      // Binary companion
       
      
      sprintf(cmd,"SELECT binaryType_id FROM binaryType WHERE pulsar_id=%d;",pulsarID[p]);
      //      printf("Running %s\n",cmd);
      nType = runSQL_returnIntArray(cmd,db,typeID);
      if (nType>0)
	{
	  printf("BINCOMP  ");
	  for (i=0;i<nType;i++)
	    {
	      if (i>0) printf(",");
	      sprintf(cmd,"SELECT label FROM binaryTypeOptions LEFT JOIN binaryType ON binaryType.binaryTypeOptions_id=binaryTypeOptions.binaryTypeOptions_id WHERE binaryType_id=%d",typeID[i]);

	      //	      printf("Running str >%s<\n",cmd);
	      runSQL_returnStr(cmd,db,typeList);

	      // Check the confidence
	      sprintf(cmd,"SELECT confidence FROM binaryType WHERE binaryType_id=%d",typeID[i]);
	      confidence1 = runSQL_returnInt(cmd,db);
	      
	      // Is there a reference?
	      sprintf(cmd,"SELECT citation_id FROM binaryType WHERE binaryType_id=%d",typeID[i]);
	      //	      printf("Running: %s\n",cmd);
	      nRef = runSQL_returnInt(cmd,db);
	      //	      printf("nRef = %d\n",nRef);
	      if (nRef > 0)
		{
		  if (confidence1==2)
		    printf("%s(?)[",typeList);
		  else
		    printf("%s[",typeList);
		  sprintf(cmd,"SELECT citation.v1label FROM citation WHERE citation_id=%d\n",nRef);
		  runSQL_returnStr(cmd,db,ref);
		  //      sprintf(cmd,"SELECT paper.v1Ref FROM pulsarName LEFT JOIN paper ON paper.paperID=pulsarName.paperID WHERE pulsarID=%d AND primaryFlag=%d",pulsarID[p],primary);
		  printf("%s",ref);
		  printf("]");
		}
	      else
		{
		  if (confidence1==2)
		    printf("%s(?)",typeList);
		  else
		    printf("%s",typeList);
		}
		}
	  printf("\n");
	}


      
      // char *assocList;
      //  int *assocID;
      //  int assoc_refID[MAX_REF];
      //  int nAssoc;
      //      printf("Here\n");
      //SELECT pulsarName.name,linkAST2AssocSource.assocSourceTypeID,assocSource2Pulsar.assocSource2PulsarID,assocSourceType.label,assocSourceName.name FROM assocSource2Pulsar LEFT JOIN linkAssocSource2ASP ON linkAssocSource2ASP.assocSource2PulsarID=assocSource2Pulsar.assocSource2PulsarID LEFT JOIN assocSourceName ON assocSourceName.assocSourceID=linkAssocSource2ASP.assocSourceID LEFT JOIN pulsarName ON pulsarName.pulsarID=assocSource2Pulsar.pulsarID LEFT JOIN linkAST2AssocSource ON linkAST2AssocSource.assocSourceID=assocSourceName.assocSourceID LEFT JOIN assocSourceType ON assocSourceType.assocSourceTypeID=linkAST2AssocSource.assocSourceTypeID WHERE pulsarName.name LIKE "J0205+6449";
      //assocSource2Pulsar.assocSource2PulsarID
      sprintf(cmd,"SELECT association_id FROM association WHERE pulsar_id=%d;",pulsarID[p]);
      //      printf("Running %s\n",cmd);
      nAssoc = runSQL_returnIntArray(cmd,db,assocID);
      //      printf("NASSOC = %d\n",nAssoc);
      if (nAssoc>0)
	{
	  int nRet;
	  int asc1;
	  int asc2[64];
	  int nAsc2;
	  char ascType[1024];
	  char name[1024];
	  char cite[1024];
	  
	  //	  printf("nAssoc = %d\n",nAssoc);
	  printf("ASSOC    ");
	  for (i=0;i<nAssoc;i++)
	    {
	      //	      printf("id = %d\n",assocID[i]);
	      if (i>0) printf(",");

	      // Get the type
	      sprintf(cmd,"SELECT label FROM associationType LEFT JOIN association ON association.associationType_id=associationType.associationType_id WHERE association_id=%d",assocID[i]);
	      //	      printf("Running %s\n",cmd);
	      nRet = runSQL_returnStr(cmd,db,ascType);
	      printf("%s:",ascType);
	      // Now get the name of the association
	      // NOTE: Now replacing any spaces with underscores
	      sprintf(cmd,"SELECT REPLACE(name,' ','_') FROM association WHERE association_id=%d",assocID[i]);     
	      nRet = runSQL_returnStr(cmd,db,name);
	      printf("%s",name);
	      // Get the citation
	      sprintf(cmd,"SELECT v1label FROM citation LEFT JOIN association ON association.citation_id=citation.citation_id WHERE association_id=%d",assocID[i]);     
	      //printf("Running %s\n",cmd);
	      strcpy(cite,"");
	      nRet = runSQL_returnStr(cmd,db,cite);
	      if (strlen(cite)>1)
		printf("[%s]",cite);

	    }
	  
	  printf("\n");
	}
      // First get the DISCOVERY survey
      
      sprintf(cmd,"SELECT shortLabel FROM survey LEFT JOIN pulsar ON pulsar.survey_id=survey.survey_id WHERE pulsar.pulsar_id=%d;",pulsarID[p]);
      //      printf("Running >%s<\n",cmd);
      runSQL_returnStr(cmd,db,discoveryStr);
      printf("SURVEY   ");
      printf("%s",discoveryStr);
      //            sprintf(cmd,"SELECT shortLabel FROM survey LEFT JOIN surveyToPulsar ON surveyToPulsar.survey_id=survey.survey_id LEFT JOIN pulsar ON pulsar.pulsar_id=surveyToPulsar.pulsar_id WHERE pulsar.pulsar_id=%d;",pulsarID[p]);
      sprintf(cmd,"SELECT shortLabel FROM survey LEFT JOIN surveyToPulsar ON surveyToPulsar.survey_id=survey.survey_id LEFT JOIN pulsar ON surveyToPulsar.pulsar_id=pulsar.pulsar_id WHERE pulsar.pulsar_id=%d AND pulsar.survey_id IS NOT survey.survey_id;",pulsarID[p]);
      // printf("cmd = %s\n",cmd);
      nSurvey = runSQL_returnStrArray(cmd,db,surveyList);

      //      printf("nSurvey = %d\n",nSurvey);
      if (nSurvey>0)
	{

	  for (i=0;i<nSurvey;i++)
	    {
		{
		  printf(",");
		  printf("%s",surveyList[i]);
		}
	    }
	}
      printf("\n");


      printf("@-----------------------------------------------------------------\n");
    }
  sqlite3_close(db);
  
  // Deallocate memory
  for (i=0;i<MAX_SURVEY_LIST;i++)
    free(surveyList[i]);
  free(surveyList);
  free(measurement);

  free(typeList);
  free(assocList);
  free(typeID);
  free(sTypeID);
  //free(parameterID);
  free(assocID);
}


void processRAJ(measurementStruct *measurement)
{
  int i;
  double rval,rerr;
  double trval;
  double turn;
  char hms[1024];
  int lv,le;
  char cval[128],cerr[128],msg[128];

  int hh, mm, isec;
  double sec;
  char tstr[1024];
  char fmt[1024];
  int  dispPos=3; // 3 = sec, 2 = min, 1 = hour
  int  plusMinus=0;
      int lastDP_err;

  
  //  printf("GEORGE IN HERE\n");
  //  sscanf(measurement->value,"%lf",&rval);
  if (strcmp(measurement->error,"NULL")==0)
    rerr=-1;
  else
    {
      int nColon=0;
      int nDP=0;
      int lastDP,lastColon,ndigits;
      sscanf(measurement->error,"%lf",&rerr);
      //      printf("Loaded in %f %f %s\n",rval,rerr,measurement->error);

      // Are we in hours, minutes or seconds?
      for (i=0;i<strlen(measurement->value);i++)
	{
	  if (measurement->value[i]==':') {nColon++; lastColon = i;}
	  if (measurement->value[i]=='.') {nDP++; lastDP = i;}
	}
      //      printf("nDp = %d nColon = %d\n",nDP,nColon);
      
      if (nDP > 1) {printf("RAJ: ERROR: cannot have more than one decimal place: %s\n",measurement->value); exit(1);}
      else if (nDP == 1 && nColon != 2) {printf("RAJ: ERROR: Cannot understand %s\n",measurement->value); exit(1);}
      else if (nDP == 1 && nColon == 2){
	ndigits = strlen(measurement->value)-lastDP-1;
	//	printf("number of digits = %d and %g\n",ndigits,rerr*24./360*60*60*pow(10,ndigits));
	lastDP_err = (int)(rerr*24./360*60*60*pow(10,ndigits)+0.5);
	//	printf("lastDP_Err = %d\n",lastDP_err);
	//	finalError = errVal*pow(10,-ndigits)/240.0; // 240 = /60.0/60.0*180/12.0;
      }
      else if (nDP == 0 && nColon == 2)
	lastDP_err = (int)(rerr*24./360*60*60+0.5);
      else if (nDP == 0 && nColon == 1)
	lastDP_err = (int)(rerr*24./360*60+0.5);
      else if (nDP == 0 && nColon == 0)
	lastDP_err = (int)(rerr*24./360+0.5);
      else
	{printf("Confused by RAJ: %s\n",measurement->value); exit(1);}

      


         

      /*
      turn = rval/360.;
      
      
      if (fabs(rval) < 1e-5 && fabs(rerr) < 1e-5)
	{
	  printf("WARNING: Something wrong in RAJ. All zeroes\n");
	  strcpy(hms,"00:00:00");
	  strcpy(cerr,"0");
	}
      else
	{
	  if      (rerr >= 360./24.0)         dispPos = 1;
	  else if (rerr >= 360./24.0/60.)     dispPos = 2;
	  else dispPos = 3; 
	  printf("dispPs = %d, rval = %g, rerr= %g\n",dispPos,rval,rerr);
	  
	  strcpy(hms,"");
	  hh = (int)(turn*24);
	  sprintf(tstr,"%02d",hh);
	  strcat(hms,tstr);
	  
	  if (dispPos > 1)
	    {
	      mm = (int)((turn*24.-hh)*60.);
	      sprintf(tstr,":%02d",mm);
	      strcat(hms,tstr);
	    }
	  if (dispPos==2)
	    {
	      rerr = rerr*24./360*60.;
	      printf("err = %g\n",rerr);
	      if (rerr > 2)
		{
		  trval = mm;
		  rnd8(trval,rerr,1,cval,&lv,cerr,&le,msg);
		}
	      else
		{
		  //	      printf("Doing seconds\n");
		  // Add in seconds
		  dispPos=3;
		  rerr = rerr/24.*360/60.;
		}
	    }
	  
	  if (dispPos == 3)
	    {
	      sec = ((turn*24.-hh)*60.-mm)*60.;
	      trval =  sec;
	      if (trval < 1e-6) trval = 0; 
	      rerr = rerr*24./360*60*60.;
	      //	        printf("Here %g %g\n",trval,rerr);
	      rnd8(trval,rerr,1,cval,&lv,cerr,&le,msg);
	      //	      printf("Have lv = %d le = %d (%s) (%s)\n",lv,le,cval,cerr);
	      strcat(hms,":");
	      if (trval < 10)
		strcat(hms,"0");
	      strcat(hms,cval);
	    }
    }       */

    }
  // Need to write this myself to get the correct number of digits etc.
  //turn_hms(turn,hms);
  printf("%-9s",measurement->label,cval);
  printf("%-25s",measurement->value);
  if (rerr > 0)
    printf("%d    ",lastDP_err);
  else
    printf("%-5s","");
  printf("%-5s",measurement->ref);

}

void processDECJ(measurementStruct *measurement)
{
  int i;
  double rval,rerr;
  double trval;
  double turn;
  char dms[1024];
  int lv,le;
  char cval[128],cerr[128],msg[128];

  int hh, mm, isec;
  double sec;
  char tstr[1024];
  char fmt[1024];
  int  dispPos=3; // 3 = sec, 2 = min, 1 = hour
  int  plusMinus=0;
  int lastDP_err;

  
  //  printf("GEORGE IN HERE\n");
  //  sscanf(measurement->value,"%lf",&rval);
  if (strcmp(measurement->error,"NULL")==0)
    rerr=-1;
  else
    {
      int nColon=0;
      int nDP=0;
      int lastDP,lastColon,ndigits;
      sscanf(measurement->error,"%lf",&rerr);
      //      printf("Loaded in %f %f %s\n",rval,rerr,measurement->error);

      // Are we in hours, minutes or seconds?
      for (i=0;i<strlen(measurement->value);i++)
	{
	  if (measurement->value[i]==':') {nColon++; lastColon = i;}
	  if (measurement->value[i]=='.') {nDP++; lastDP = i;}
	}
      //      printf("nDp = %d nColon = %d\n",nDP,nColon);
      
      if (nDP > 1) {printf("DECJ: ERROR: cannot have more than one decimal place: %s\n",measurement->value); exit(1);}
      else if (nDP == 1 && nColon != 2) {printf("DECJ: ERROR: Cannot understand %s\n",measurement->value); exit(1);}
      else if (nDP == 1 && nColon == 2){
	ndigits = strlen(measurement->value)-lastDP-1;
	//	printf("number of digits = %d and %g\n",ndigits,rerr*24./360*60*60*pow(10,ndigits));
	lastDP_err = (int)(rerr*60*60*pow(10,ndigits)+0.5);
	//	printf("lastDP_Err = %d\n",lastDP_err);
	//	finalError = errVal*pow(10,-ndigits)/240.0; // 240 = /60.0/60.0*180/12.0;
      }
      else if (nDP == 0 && nColon == 2)
	lastDP_err = (int)(rerr*60*60+0.5);
      else if (nDP == 0 && nColon == 1)
	lastDP_err = (int)(rerr*60+0.5);
      else if (nDP == 0 && nColon == 0)
	lastDP_err = (int)(rerr+0.5);
      else
	{printf("Confused by DECJ: %s\n",measurement->value); exit(1);}

      


         


    }
  // Need to write this myself to get the correct number of digits etc.
  //turn_hms(turn,hms);
  printf("%-9s",measurement->label,cval);
  printf("%-25s",measurement->value);
  if (rerr > 0)
    printf("%d    ",lastDP_err);
  else
    printf("%-5s","");
  printf("%-5s",measurement->ref);

}


/*
void processDECJ(measurementStruct *measurement)
{
  double rval,rerr;
  double trval;
  double turn;
  char dms[1024];
  int lv,le;
  char cval[128],cerr[128],msg[128];

  int dd, mm, isec;
  double sec;
  char tstr[1024];
  char fmt[1024];
  int  dispPos=3; // 3 = sec, 2 = min, 1 = hour
  int  plusMinus=0;
  sscanf(measurement->value,"%lf",&rval);
  if (strcmp(measurement->error,"NULL")==0)
    rerr=-1;
  else
    {
      sscanf(measurement->error,"%lf",&rerr);
      
      
      //  printf("HAve rval = %g, rerr = %g\n",rval,rerr);
      if (fabs(rval) < 1e-5 && fabs(rerr) < 1e-5)
	{
	  printf("WARNING: Something wrong in DECJ. All zeroes\n");
	  strcpy(dms,"00:00:00");
	  strcpy(cerr,"0");
	}
      else
	{
	  if (rval < 0) {plusMinus=-1; rval*=-1;}
	  else plusMinus=1;
	  
	  turn = rval/360.;
	  
	  if      (rerr >= 1.)         dispPos = 1;
	  else if (rerr >= 1./60.)     dispPos = 2;
	  else  dispPos = 3; 
	  //      printf("dispPs = %d, rval = %g, rerr= %g\n",dispPos,rval,rerr);
	  
	  strcpy(dms,"");
	  dd = (int)(turn*360);
	  //      printf("dd = %d\n",dd);
	  if (plusMinus==1) sprintf(tstr,"+%02d",dd);
	  else sprintf(tstr,"-%02d",dd);
	  strcat(dms,tstr);
	  //      printf("here with >%s< >%s<\n",tstr,dms);
	  
	  if (dispPos > 1)
	    {
	      mm = (int)((turn*360.-dd)*60.+1e-5);
	      sprintf(tstr,":%02d",mm);
	      strcat(dms,tstr);
	      //	  printf("here2 with >%s< >%s< (%g %d %.5f %d)\n",tstr,dms,turn*360,dd,(turn*360.-dd)*60.,(int)((turn*360.-dd)*60));
	    }
	  if (dispPos==2)
	    {
	      //	  printf("Here with rerr = %g %g\n",rerr,rerr*60);
	      rerr = rerr*60.;
	      if ((int)(rerr+1e-5) >= 2) 
		{
		  rerr = (int)(rerr+1.0e-5);
		  trval = mm;
		  rnd8(trval,rerr,1,cval,&lv,cerr,&le,msg);
		  //	      printf("Here %g %s (%g le = %d msg = %s)\n",rerr,cerr,trval,le,msg);
		}
	      else
		{
		  // Add in seconds
		  dispPos=3;
		  rerr = rerr/60.;
		}
	    }
      
      
	  if (dispPos == 3)
	    {
	      sec = ((turn*360.-dd)*60.-mm)*60.;
	      trval =  sec;
	      if (trval < 1e-6) trval = 0; // For J2340+08
	      
	      rerr = rerr*60*60.;
	      //	  printf("Here %g %g (%g %g %g)\n",trval,rerr,turn*360,(float)dd,(float)mm);
	      rnd8(trval,rerr,1,cval,&lv,cerr,&le,msg);
	      //	  printf("Have lv = %d le = %d (%s) (%s)\n",lv,le,cval,cerr);
	      //	  printf("dms1 = %s\n",dms);
	      strcat(dms,":");
	      //	  printf("dms2 = %s\n",dms);
	      if (trval < 10)
		strcat(dms,"0");
	      //	  printf("dms3 = %s\n",dms);
	      strcat(dms,cval);
	      //	  printf("dms4 = %s\n",dms);
	    }
	}
    }
      // Need to write this myself to get the correct number of digits etc.
      //turn_hms(turn,hms);
      printf("%-9s",measurement->label,cval);
      strcpy(dms,measurement->value);
      printf("%-25s",dms);
  printf("%-5s",cerr);
  printf("%-5s",measurement->ref);
}
*/
int loadMeasurements(int pulsarID,measurementStruct *measurement,sqlite3 *db,int selectDuplicate)
{
  int rc;
  sqlite3_stmt *statement;
  char cmd[1024];
  int cols;
  int result=0;
  int row=0;
  int i,j,drop;
  
  //  sprintf(cmd,"SELECT value.valueID,v1label.label,value.value,value.timeDeriv,errorSym.error,paper.v1Ref,measuredValue.measurementTime FROM value LEFT JOIN v1Label on value.v1LabelID=v1Label.v1LabelID LEFT JOIN errorSym ON errorSym.valueID=value.valueID LEFT JOIN paper ON paper.paperID=value.paperID LEFT JOIN measuredValue ON measuredValue.valueID=value.valueID WHERE value.pulsarID=%d;",pulsarID);

  sprintf(cmd,"SELECT parameterType.parameterType_id,parameter.parameter_id,parameterType.label,parameter.value,timeDerivative,companionNumber,uncertainty,v1label,referenceTime,centralFrequency,ancillary.value FROM parameter LEFT JOIN pulsar ON parameter.pulsar_id=pulsar.pulsar_id LEFT JOIN parameterType ON parameterType.parameterType_id=parameter.parameterType_id LEFT JOIN citation ON citation.citation_id=parameter.citation_id LEFT JOIN observingSystem ON parameter.observingSystem_id=observingSystem.observingSystem_id LEFT JOIN ancillary ON ancillary.parameter_id=parameter.parameter_id WHERE (parameter.pulsar_id=%d AND parameterType.label NOT LIKE 'DIST\%%' AND parameter.parameter_id NOT IN (SELECT parameter_id FROM derived) AND NOT (parameter.parameterType_id IN (7,8) AND parameter.uncertainty IS NULL)) ORDER BY parameterType.parameterType_id;",pulsarID);
  
  //  printf("Running %s\n",cmd);

  //exit(1);
  if (sqlite3_prepare_v2(db,cmd,-1,&statement,NULL) == SQLITE_OK)
   
    {
      cols = sqlite3_column_count(statement);
      //printf("Cols = %d\n",cols);
      if (cols != 11)
	{
	  printf("Wrong number of columns in function call\n");
	  exit(1);
	}
      while (1==1)
	{
	  result = sqlite3_step(statement);
	  //	  printf("result = %d\n",result);

	  if (result == SQLITE_ROW)
	    {
	      int valueID;
	      valueID = sqlite3_column_int(statement,0);
	      measurement[row].parameterTypeID = valueID;
	      //	      printf("HAVE VALUEID %d\n",valueID);
	    }
	  else
	    break;

	  
	  if (result == SQLITE_ROW)
	    {
	      int valueID;
	      valueID = sqlite3_column_int(statement,1);
	      measurement[row].valueID = valueID;
	    }
	  
	  if (result == SQLITE_ROW)
	    {
	      strcpy(measurement[row].label,(char *)sqlite3_column_text(statement,2));
	      //	      printf("CheCKING: %s\n",measurement[row].label);
	    }
	  if (result == SQLITE_ROW)
	    strcpy(measurement[row].value,(char *)sqlite3_column_text(statement,3));
	  
	  if (result == SQLITE_ROW)
	    measurement[row].timeDeriv = sqlite3_column_int(statement,4);

	  if (result == SQLITE_ROW)
	    measurement[row].binaryNum = sqlite3_column_int(statement,5);

	  if (result == SQLITE_ROW && sqlite3_column_text(statement,6)!=NULL)
	    strcpy(measurement[row].error,(char *)sqlite3_column_text(statement,6));
	  else if (result == SQLITE_ROW && sqlite3_column_text(statement,6)==NULL)
	    strcpy(measurement[row].error,"NULL");

	  if (result == SQLITE_ROW && sqlite3_column_text(statement,7)!=NULL)
	    strcpy(measurement[row].ref,(char *)sqlite3_column_text(statement,7));

	  if (result == SQLITE_ROW && sqlite3_column_text(statement,8)!=NULL)
	    strcpy(measurement[row].measurementEpochStr,(char *)sqlite3_column_text(statement,8));
	  else if (result == SQLITE_ROW)
	    strcpy(measurement[row].measurementEpochStr,"NULL");
	    //	    measurement[row].measurementEpoch=-1;

	  if (result == SQLITE_ROW && sqlite3_column_text(statement,9)!=NULL)
	    measurement[row].cfreq = sqlite3_column_double(statement,9);
	  else if (result == SQLITE_ROW)
	    measurement[row].cfreq=-1;
	  
	  if (result == SQLITE_ROW && sqlite3_column_text(statement,10)!=NULL)
	    measurement[row].widthHeight = sqlite3_column_double(statement,10);
	  else if (result == SQLITE_ROW)
	    measurement[row].widthHeight=-1;
	  else
	    break;

	  // Should we have removed the previous value?
	  //	  printf("Checking %d %d %d\n",measurement[row].parameterTypeID,measurement[row].timeDeriv,measurement[row].binaryNum);
	  if (row > 0)
	    {
	      int k;
	      int mustFix=0;
	      for (k=0;k<row;k++)
		{
		  //		  printf("Checking %s\n",measurement[row].label);
		  if (strcmp(measurement[row].label,"FLUX")!=0 &&
		      strcmp(measurement[row].label,"WIDTH")!=0 &&
		      strcmp(measurement[row].label,"GLEP")!=0 &&
		      strcmp(measurement[row].label,"GLF0")!=0 &&
		      strcmp(measurement[row].label,"GLF1")!=0 &&
		      strcmp(measurement[row].label,"GLF0D")!=0 &&
		      strcmp(measurement[row].label,"GLTD")!=0 &&
		      strcmp(measurement[row].label,"GLQ")!=0 &&
		      strcmp(measurement[row].label,"GLDF0_F0")!=0 &&
		      strcmp(measurement[row].label,"GLDF1_F1")!=0)		      
		      {
			if ((measurement[row].parameterTypeID == measurement[k].parameterTypeID &&  // Check if two parameters are the same
			     measurement[row].timeDeriv == measurement[k].timeDeriv &&
			     measurement[row].binaryNum == measurement[k].binaryNum))
			{
			  mustFix=1;
			  printf("# %s_%d, nc = %d (%s versus %s).\n",measurement[row].label,measurement[row].timeDeriv,measurement[row].binaryNum,measurement[row].ref,measurement[k].ref);
			  if (selectDuplicate==0)
			    {
			      printf("# Chose most recently entered into database\n");
			      //		      replaceMeasurement(measunt,row,k);
			      replaceMeasurement(measurement,row,k);
			    }
			  else if (selectDuplicate==1)
			    {
			      float e1,e2;
			      if (strcmp(measurement[k].error,"NULL")==0 && strcmp(measurement[row].error,"NULL")==0)
				{
				  printf("# Errors are >%s< and >%s<. Taking original value\n",measurement[row].error,measurement[k].error);
				}
			      else
				{
				  printf("# Errors are >%s< [%s] and >%s< [%s]. Taking smallest uncertainty\n",measurement[row].error,measurement[row].ref,measurement[k].error,measurement[k].ref);
				  
				  if (strcmp(measurement[k].error,"NULL")==0 && strcmp(measurement[row].error,"NULL")!=0)
				    replaceMeasurement(measurement,row,k);
				  else if (strcmp(measurement[row].error,"NULL")==0 && strcmp(measurement[k].error,"NULL")!=0)
				    {// Don't change
				    }
				  else
				    {	    
				      sscanf(measurement[row].error,"%f",&e1);
				      sscanf(measurement[k].error,"%f",&e2);
				      //				      printf("IN HERE\n");
				      if (e1 < e2)
					replaceMeasurement(measurement,row,k);
				    }
				}
			    }
			  else
			    {
			      printf("Don't know how to choose between parameters. select = %d\n",selectDuplicate);
			    }
			}
		    }
		}
	      if (mustFix==1)
		row-=1;
	    }
	  
	  row++;
	}
      sqlite3_finalize(statement);
    }

  // Check for an F and a P
  for (i=0;i<row;i++)
    {
      for (j=i+1;j<row;j++)
	{
	  if ((strcmp(measurement[i].label,"F")==0 && strcmp(measurement[j].label,"P") ==0  &&  // Check if two parameters are the same
	       measurement[i].timeDeriv == measurement[j].timeDeriv &&
	       measurement[i].binaryNum == measurement[j].binaryNum) ||
	      (strcmp(measurement[i].label,"P")==0 && strcmp(measurement[j].label,"F") ==0  &&  // Check if two parameters are the same
	       measurement[i].timeDeriv == measurement[j].timeDeriv &&
	       measurement[i].binaryNum == measurement[j].binaryNum) )	      
	    {
	      if (strcmp(measurement[i].label,"P")==0)
		drop = i;
	      else
		drop = j;
	      row = dropMeasurement(measurement,drop,row);
	      printf("# Have both P and F. Choosing to keep F\n");
	    }
	}
    }
  
  return row;

}

void replaceMeasurement(measurementStruct *m,int from,int to)
{
  m[to].parameterTypeID = m[from].parameterTypeID;
  m[to].valueID = m[from].valueID;
  strcpy(m[to].label,m[from].label);
  strcpy(m[to].value,m[from].value);
  m[to].timeDeriv = m[from].timeDeriv;
  strcpy(m[to].error,m[from].error);
  strcpy(m[to].ref,m[from].ref);
  m[to].binaryNum = m[from].binaryNum;
  m[to].cfreq = m[from].cfreq;
  m[to].measurementEpoch = m[from].measurementEpoch;
  strcpy(m[to].measurementEpochStr,m[from].measurementEpochStr);
  m[to].widthHeight = m[from].widthHeight;
}

int dropMeasurement(measurementStruct *m,int drop,int row)
{
  int i;

  for (i=drop;i<row-1;i++)
    {
      m[i].parameterTypeID = m[i+1].parameterTypeID;
      m[i].valueID = m[i+1].valueID;
      strcpy(m[i].label,m[i+1].label);
      strcpy(m[i].value,m[i+1].value);
      m[i].timeDeriv = m[i+1].timeDeriv;
      strcpy(m[i].error,m[i+1].error);
      strcpy(m[i].ref,m[i+1].ref);
      m[i].binaryNum = m[i+1].binaryNum;
      m[i].cfreq = m[i+1].cfreq;
      m[i].measurementEpoch = m[i+1].measurementEpoch;
      strcpy(m[i].measurementEpochStr,m[i+1].measurementEpochStr);
      m[i].widthHeight = m[i+1].widthHeight;
    }
}


int rnd8(double rval,double rerr,int ifac,char *cval,int *lv,char *cerr,int *le,char *msg)
{
  double vv, ee, xv, xe;
  int ixv, ixe, iee, j, ivv, ise, irnd, ilim,ret,ret_lv,ret_le;
  char cexp[9], fmt[12];
  char temp[1024];

  if (rval == 0 && rerr < 1e-6)
    {
      char errStr[1024],firstDigit[1024],exponent[1024];
      char *checkDP;

      sprintf(errStr,"%g",rerr);
      //      printf("errStr = %s\n",errStr);
      
      // Take first digit
      strcpy(firstDigit,errStr); firstDigit[1] = '\0';
      strcpy(exponent,errStr+1); 
      checkDP = strstr(exponent,".");
      if (checkDP!=NULL)
	{
	  char *newExponent = strstr(exponent,"e")+1;
	  char newErr[1024];
	  int intErr;
	  float exp;
	  
	  strcpy(newErr,errStr);
	  strcpy(strstr(newErr,"e"),"\0");
	  sscanf(newExponent,"%f",&exp);
	  intErr = (int)(rerr*10.0/pow(10,exp)+0.5);
	  //	  printf("intErr = %d %g %g %g\n",intErr,rerr,exp,rerr*10.0/pow(10,exp));
	  //	  printf("newErr = %s\n",newErr);
	  //	  printf("CheckDP = %s >%s<\n",checkDP,newExponent);
	  sprintf(cerr,"%d",intErr);
	  sprintf(cval,"0.0e%s",newExponent);
	  //	  printf("errStr = %s\n",errStr);
	  //	  	  printf("VALUE HERE = 0 (%g %g) [%s] [%s] [%s] [%s %s]\n",rval,rerr,errStr,firstDigit,exponent,cval,cerr);
	  return 0;
	}
      
      sprintf(cval,"0%s",exponent);
      sprintf(cerr,firstDigit);
      //      printf("VALUE = 0 (%g %g) [%s] [%s] [%s] [%s %s]\n",rval,rerr,errStr,firstDigit,exponent,cval,cerr);
      *lv=strlen(cval)-1;
      return 0;
    }
  
  ilim = 4;

  strcpy(cval," ");
  strcpy(cerr," ");
  strcpy(msg," ");
  ret = 0;
  ret_lv = 0;
  ret_le = 0;

  /* Get scale factors */
  vv = fabs(rval);
  ee = rerr*ifac;
  if (vv>0.0)
    xv=log10(vv);
  else
    xv=0.0;

  ixv=fabs(xv);
  if (xv<0.0)ixv=-ixv-1;

  xe = log10(ee/2.0)+1.0e-10;
  ixe = fabs(xe);
  if (xe<0.0)ixe=-ixe-1; 
  ise=ixe-ixv; /* Scale of error wrt value */

  ixv = -ixv; /* Reverse signs of scale factors */
  ixe = -ixe;
  ise = -ise;

  /* Check for encoding as integer */
  if (xv>=log10(2.0) && xv<(ilim+10) && ee>=2.0)
    {
      irnd=xe;
      ivv=nint_derived(vv);
      if (irnd>1)
	{
	  irnd=nint_derived(pow(10.0,irnd));
	  ivv = nint_derived(vv/irnd)*irnd;
	}
      if (ivv!=0)
	ret_lv=log10((double)ivv+1.0e-10)+2.0;

      if (rval<0.0) ivv=-ivv;
      sprintf(fmt,"%%%dd",ret_lv);
      sprintf(cval,fmt,ivv);
    }
  else /* Encode as real */
    {
      vv=rval*pow(10.0,ixv); /* Scale for mantissa */
      ee=ee*pow(10.0,ixe);   /* scale error */

      if (ixv<-ilim || ixv>ilim) /* Use exponent notation */
	{
	  if (ise<1) /* If err > 0.2, print val as fn.1 and scale error */
	    {
	      ee=ee*pow(10.0,1-ise);
	      ise=1; 
	    }
	  strcpy(cexp," ");
	  sprintf(cexp,"%d",-ixv); 
	  j=0; /* Strip off leading blanks */
	  do
	    {
	      j=strlen(cexp)-1;
	      if (j==0){
		strcpy(temp,cexp+1);
		strcpy(cexp,temp);
	      }
	    }
	  while (j==0);
	  if (vv<0) /* allow space for - sign */
	    sprintf(fmt,"%%%d.%df",ise+4,ise);
	  else
	    sprintf(fmt,"%%%d.%df",ise+3,ise);
	  sprintf(cval,fmt,vv);
	  if (cval[0]==' ') 
	    {
	      strcpy(temp,cval+1);
	      strcpy(cval,temp);
	    }
	  ret_lv = strlen(cval)-1;
	  strcat(cval,"E");
	  strcat(cval,cexp);
	}
      else
	{
	  //	  printf("GEORGE 2 %d\n",ise);
	  if (ise<1)
	    {
	      if (ixv<1)
		{
		  ixv=1;
		  ise=ise-1;
		}
	      sprintf(fmt,"%%%d.%df",3+ixv,ixv);
	      ee=ee*pow(10.0,-ise);
	    }
	  else
	    {
	      //	      printf("GEORGE 3 ixv = %d ixe = %d\n",ixv,ixe);
	      if (ixv<0)ixv=0;
	      if (ixe<1)ixe=1;

	      sprintf(fmt,"%%%d.%df",3+ixv+ise,ixe);
	      //	      printf("Format = %s\n",fmt);
	    }
	  sprintf(cval,fmt,rval);
	  //	  printf("OUTPUT %s\n",cval);
	}
    }

  if (cval[0]==' ') 
    {
      strcpy(temp,cval+1);
      strcpy(cval,temp);  /* For positive numbers */
    }
  ret_lv=strlen(cval)-1; 

  //  printf("Here ee = %g\n",ee);
  irnd = log10(ee/2.0);
  if (irnd>1) /* Round error */
    {
      irnd=nint_derived(pow(10.0,irnd));
      iee=(int)(ee/irnd+0.999)*irnd;
    }
  else
    iee = ee+0.999;  /* Round error up */

  ee=iee;
  ret_le = log10(ee+0.999)+1.0;



  sprintf(fmt,"%%%dd",ret_le);
  sprintf(cerr,fmt,iee);
  //  printf("Here with %s %s %d\n",cerr,fmt,iee);

  *le = ret_le;
  *lv = ret_lv;


  return 0;
}
int nint_derived(double x){
  int i;
  if(x>0.){
    i=(int)(x+0.5);
  }
  else{
    i=(int)(x-0.5);
  }
  return(i);
}

double turn_deg(double turn){
 
  /* Converts double turn to string "sddd.ddd" */
  return turn*360.0;
}

int turn_dms(double turn, char *dms){
  
  /* Converts double turn to string "sddd:mm:ss.sss" */
  
  int dd, mm, isec;
  double trn, sec;
  char sign;
  
  sign=' ';
  if (turn < 0.){
    sign = '-';
    trn = -turn;
  }
  else{
    sign = '+';
    trn = turn;
  }
  dd = trn*360.;
  mm = (trn*360.-dd)*60.;
  sec = ((trn*360.-dd)*60.-mm)*60.;
  isec = (sec*1000. +0.5)/1000;
    if(isec==60){
      sec=0.;
      mm=mm+1;
      if(mm==60){
        mm=0;
        dd=dd+1;
      }
    }
  sprintf(dms,"%c%02d:%02d:%010.7f",sign,dd,mm,sec);

  return 0;
}


int turn_hms(double turn, char *hms){
 
  /* Converts double turn to string " hh:mm:ss.ssss" */
  
  int hh, mm, isec;
  double sec;

  hh = turn*24.;
  mm = (turn*24.-hh)*60.;
  sec = ((turn*24.-hh)*60.-mm)*60.;
  isec = (sec*10000. +0.5)/10000;
    if(isec==60){
      sec=0.;
      mm=mm+1;
      if(mm==60){
        mm=0;
        hh=hh+1;
        if(hh==24){
          hh=0;
        }
      }
    }

  sprintf(hms," %02d:%02d:%010.7f",hh,mm,sec);

  return 0;
}


double hms_turn(char *line){

  /* Converts string " hh:mm:ss.ss" or " hh mm ss.ss" to double turn */
  
  int i;int turn_hms(double turn, char *hms);
  double hr, min, sec, turn=0;
  char hold[MAX_STRLEN];

  strcpy(hold,line);

  /* Get rid of ":" */
  for(i=0; *(line+i) != '\0'; i++)if(*(line+i) == ':')*(line+i) = ' ';

  i = sscanf(line,"%lf %lf %lf", &hr, &min, &sec);
  if(i > 0){
    turn = hr/24.;
    if(i > 1)turn += min/1440.;
    if(i > 2)turn += sec/86400.;
  }
  if(i == 0 || i > 3)turn = 1.0;


  strcpy(line,hold);

  return turn;
}

double dms_turn(char *line){

  /* Converts string "-dd:mm:ss.ss" or " -dd mm ss.ss" to double turn */
  
  int i;
  char *ic, ln[40];
  double deg, min, sec, sign, turn=0;

  /* Copy line to internal string */
  strcpy(ln,line);

  /* Get rid of ":" */
  for(i=0; *(ln+i) != '\0'; i++)if(*(ln+i) == ':')*(ln+i) = ' ';

  /* Get sign */
  if((ic = strchr(ln,'-')) == NULL)
     sign = 1.;
  else {
     *ic = ' ';
     sign = -1.;
  }

  /* Get value */
  i = sscanf(ln,"%lf %lf %lf", &deg, &min, &sec);
  if(i > 0){
    turn = deg/360.;
    if(i > 1)turn += min/21600.;
    if(i > 2)turn += sec/1296000.;
    if(turn >= 1.0)turn = turn - 1.0;
    turn *= sign;
  }
  if(i == 0 || i > 3)turn =1.0;

  return turn;
}

void convertLabelToV1(measurementStruct *m)
{
  char tmp[1024];
  if (m->binaryNum < 2)
    {
      if (m->timeDeriv > 0)
	{
	  if (m->timeDeriv == 1 && strcmp(m->label,"PB")==0)
	    {strcpy(m->label,"PBDOT");}
	  else if (m->timeDeriv == 1 && strcmp(m->label,"OM")==0)
	    strcpy(m->label,"OMDOT");
	  else if (m->timeDeriv == 2 && strcmp(m->label,"OM")==0)
	    strcpy(m->label,"OM2DOT");
	  else if (m->timeDeriv == 1 && strcmp(m->label,"A1")==0)
	    strcpy(m->label,"A1DOT");
	  else if (m->timeDeriv == 2 && strcmp(m->label,"A1")==0)
	    strcpy(m->label,"A12DOT");
	  else if (m->timeDeriv == 1 && strcmp(m->label,"ECC")==0)
	    strcpy(m->label,"ECCDOT");
	  else
	    {
	      strcpy(tmp,m->label);
	      sprintf(m->label,"%s%d",tmp,m->timeDeriv);
	    }
	}
      else if (m->timeDeriv == 0 && strcmp(m->label,"F")==0)
	strcpy(m->label,"F0");
      else if (m->timeDeriv == 0 && strcmp(m->label,"P")==0)
	strcpy(m->label,"P0");
      else if (m->timeDeriv == 0 && strcmp(m->label,"FB")==0)
	strcpy(m->label,"FB0");
    }
  else
    {
      if (m->timeDeriv == 1 && strcmp(m->label,"PB")==0)
	{strcpy(m->label,"PBDOT");}

      if (m->timeDeriv == 0 && strcmp(m->label,"PB")==0)
	{
	  if (m->binaryNum==1) sprintf(m->label,"PB");
	  else sprintf(m->label,"PB_%d",m->binaryNum);
	}
      else if (strcmp(m->label,"PBDOT")==0 && m->binaryNum > 0)
	sprintf(m->label,"PBDOT_%d",m->binaryNum);
      else if (m->timeDeriv == 0 && strcmp(m->label,"A1")==0)
	{
	  if (m->binaryNum==1) sprintf(m->label,"A1");
	  else sprintf(m->label,"A1_%d",m->binaryNum);
	}
      else if (m->timeDeriv == 0 && strcmp(m->label,"ECC")==0)
	{
	  if (m->binaryNum==1) sprintf(m->label,"ECC");
	  else sprintf(m->label,"ECC_%d",m->binaryNum);
	}
      else if (m->timeDeriv == 0 && strcmp(m->label,"T0")==0)
	{
	  if (m->binaryNum==1) sprintf(m->label,"T0");
	  else sprintf(m->label,"T0_%d",m->binaryNum);
	}
      else if (m->timeDeriv == 0 && strcmp(m->label,"OM")==0)
	{
	  if (m->binaryNum==1) sprintf(m->label,"OM");
	  else sprintf(m->label,"OM_%d",m->binaryNum);
	}
      else if (m->timeDeriv == 0 && strcmp(m->label,"SINI")==0)
	{
	  if (m->binaryNum==1) sprintf(m->label,"SINI");
	  else sprintf(m->label,"SINI_%d",m->binaryNum);
	}
      else if (m->timeDeriv == 0 && strcmp(m->label,"M2")==0)
	{
	  if (m->binaryNum==1) sprintf(m->label,"M2");
	  else sprintf(m->label,"M2_%d",m->binaryNum);
	}
      else if (m->timeDeriv == 0 && strcmp(m->label,"EPS1")==0)
	{
	  if (m->binaryNum==1) sprintf(m->label,"EPS1");
	  else sprintf(m->label,"EPS1_%d",m->binaryNum);
	}
      else if (m->timeDeriv == 0 && strcmp(m->label,"EPS2")==0)
	{
	  if (m->binaryNum==1) sprintf(m->label,"EPS2");
	  else sprintf(m->label,"EPS2_%d",m->binaryNum);
	}
      else if (m->timeDeriv == 0 && strcmp(m->label,"TASC")==0)
	{
	  if (m->binaryNum==1) sprintf(m->label,"TASC");
	  else sprintf(m->label,"TASC_%d",m->binaryNum);
	}
    }

  
  if (strcmp(m->label,"FLUX")==0)
    {
      if (m->cfreq == 100000)
	sprintf(m->label,"S100G");
      else if (m->cfreq == 150000)
	sprintf(m->label,"S150G");
      else
	sprintf(m->label,"S%.0f",m->cfreq);

      if (strcmp(m->label,"S1250")==0)
	strcpy(m->label,"S1400");
      if (strcmp(m->label,"S1489")==0)
	strcpy(m->label,"S1400");


    }
      else if (strcmp(m->label,"WIDTH")==0)
    sprintf(m->label,"W%.0f",m->widthHeight);

}
