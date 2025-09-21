#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "sqlite3.h"
#include "sqliteRoutines.h"

// Copyright: CSIRO 2024
// George Hobbs: George.Hobbs@csiro.au

void runSQL(char *cmd,sqlite3 *db)
{
  int rc;
  sqlite3_stmt *statement;

  if (sqlite3_prepare_v2(db,cmd,-1,&statement,NULL) == SQLITE_OK)
    {
      sqlite3_step(statement);
      sqlite3_finalize(statement);
    }
}

int runSQL_returnStr(char *cmd,sqlite3 *db,char *ret)
{
  int rc;
  sqlite3_stmt *statement;
  int  cols,col;
  int result=0;
  int row=0;
  int retVal=-1;

  if (sqlite3_prepare_v2(db,cmd,-1,&statement,NULL) == SQLITE_OK)
    {
      cols = sqlite3_column_count(statement);
      if (cols != 1)
	{
	  printf("Wrong number of columns in function call\n");
	  exit(1);
	}
      result = sqlite3_step(statement);
      if (result == SQLITE_ROW)
	{
	  strcpy(ret,(char *)sqlite3_column_text(statement,0));
	  retVal=1;
	}
      sqlite3_finalize(statement);
    }
  return retVal;
}

int runSQL_returnInt(char *cmd,sqlite3 *db)
{
  int rc;
  sqlite3_stmt *statement;
  int  cols,col;
  int result=0;
  int row=0;
  int retVal=-1;

  if (sqlite3_prepare_v2(db,cmd,-1,&statement,NULL) == SQLITE_OK)
    {
      cols = sqlite3_column_count(statement);
      if (cols != 1)
	{
	  printf("Wrong number of columns in function call\n");
	  exit(1);
	}
      result = sqlite3_step(statement);
      if (result == SQLITE_ROW)
	retVal = sqlite3_column_int(statement,0);

      sqlite3_finalize(statement);
    }

  return retVal;
}

float runSQL_returnFloat(char *cmd,sqlite3 *db)
{
  int rc;
  sqlite3_stmt *statement;
  int  cols,col;
  int result=0;
  int row=0;
  float retVal=-1;

  if (sqlite3_prepare_v2(db,cmd,-1,&statement,NULL) == SQLITE_OK)
    {
      cols = sqlite3_column_count(statement);
      if (cols != 1)
	{
	  printf("Wrong number of columns in function call\n");
	  exit(1);
	}
      result = sqlite3_step(statement);
      if (result == SQLITE_ROW)
	retVal = sqlite3_column_double(statement,0);

      sqlite3_finalize(statement);
    }

  return retVal;
}

int runSQL_returnFloatArray(char *cmd,sqlite3 *db,float *val)
{
  int rc;
  sqlite3_stmt *statement;
  int  cols,col;
  int result=0;
  int row=0;
  if (sqlite3_prepare_v2(db,cmd,-1,&statement,NULL) == SQLITE_OK)
    {
      cols = sqlite3_column_count(statement);
      if (cols != 1)
	{
	  printf("Wrong number of columns in function call\n");
	  exit(1);
	}
      while (1==1)
	{
	  result = sqlite3_step(statement);
	  if (result == SQLITE_ROW)
	      val[row] = sqlite3_column_double(statement,0);
	  else
	    break;

	  row++;
	}
      sqlite3_finalize(statement);
    }
  return row;

}

int runSQL_returnIntStrArray(char *cmd,sqlite3 *db,int *intVal,char **strVal)
{
  int rc;
  sqlite3_stmt *statement;
  int  cols,col;
  int result=0;
  int row=0;
  if (sqlite3_prepare_v2(db,cmd,-1,&statement,NULL) == SQLITE_OK)
    {
      cols = sqlite3_column_count(statement);
      if (cols != 2)
	{
	  printf("Wrong number of columns in function call\n");
	  exit(1);
	}
      while (1==1)
	{
	  result = sqlite3_step(statement);
	  if (result == SQLITE_ROW)
	      intVal[row] = sqlite3_column_int(statement,0);
	  else
	    break;

	  if (result == SQLITE_ROW)
	    strcpy(strVal[row],(char *)sqlite3_column_text(statement,1));
	  else 
	    break;
	  row++;
	}
      sqlite3_finalize(statement);
    }
  return row;
}

int runSQL_returnIntArray(char *cmd,sqlite3 *db,int *intVal)
{
  int rc;
  sqlite3_stmt *statement;
  int  cols,col;
  int result=0;
  int row=0;
  if (sqlite3_prepare_v2(db,cmd,-1,&statement,NULL) == SQLITE_OK)
    {
      cols = sqlite3_column_count(statement);
      if (cols != 1)
	{
	  printf("Wrong number of columns in function call\n");
	  exit(1);
	}
      while (1==1)
	{
	  result = sqlite3_step(statement);
	  if (result == SQLITE_ROW)
	      intVal[row] = sqlite3_column_int(statement,0);
	  else
	    break;
	  row++;
	}
      sqlite3_finalize(statement);
    }
  return row;
}

int runSQL_returnStrArray(char *cmd,sqlite3 *db,char **strVal)
{
  int rc;
  sqlite3_stmt *statement;
  int  cols,col;
  int result=0;
  int row=0;

  if (sqlite3_prepare_v2(db,cmd,-1,&statement,NULL) == SQLITE_OK)
    {
      cols = sqlite3_column_count(statement);
      if (cols != 1)
	{
	  printf("Wrong number of columns in function call\n");
	  exit(1);
	}
      while (1==1)
	{
	  result = sqlite3_step(statement);
	  if (result == SQLITE_ROW)
	    {
	      if (sqlite3_column_text(statement,0)==NULL)
		{
		  strcpy(strVal[row],"NULL");
		}
	      else
		strcpy(strVal[row],(char *)sqlite3_column_text(statement,0));
	    }
	  else
	    break;
	  row++;
	}
      sqlite3_finalize(statement);
    }
  return row;
}

