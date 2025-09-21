// Copyright: CSIRO 2024
// George Hobbs: George.Hobbs@csiro.au


void runSQL(char *cmd,sqlite3 *db);
int runSQL_returnIntStrArray(char *cmd,sqlite3 *db,int *intVal,char **strVal);
int runSQL_returnInt(char *cmd,sqlite3 *db);
float runSQL_returnFloat(char *cmd,sqlite3 *db);
int runSQL_returnIntArray(char *cmd,sqlite3 *db,int *intVal);
int runSQL_returnStrArray(char *cmd,sqlite3 *db,char **strRet);
int runSQL_returnFloatArray(char *cmd,sqlite3 *db,float *val);
int runSQL_returnStr(char *cmd,sqlite3 *db,char *ret);
