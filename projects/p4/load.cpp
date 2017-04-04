#include <stdio.h>
#include "sqlite3.h"
#include <iostream>
#include <fstream>
#include "load.h"
#include <string.h>
#include <string>
// #define DEBUG
// #define PRINT
// #define PRINTMSG

using namespace std;

  char *zErrMsg = 0;
  char cmd[CMD_SIZE];
  int rc;
  sqlite3 *db;
  const string valueDelim = "^";
  const string charQuote = "~";

void  clearcmd()
{
  memset(cmd, '\0', CMD_SIZE); // clear cmd
}

int main(int argc, char *argv[])
{
    int conn;
    conn = sqlite3_open("nutrients.db",&db);
    if ( conn ) {
      fprintf(stderr, "Unable to open the data base: %s\n", sqlite3_errmsg(db));
      return(1);
    }

    string filename;

    { // Table 4. Food Description File
      filename = "FOOD_DES.txt"; // 4
      int numValue = 14;
      string values[] = {"NDB_No","FdGrp_Cd","Long_Desc","Shrt_Desc","ComName",
        "ManufacName","Survey","Ref_desc","Refuse","SciName","N_Factor",
        "Pro_Factor","Fat_Factor","CHO_Factor"};
      Datatype types[] = {VARCHAR,VARCHAR,VARCHAR,VARCHAR,VARCHAR,VARCHAR,
         VARCHAR,VARCHAR,NUMERIC,VARCHAR,NUMERIC,NUMERIC,NUMERIC,NUMERIC};
      int sizes[] = {5, 4 , 200, 60, 100, 65, 1, 135, 2, 65,4,4,4,4};
      bool hasNull[] = { false, false, false, false, true, true, true,
                        true, true, true, true, true, true, true};
      loadData(db,filename, numValue, values, types, sizes, hasNull);
    }

    { // Table 5. Food Group Description File
      filename = "FD_GROUP.txt"; // 5
      int numValue = 2;
      string values[] = {"FdGrp_Cd", "FdGrp_Desc"};
      Datatype types[] = {VARCHAR,VARCHAR};
      int sizes[] = {4, 60};
      bool hasNull[] = {false, false};
      loadData(db,filename, numValue, values, types, sizes, hasNull);
    }

    { // Table 6. LanguaL Factor File 
      filename = "LANGUAL.txt";  // 6
      int numValue = 2;
      string values[] = {"NDB_No","Factor_Code"};
      Datatype types[] = {VARCHAR, VARCHAR};
      int sizes[] = {5,5};
      bool hasNull[] = {false,false}; //
      loadData(db,filename, numValue, values, types, sizes, hasNull);
    }

    { // Table 7. LanguaL Factors Description File
      filename = "LANGDESC.txt"; // 7
      int numValue = 2;
      string values[] = {"Factor_Code", "Description"};
      Datatype types[] = {VARCHAR, VARCHAR};
      int sizes[] = {5,140};
      bool hasNull[] = {false,false}; // 
      loadData(db,filename, numValue, values, types, sizes, hasNull);
    }

    { // Table 8. Nutrient Data File
      filename = "NUT_DATA.txt"; // 8
      int numValue = 18;
      string values[] = {"NDB_No","Nutr_No", "Nutr_Val", "Num_Data_Pts", 
          "Std_Error", "Src_Cd", "Deriv_Cd", "Ref_NDB_No", "Add_Nutr_Mark",
          "Num_Studies", "Min", "Max", "DF", "Low_EB", "Up_EB", "Stat_cmt",
          "AddMod_Date", "CC"};
      Datatype types[] = {  VARCHAR, VARCHAR, NUMERIC,
                            NUMERIC, NUMERIC, VARCHAR,
                            VARCHAR, VARCHAR, VARCHAR,
                            NUMERIC, NUMERIC, NUMERIC,
                            NUMERIC, NUMERIC, NUMERIC,
                            VARCHAR, VARCHAR, VARCHAR};
      int sizes[] = {5,3,10,5, 8,2, 
                      4,5,1,2,10,10,
                      4,10,10,10,10,1};
      bool hasNull[] = {false, false, false, false, true, false,
                        true, true, true, true, true, true,
                        true, true, true, true, true, true,};
      loadData(db,filename, numValue, values, types, sizes, hasNull);
    }

    { // Table 9. Nutrient Definition File
      filename = "NUTR_DEF.txt"; // 9
      int numValue = 6;
      string values[] = {"Nutr_No", "Units", "Tagname",
                        "NutrDesc", "Num_Dec", "SR_Order"};
      Datatype types[] = {  VARCHAR, VARCHAR, VARCHAR,
                            VARCHAR, VARCHAR, NUMERIC};
      int sizes[] = {3, 7 , 20, 60 ,1 ,6};
      bool hasNull[] = {false, false, true, false, false, false };
      loadData(db,filename, numValue, values, types, sizes, hasNull);
    }

    { // Table 10. Source Code File
      filename = "SRC_CD.txt";   // 10
      int numValue = 2;
      string values[] = {"Src_Cd", "SrcCd_Desc"};
      Datatype types[] = {VARCHAR, VARCHAR};
      int sizes[] = {2,60};
      bool hasNull[] = {false,false}; // 
      loadData(db,filename, numValue, values, types, sizes, hasNull);
    }

    { // Table 11. Data Derivation Code Description File
      filename = "DERIV_CD.txt"; // 11
      int numValue = 2;
      string values[] = {"Deriv_Cd", "Deriv_Desc"};
      Datatype types[] = {VARCHAR,VARCHAR};
      int sizes[] = {4, 120};
      bool hasNull[] = { false, false};
      loadData(db,filename, numValue, values, types, sizes, hasNull);
    }

    { // Table 12. Weight File
      filename = "WEIGHT.txt";   // 12
      int numValue = 7;
      string values[] = {"NDB_No", "Seq","Amount","Msre_Desc","Gm_Wgt",
                        "Num_Data_Pts","Std_Dev"};
      Datatype types[] = {VARCHAR, VARCHAR, NUMERIC, VARCHAR,
                          NUMERIC, NUMERIC, NUMERIC};
      int sizes[] = {5, 2, 5, 84, 7, 3,7};
      bool hasNull[] = { false, false, false, false, false, true, true};
      loadData(db,filename, numValue, values, types, sizes, hasNull);
    }

    { // Table 13. Footnote File
      filename = "FOOTNOTE.txt"; // 13
      int numValue = 5;
      string values[] = {"NDB_No", "Footnt_No","Footnt_Typ",
                        "Nutr_No","Footnt_Txt"};
      Datatype types[] = {VARCHAR,VARCHAR,VARCHAR,VARCHAR,VARCHAR};
      int sizes[] = {5, 4, 1, 3, 200};
      bool hasNull[] = { false, false, false, true, false};
      loadData(db,filename, numValue, values, types, sizes, hasNull);
    }

    { // Table 14. Sources of Data Link File
      filename = "DATSRCLN.txt"; // 14
      int numValue = 3;
      string values[] = {"NDB_No", "Nutr_No","DataSrc_ID"};
      Datatype types[] = {VARCHAR,VARCHAR,VARCHAR};
      int sizes[] = {5, 3, 6};
      bool hasNull[] = { false, false, false};
      loadData(db,filename, numValue, values, types, sizes, hasNull);
    }

    { // Table 15. Sources of Data File
      filename = "DATA_SRC.txt"; // 15
      int numValue = 9;
      string values[] = {"DataSrc_ID", "Authors","Title","Year","Journal",
                            "Vol_City","Issue_State","Start_Page","End_Page"};
      Datatype types[] = {VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR,
                          VARCHAR, VARCHAR, VARCHAR, VARCHAR};
      int sizes[] = {6, 255, 255, 4, 135, 16, 5, 5, 5};
      bool hasNull[] = { false, true, false,  true, true, true,
                         true, true, true};
      loadData(db,filename, numValue, values, types, sizes, hasNull);
    }

    sqlite3_close(db);
}

void loadData(sqlite3 *db, string filename, int numValue,
              string values[], Datatype types[], int sizes[], bool hasNull[] )
{
      string tablename = filename.substr(0,filename.length()-4);
      rc = createTable(db, tablename, numValue, values, types, sizes, hasNull);
      if ( rc != -1 )
        addToTable(db, filename, tablename);
#ifdef PRINT
      // check the content
      clearcmd();
      sprintf(cmd,"select * from %s;", tablename.c_str());
      rc = sqlite3_exec(db, cmd, callback, 0, &zErrMsg);
       if( rc!=SQLITE_OK ){
         fprintf(stderr, "SQL error: %s\n", zErrMsg);
         sqlite3_free(zErrMsg);
       }
#endif
}

static int callback(void *NotUsed, int argc, char **argv, char **azColName){
  printf("%s", argv[0] ? argv[0] : "NULL");
  for(int i=1; i<argc; i++){
    printf("|%s", argv[i] ? argv[i] : "NULL");
  }
  printf("\n");
  return 0;
}

int createTable(sqlite3* db, string& tablename, int numValue,
                string values[], Datatype types[], int sizes[], bool hasNull[])
{
  clearcmd();
  sprintf(cmd, "create table %s (\n", tablename.c_str());

  for ( int i = 0 ; i < numValue; ++i ) {
    if ( types[i] == VARCHAR ) {
      sprintf(cmd+strlen(cmd), "\t%s varchar(%d)", values[i].c_str(), sizes[i]);
    } else {
      sprintf(cmd+strlen(cmd), "\t%s decimal(%d)",values[i].c_str(), sizes[i] );
    }
    if ( !hasNull[i] ) sprintf(cmd+strlen(cmd), " NOT NULL");
    sprintf(cmd+strlen(cmd), ",\n");
  }
  // remove the last comma
  sprintf(cmd+strlen(cmd)-2, "\n);\n");
#ifdef DEBUG
  cout<<" strlen of cmd: "<<strlen(cmd)<<endl;
  cout<<cmd<<endl;
#endif
//   cout<<cmd<<endl;

  rc = sqlite3_exec(db, cmd, callback, 0, &zErrMsg);

  if( rc!=SQLITE_OK ){
    fprintf(stderr, "SQL error: %s\n", zErrMsg);
    char * pos = strstr(zErrMsg, "already exists");
    sqlite3_free(zErrMsg);
    if ( pos != NULL )
      return -1;
  }
  return 0;
}

int addToTable(sqlite3* db, string &filename, string& tablename)
{
    ifstream rawDataFileStream(filename.c_str());
    string line;
    size_t start = 0;
    size_t idx;
    // Read data and insert into db
    while ( getline(rawDataFileStream, line) ) {
#ifdef DEBUG
  cout<<endl<<"new line:" <<line<<endl;
#endif
      memset(cmd, '\0', CMD_SIZE); // clear cmd
      sprintf(cmd, "insert into %s  values(\n", tablename.c_str());
      
      // escape quote mark
      start = 0;
      while ((idx = line.find('"', start) ) != string::npos ) {
        line.replace(idx,1,"\"\"");
        start = idx+2;
      }
      // separate values
      start = 0;
      while ((idx = line.find(valueDelim, start) ) != string::npos ) {
        line.replace(idx,1,",");
        start = idx+1;
      }
      // surrounding strings with ""
      start = 0;
      while ((idx = line.find(charQuote, start) ) != string::npos ) {
        line.replace(idx,1,"\"");
        start = idx+1;
      }
      // replace ",," as ",NULL,"
      while (( idx = line.find(",,") ) != string::npos ) {
          line.replace(idx,2,",NULL,");
      }
      // check if the last char is ',' , replace with ",NULL"
      if ( (idx = line.find(",", line.size()-2))==line.size()-2 ) {
          line.replace(idx,1,",NULL");
      }
      sprintf(cmd+strlen(cmd), "%s\n);", line.c_str());
#ifdef DEBUG
  cout<<"replaced line:" <<line<<endl;
  cout<<" strlen of cmd: "<<strlen(cmd)<<endl;
  cout<<cmd<<endl;
#endif
      rc = sqlite3_exec(db, cmd, callback, 0, &zErrMsg);
      if( rc!=SQLITE_OK ){
        fprintf(stderr, "SQL error: %s\n", zErrMsg);
        sqlite3_free(zErrMsg);
      }
    }
    return 0;
}
