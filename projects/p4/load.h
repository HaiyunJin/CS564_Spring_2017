
using namespace std;


#define CMD_SIZE  1024
#define LINE_SIZE  1024

enum Datatype {
  VARCHAR = 0 ,
  NUMERIC = 1
};


static int callback(void *NotUsed, int argc, char **argv, char **azColName);

/**
 * Create a table in given database
 *
 * @param db Database to store the table
 * @param tablename Table name
 *
 * @return 0
 *
 * @author Haiyun Jin
 *
 */
// int createTable(sqlite3* db, string& tablename);
int createTable(sqlite3* db,
            string& tablename,
            int numValue,
            string values[],
            Datatype types[],
            int sizes[],
            bool hasNull[],
            bool unique[],
            string primarykey);

/**
 * Add the data in file into a table in Database
 *
 * @param db Database
 * @param filename File name of the data source
 * @oaram tablename Table to add data
 *
 * @return 0
 *
 * @author Haiyun Jin
 */
int addToTable(sqlite3* db, string &filename, string& tablename);


/** Perfome the creating tabel and insert data entries
 */
void loadData(sqlite3 *db,
              string filename,
              int numValue,
              string values[],
              Datatype types[],
              int sizes[],
              bool hasNull[],
              bool unique[],
              string primarykey);

