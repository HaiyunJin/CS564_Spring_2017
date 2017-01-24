/** 
 *  @file wl.cpp
 *  @author Haiyun Jin
 *  @version 0.1
 *  
 *   Description: Word locator
 *   Student Name: Haiyun Jin
 *   UW Campus ID: 9069998087
 *   email: hjin38@wisc.edu
 */

/**
 * @mainpage
 *  Word Locator.\n
 *  Run with ./wl \n
 *  Commands:
 *  - load <filename> \n Load a file. Store all words.\n
 *  - locate <word> <n> \n find the position of the nth occurrance of word.\n
 *  - new \n Clear the stores words.\n
 *  - end \n End the program.
 */

#include <iostream>
#include <fstream>
#include <sstream> // string stream
#include <string>
#include <stdlib.h>
#include "wl.h"

using namespace std;

int buildTree(ifstream &srcfile, RBTree *tree);
int locate(RBTree *tree, string word, int index);
void insert(RBTree *tree, string word, int pos);
int filterStr(string &line);
int splitstr(string &line, string **argvs);

/**
 *  @brief Print out the usage
 */
void 
usage() {
  cerr<<"./wl"<<endl;
  exit(1);
}


/** @fn void invalidcmd()
 *  @brief Print out the standard error message
 */
void
invalidcmd() {
  cout<<"ERROR: Invalid command"<<endl; 
}

/** @fn int main(argc, *argv[])
 *  @brief Main
 *  @param argc - number of argument passed from command line
 *  @param argv - array of string
 */
int 
main(int argc, char *argv[]) {
  if ( argc > 1 ) usage();

  string *argvs, command, word;
  char *endptr = NULL;
  int argnum, index, pos;
  ifstream srcfile;
  RBTree *tree = new RBTree;
  // main loop
  while ( true ) {
    cout<<'>';
    getline(cin,command);
//     cout<<command<<endl;
    // parse the command
    if ( (argnum = splitstr(command, &argvs)) ==  0 )
      continue; // no command, try read again
    
    command = argvs[0];
    // Check the first command
    if ( command == "load" && argnum == 2 ) {
//      cout<< "I will load" << endl;
        srcfile.open(argvs[1].c_str());
        if(srcfile.fail()) {
//  cerr<<"Open file " << argvs.at(1).c_str() << " failed"<<endl;
          invalidcmd();
          delete [] argvs;
          continue;
        }
        tree->clear();
        buildTree(srcfile, tree);
        //
    } else if ( command == "locate" && argnum == 3 ) {
// cout<< "I will locate" << endl;
      word = argvs[1]; //< whatever being located at second arg
      if (filterStr(word) == 1 ) { // check if the word is valid
        invalidcmd();
        delete [] argvs;
        continue;
      }
      index = strtol(argvs[2].c_str(),&endptr, 10);
      if ( *endptr != '\0' ) {
        invalidcmd();
        free(endptr);
        delete [] argvs;
        continue;
      }
      // valid query, look into the database and find
// cout << "Will perform: locate "<<word<<" "<<index<<endl; 
      if ( (pos = locate(tree, word, index)) < 0 ) {
        // cerr << "No matching entry"<<endl;
        cout << "No matching entry"<<endl;
        delete [] argvs;
        continue;
      }
      cout<<pos<<endl;
    } else if ( command == "new" && argnum == 1 ) {
// cout<< "I will new" << endl;
        tree->clear();
    } else if ( command == "end" && argnum == 1 ) {
// cout<< "I will clean mem and exit" << endl;
        // clean the memory
        delete tree;
        delete [] argvs;
        return 1;
// } else if ( command == "print" ) {
//   tree->printTree();
    } else // any other command
      invalidcmd();
    delete [] argvs;
  }
  return 1;
}

/** 
 * @brief Build tree from the file handler and close it
 * 
 * @param srcfile - ifstream file to read words from
 * @param tree    - pointer to the RBTree
 * @return  1 if build successfully, -1 if failed
 */
int
buildTree(ifstream &srcfile, RBTree *tree) {
  string *words;
  string line;
  // readline
  int pos = 1; // position starts from 1, not 0
  int wcount;
  while (getline(srcfile,line)) {
    filterStr(line);  // wash the line
    if ( (wcount = splitstr(line, &words)) == 0 ) { // split into words
      continue;
    }
//      for ( int i = 0 ; i < wcount; i++ )
//        cout << words[i] << " ";
//      cout<< endl;
    for ( int n = 0 ; n < wcount ; ++n )
      insert(tree, words[n], pos++);
    delete [] words;
  }
  srcfile.close();
  return 1;
}

/**
 * @brief Helper function that filter out all invalid characters
 *
 * @param line - The line to be filtered;
 * @return -1 if no invalid char. 1 if at least one invalid char
 */
int filterStr(string &line) {
    stringstream lineStr;
    stringstream olineStr;
    lineStr.str("");
    olineStr.str("");
// cout<<"Whole line: "<<line<<endl;
    lineStr<<line;
    int change = -1;
    char c;
    for ( unsigned int i = 0 ; i < line.size(); ++i ) {
      lineStr.get(c);
      if ( (c >= 'A' && c <= 'Z')     // capital letter
          || (c >= 'a' && c <= 'z')   // lower case letter
          || (c >= '0' && c <= '9')   // numbers 
          || (c == '\'' ) ) {         // apostrophe
        olineStr<<c;
      } else {
        change = 1;
        olineStr<<" ";
      }
    }
// cout<<"rewri line: "<<olineStr.str()<<endl;
    line = olineStr.str();
    return change;
}

/**
 * @brief Insert the word and pos into the tree
 *
 * @param tree - tree pointer
 * @param word - string word
 * @param pos  - int position
 */
void insert(RBTree *tree, string word, int pos) {
  wpp_t newword;
  newword.word = word;
  newword.pos = pos;
  tree->insert(newword);
}

/** 
 * @brief Given the word and index, return the position in the file 
 *
 * @param tree  - The RBTree to look into
 * @param word  - A string of word to locate
 * @param index - The index'th occurrence of word
 * @return An integer of the postion in the file. If not found, return -1.
 */
int
locate(RBTree *tree, string word, int index) {
  int pos = -1;
  TreeNode *node = tree->getNode(word);
// cout<<"Returned node: "<<node<<endl;
  if ( node != NULL )
    pos = node->poss->get(index-1); // index in tree start from 0
  return pos;
}

/** 
 *  @brief Split the command read from user input
 *  If there is no arguments in the line, does not allocate memory,
 *  thus do not need to delete.
 *
 *  @param command - A string contains the command read
 *  @param argvs - arguements splited from the command
 *  @return number of argument int the line.
 */
int
splitstr(string &line, string **argvs) {
  stringstream lineStr(line);
  string arg, *strs;
  int noarg = 0, i = 0;

  while ( lineStr >> arg)
    noarg++;
  if ( noarg == 0 ) return 0;

  lineStr.str(""); // empty stream
  lineStr.clear(); // clear the eof bit
  lineStr<<line;   // fill with new

  strs = new string[noarg];
  while ( lineStr >> arg) {
    toLowerCase(arg);
    strs[i++] = string(arg);
  }
  
  *argvs = strs;
  return noarg;
}

