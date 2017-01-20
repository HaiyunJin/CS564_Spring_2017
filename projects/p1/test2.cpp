// istream::getline example
#include <iostream>     // std::cin, std::cout
#include <string>

#include <fstream>
#include <sstream>
#include "common.h"

using namespace std;



// /** convert str to lower case in-place */
// void toLowerCase(string &str) {
//   const int length = str.length();
//   for ( int i = 0 ; i < length; ++i ) {
//     str[i] = tolower(str[i]);
//   }
// }


int main () {
  // char name[256], title[256];
  string name, title;
  ofstream myfile;
  ifstream myfilein;
  myfilein.open("test2.in");
  myfile.open("newfile.txt");
//   myfile<<"new content" << endl;
//  myfile.close();

//   streambuf *cinbuf = cin.rdbuf();
//   streambuf *coutbuf = cout.rdbuf();
//   cin.rdbuf(myfilein.rdbuf());
//   cout.rdbuf(myfile.rdbuf());


  std::cout << "Please, enter your name: ";
  getline(cin,name);
//  cin >> name;
  toLowerCase(name);
//   getline(myfilein,name);

  std::cout << "Please, enter your favourite movie: ";
  getline(cin,title);
//  cin>>title;
  toLowerCase(title);
//   getline(myfilein,title);

  std::cout << endl;
  std::cout << name << "'s favourite movie is " << title;
  std::cout << endl;

  char c;
  string a("  De*%#$lp  ld'dp 12dd dd d||~a");
  cout<<a<<endl;
  istringstream astr(a);
  ostringstream ostr;
  for ( int i = 0 ; i < a.size(); ++i ) {
    astr.get(c);
    if ( (c >= 'A' && c <= 'Z') 
        || (c >= 'a' && c <= 'z') 
        || (c >= '0' && c <= '9') 
        || (c == '\'' ) ) {
      //cout<<c<<endl;
      ostr<<c;
    } else {
      ostr<<" ";
    }
  }
  cout<<ostr.str()<<endl;
//     astr.get(c);
//     cout<<c<<endl;
//   }

  RBTree *tree;
  cout<<tree<<endl;
  tree = new RBTree;
  cout<<tree<<endl;
  delete tree;

  return 0;
}
