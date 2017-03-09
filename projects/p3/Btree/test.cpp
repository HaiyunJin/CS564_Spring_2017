
#include <vector>
#include <iostream>
#include <string>
#include <string.h>

// #define DEBUG

using namespace std;


int readDoubleAsInt( int a);
void printstuff(void * stuff);

int main(int argc, char **argv)
{
  string *str2 = new string("2");
  string *str1 = new string("1");
  bool b;
  b = *str1 < *str2;
  cout<<b<<endl;
  b = *str1 > *str2;
  cout<<b<<endl;
  b = str1 < str2;
  cout<<b<<endl;
  b = str1 > str2;
  cout<<b<<endl;
  readDoubleAsInt(12.3);


  cout<<"Using char array"<<endl;
  char a[5];
  a[0] = 'a';
  for ( int i = 1 ; i < 5; ++i ) {
    a[i] = a[i-1]+1;
  }

  printstuff((void*)a);

  cout<<"Using std::stirng"<<endl;
//   string * stra = new string("abcdefg");
  string stra ="cdefg";
  string * strap = &stra;
//   void * strav = reinterpret_cast<void *>(stra);
//   cout<<*stra<<" the original string"<<endl;
//   printstuff((void*)(&(stra[0u])));
//   printstuff((void*)(&((*strap)[0u])));
//   printstuff((void*)(&((*strap)[0u])));
  printstuff((void*)(&stra[0u]));
  printstuff((void*)(&stra));



}

int readDoubleAsInt( int a) {
  cout<<a<<endl;
}


void printstuff(void * stuff) {
//   char* charStuff = reinterpret_cast<char *>( stuff);

  char res[5];
  strncpy(res, (char*)stuff, 5);
//   char* charStuff = (char *)(stuff);
  cout<<"  char Stuff "<<res<<endl;

// 
//   string * stringStuff = (string*)stuff;
//   cout<<"string Stuff "<<*stringStuff<<endl;
}




