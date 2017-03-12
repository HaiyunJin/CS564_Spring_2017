
#include <vector>
#include <iostream>
#include <string>
#include <string.h>

// #define DEBUG

using namespace std;


int readDoubleAsInt( int a);
void printstuff(void * stuff);

template < class T >
void printT(void * a) {
  std::cout<<(*((T *)a))<<endl;
}


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

//   printstuff((void*)a);


  printT<char[5]>((void*)a);
  cout<<"Using std::stirng"<<endl;
//   string * stra = new string("abcdefg");
  string stra ="cdefg";
  string * strap = &stra;
//   void * strav = reinterpret_cast<void *>(stra);
//   cout<<*stra<<" the original string"<<endl;
//   printstuff((void*)(&(stra[0u])));
//   printstuff((void*)(&((*strap)[0u])));
//   printstuff((void*)(&((*strap)[0u])));
//   printstuff((void*)(&stra[0u]));
//   printstuff((void*)(&stra));
    printT<string>((void *)strap);

  cout<<"Using int"<<endl;
  int inta = 123;
  printT<int>((void*)(&inta));



  cout<<endl;
  cout<<endl;
  cout<<endl;
  cout<<"Testing char[] compare"<<endl;
//   char chara[6] = {'a','b','c','d','e','f'};
//   char charb[5] = {'a','b','d','e','f'};
  char chara[10] = {'0','0','0','2','5',' ','s','t','r'};
  char charb[10] = {'0','4','9','9','9',' ','s','t','r'};
// cout<<"char chara[6] = {'a','b','c','d','e','f'};"<<endl;
// cout<<"char charb[5] = {'a','b','d','e','f'};"<<endl;
  cout<<"char chara[10] = {'0','0','0','2','5',' ','s','t','r'};"<<endl;
  cout<<"char charb[10] = {'0','4','9','9','9',' ','s','t','r'};"<<endl;
  bool com = chara > charb;
  cout<<"chara > charb: "<<com<<endl;
  com = chara < charb;
  cout<<"chara < charb: "<<com<<endl;

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




