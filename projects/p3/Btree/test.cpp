
#include <vector>
#include <iostream>
#include <string>

// #define DEBUG

using namespace std;


int readDoubleAsInt( int a);

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
}

int readDoubleAsInt( int a) {
  cout<<a<<endl;
}

