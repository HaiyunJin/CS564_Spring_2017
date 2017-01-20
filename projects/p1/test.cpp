
#include <iostream>
using namespace std;

int
main() {
  int size = 10;
  int * array = new int[size];
  array[1] = 1; 
  array[2] = 2; 
  cout<<array[0]<<array[1]<<array[2]<<endl;
  cout<<array[3]<<array[4]<<array[5]<<endl;

  delete [] array;


  string str, str2;
  cout<<str.size()<<endl;
  str = "hello";
  cout<<str.size()<<endl;
  str = "hellodddddddddddddddddddddddddddddddddddddddddddddddddddddd";
  cout<<str.size()<<endl;
  str2 = "hello2";
  cout<<"str: " << (str<str2) << endl;


  typedef struct wpp {
    string word;
    int pos;
  } wpp_t;

  wpp_t wpp1;
  wpp1.word = "Hello";
  wpp1.pos=2;

  wpp_t wpp2;
  wpp2.word = "Hellodfdfdfddfd";
  wpp2.pos=2;

  cout<<"sizeof wpp_t "<<sizeof(wpp_t)<<endl;
  cout<<"sizeof wpp1 "<<sizeof(wpp1)<<endl;
  cout<<"sizeof wpp2 "<<sizeof(wpp2)<<endl;


  return 1;

}


