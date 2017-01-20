

#include <string>

using namespace std;


/** convert str to lower case in-place */
void toLowerCase(string &str) {
  const int length = str.length();
  for ( int i = 0 ; i < length; ++i ) {
    str[i] = tolower(str[i]);
  }
}


/** ArrayList
 *  TreeNode
 *  RBTree
 */

/**  ArrayList:
 *   At each node, use an arraylist to store the pos
 */
class ArrayList
{
  public:
    ArrayList(); //< Constructor
    ~ArrayList(); //< Destructor
    void add(int pos); 
    int get(int n);
  private:
    int* array;
    int arraySize;
    int size;
    void resize();
};

/** Constructor of ArrayList */
ArrayList::ArrayList(void) {
  arraySize = 1;
  size = 0;
  array = new int[arraySize];
}

/** Destructor of ArrayList */
ArrayList::~ArrayList(void) {
  delete [] array;
}

/** Add a pos into the arary
 * @param pos new pos
 */
void ArrayList::add(int pos) {
  if ( size == arraySize ) {//< need to resize
    resize();
  }
  array[size++] = pos;
  return;
}

/** Resize the arrat */
void ArrayList::resize() {
  arraySize = arraySize + arraySize/2 + 1; //< increase by about 50%
  int *newarray = new int[arraySize];
  int i;
  for ( i = 0 ; i < size ; i++) {
    newarray[i] = array[i];
  }
  delete [] array;
  array = newarray;
  return;
}

/** get the nth pos */
int ArrayList::get(int n) {
  if ( n >= size || n < 0 ) return -1;
  return array[n];
}


/** word-pos pair*/
typedef struct wpp {
  string word;
  int pos;
} wpp_t;

/** Binary tree:
 *   Words are stored in the self-balenced binary tree.
 *   RB tree:
 *   Node: 
 *      string word
 *      arraylist poss
 *      Node left, right
 */
class TreeNode
{
  public:
    TreeNode(wpp_t newword); //< Constructor
    ~TreeNode(); //< Destructor
    ArrayList* poss; //< pos's
    string word;
    TreeNode *left, *right;
    TreeNode* parent;
    char color;
};

/** Constructor of TreeNode */
TreeNode::TreeNode(wpp_t newword) {
  word = newword.word;
  poss = new ArrayList;
  poss->add(newword.pos);
  color = 'r';
  parent = NULL;
  left = NULL;
  right = NULL;
}

/** Destructor of TreeNode */
TreeNode::~TreeNode() {
  // delete word;
  delete poss;
  delete left;
  delete right;
  delete parent;
}



/** Red-Black Tree 
 * ref: https://gist.github.com/mgechev/5911348
 */
class RBTree
{
  public:
    RBTree();  //< constructor
    ~RBTree();  //< destructor
    void clear();
    TreeNode* getNode(string word);
};

/** Constructor of RBTree */
RBTree::RBTree() {
  cout<<"New RB Tree"<<endl;
}

/** Destructor of RBTree */
RBTree::~RBTree() {
  cout<<"Destruct RB Tree"<<endl;
}

void RBTree::clear() {
  cout<<"Delete RB Tree content"<<endl;
}

/** Accessor: getNode
 * Lookup a word, return the node 
 * @param word - A string that is needed to locate
 */
TreeNode* RBTree::getNode(string word) {
  cout << "I am locating "<<word<<endl;
  return NULL;
}

