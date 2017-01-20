/* \file
 *  File: wl.h
 *  
 *   Description: Add stuff here ... 
 *   Student Name: Haiyun Jin
 *   UW Campus ID: 9069998087
 *   email: Add stuff here .. 
 */

//#include <stdio.h>
#include <iostream>
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
    void print();
  private:
    int* array;
    int arraySize;
    int size;
    void resize();
};

void ArrayList::print() {
  for ( int i = 0 ; i < size ; ++i ) {
    cout << array[i]<<" ";
  }
  cout << endl;
}

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

/** 
 * Resize the capacity of array to 150%
 */
void ArrayList::resize() {
  arraySize = arraySize + arraySize/2 + 1; //< increase by about 50%
  int *newarray = new int[arraySize];
  for ( int i = 0 ; i < size ; i++)
    newarray[i] = array[i];
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
 *   Words are stored in the self-balanced binary tree.
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
    TreeNode *getLeft();
    TreeNode *getRight();
    void setLeft(TreeNode* node);
    void setRight(TreeNode* node);
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
  delete poss;
}

/** Get the left child */
TreeNode *TreeNode::getLeft(){
  return left;
}

/** Get the right child */
TreeNode *TreeNode::getRight(){
  return right;
}

/** Set left child 
 *  @param node left node to add
 */
void TreeNode::setLeft(TreeNode* node) {  // < there must be bug in param passing
  left = node;
}

/** Set right child 
 *  @param node right node to add
 */
void TreeNode::setRight(TreeNode* node) {  // < there must be bug in param passing
  right = node;
}


/** Red-Black Tree 
 * ref: https://en.wikipedia.org/wiki/Red%E2%80%93black_tree
 */
class RBTree
{
  public:
    RBTree();  //< constructor
    ~RBTree();  //< destructor
    void insert(wpp_t newword);
    void clear();
    TreeNode* getNode(string word);
    void printTree();
  private:
    TreeNode* root;
    void balance(TreeNode *node);
    void deleteNode(TreeNode *node);
    TreeNode* getUncle(TreeNode *node);
    TreeNode* getGparent(TreeNode *node);
    void aug_printTree(TreeNode *tree);
};


/** 
 * Public print tree method 
 * */
void RBTree::printTree() {
  aug_printTree(root);
}


/**
 * Private recursive printTree method
 *
 * @param tree - The tree to be printed
 */
void RBTree::aug_printTree(TreeNode *tree) {
  if ( tree == NULL ) return;
  aug_printTree(tree->left);
cout<<tree->word<<": ";
tree->poss->print();
  aug_printTree(tree->right);
}


/** Constructor of RBTree */
RBTree::RBTree() {
//  cout<<"New RB Tree"<<endl;
  root = NULL;
}

/** Destructor of RBTree */
RBTree::~RBTree() {
//  cout<<"Destruct RB Tree"<<endl;
  deleteNode(root);
}

void RBTree::clear() {
//  cout<<"Delete RB Tree content"<<endl;
  if ( root ) {
    deleteNode(root);
    root = NULL;
  }
}

/** Recursively delete the whole tree */
void RBTree::deleteNode(TreeNode* node) {
  if ( node == NULL ) return;
  TreeNode *left, *right;
  left = node->left;
  right = node->right;
// cout<<"Address of node to be deleted: "<<node<<endl;
  delete node;
  deleteNode(left);
  deleteNode(right);
  return;
}

/** Accessor: getNode
 * Lookup a word, return the node 
 * @param word - A string that is needed to locate
 */
TreeNode* RBTree::getNode(string word) {
  if (root == NULL ) return NULL; // new node yet
  if ( word == "" ) return NULL; // invalid word
  TreeNode *curr;
  curr = root;
  while (curr != NULL ) {
    if ( curr->word == word ) {
      return curr;
    } else if ( curr->word < word )
      curr = curr->right;
    else
      curr = curr->left;
  }
  return NULL ; // no such node
}


/** Mutator: insert
 * Insert new word, either add to existing or create new node
 * @param newword wpp_t stucture with {word, pos}
 */
void RBTree::insert(wpp_t newword) {
  TreeNode *curr, *parent;
  toLowerCase(newword.word);
  curr = root;
  parent = curr;
  if ( root == NULL ) {
// cout<<"This is the root with "<<newword.word<<endl;
    root = new TreeNode(newword);
    curr = root;
// cout<< "Address of root: " <<root<<endl;
  } else {
    while ( curr!= NULL ){
      parent = curr;
// cout<< "Compare "<<curr->word<<" "<< newword.word<< endl;
      if ( curr->word == newword.word ) {
// cout<<"Just incr occurance of "<<newword.word<<" at "<<newword.pos<<endl;
        curr->poss->add(newword.pos);
        return; // only add a pos, no change in tree structure
      } else if ( curr->word < newword.word )
        curr = curr->right;
      else
        curr = curr->left;
    }
// cout<<"New node for "<<newword.word<<" and pos "<<newword.pos<<endl;
    curr = new TreeNode(newword);
    curr->parent = parent;
    if (parent->word < curr->word)
      parent->right = curr;
    else
      parent->left = curr;
  }
//cout<< "Address of parent: " <<parent<<endl;
  balance(curr);
}

/** balance
 * balance the tree by check the RB properties and rotate.
 * Starting from the newly inserted or deleted node,
 * Do it recursively.
 * @param node The starting node to balance.
 */
void RBTree::balance(TreeNode *node) {
//cout<<"balance(): Address of node: " << node<< endl;
  // Case 1: it's the root!
  if ( node->parent == NULL ) {
// cout<<"Case 1"<<endl;
    node->color = 'b';
    return;
  }

  // Case 2: parent is black, it is fine..
  if ( node->parent->color == 'b' ) {
// cout<<"Case 2"<<endl;
    return;
  }

  // Case 3: parent and uncle are red, I can't be red, I need to repaint
  TreeNode* uncle = getUncle(node);
  TreeNode* gp;
  if ( uncle != NULL && uncle->color == 'r' ) {
    gp = getGparent(node);
// cout<<"Case 3"<<endl;
    uncle->color = 'b';
    node->parent->color = 'b';
    gp->color = 'r';
    balance(gp);
    return;
  }

  // Case 4: parent red, uncle black, and node close to uncle
  TreeNode *save_p = node->parent;
  gp = getGparent(node);
  if ( node == save_p->right && gp->left == save_p ) {
// cout<<"Case 4 left"<<endl;
    // left rotate
    gp->left = node;
    node->parent = gp;
    save_p->right = node->left;
    if ( node->left != NULL )
      node->left->parent = save_p;
    node->left = save_p;
    save_p->parent = node;
  } else if ( node == node->parent->left && gp->right == save_p ) {
// cout<<"Case 4 right"<<endl;
    // right rotate
    gp->right = node;
    node->parent = gp;
    save_p->left = node->right;
    if ( node->right != NULL ) 
      node->right->parent = save_p;
    node->right = save_p;
    save_p->parent = node;
  } else { // if none of the 4 cases applies, you are fine.
// cout<<"Case 0"<<endl;
    return;
  }
  node = save_p;

  // Case 5: parent red, uncle black, I am far from uncle
  save_p = node->parent; // p changed, gp not change
  save_p->color = 'b';
  gp->color = 'r';
  if ( save_p == gp->left) {
// cout<<"Case 5 left"<<endl;
    // right rotate
    gp->left = save_p->right;
    if ( gp->left != NULL ) 
      gp->left->parent = gp;
    if ( gp->parent != NULL ) { // grandpa may be the root
      if ( gp->parent->left == gp )
        gp->parent->left = save_p;
      else 
        gp->parent->right = save_p;
    } else {
      root = save_p;
    }
    save_p->parent = gp->parent;
    save_p->right = gp;
    gp->parent = save_p;
  } else {
// cout<<"Case 5 right"<<endl;
    // left rotate
    gp->right = save_p->left;
    if ( gp->right != NULL ) 
      gp->right->parent = gp;
    if ( gp->parent != NULL ) {
      if ( gp->parent->right == gp ) 
        gp->parent->right = save_p;
      else 
        gp->parent->left = save_p;
    } else {
      root = save_p;
    }
    save_p->parent = gp->parent;
    save_p->left = gp;
    gp->parent = save_p;
  }
  return;
}

/** Get the uncle node */
TreeNode* RBTree::getUncle(TreeNode *node) {
  TreeNode *gp = getGparent(node);
  if ( gp == NULL ) // no uncle
    return NULL;
  if ( gp->left == node->parent )
    return gp->right;
  else
    return gp->left;
}

/** Get the gparent node */
TreeNode* RBTree::getGparent(TreeNode *node) {
  if ( node->parent )
    return node->parent->parent; 
  else
    return NULL;
//   try { // potentially no parent
//     return node->parent->parent;
//   } catch ( const exception&) {
//     return NULL;
//   }
}

