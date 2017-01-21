/*
 *  @file wl.h
 *  @author Haiyun Jin
 *  @version 0.1
 *  
 *   Description: Class implementation and helper function
 *   Student Name: Haiyun Jin
 *   UW Campus ID: 9069998087
 *   email: hjin38@wisc.edu
 */

#include <string>
using namespace std;

/** 
 * @brief word-pos pair
 *
 * word-position pair structure.
 * Has string word and int pos fields.
 */
typedef struct wpp {
  string word;
  int pos;
} wpp_t;

/** 
 * @brief Convert str to lower case in-place 
 *
 * @param str - string to convert
 */
void toLowerCase(string &str) {
  const int length = str.length();
  for ( int i = 0 ; i < length; ++i ) {
    str[i] = tolower(str[i]);
  }
}

/**  
 * @brief An implementation of Java ArrayList
 *
 * This implementation has only add and get method. 
 * That's enough for this project.
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
    int* array;  //< The acutual array that stores the integers.
    int arraySize;
    int size;
    void resize();
};

/*
 *  @breif Print the content in the List
 *
 *  separated by single space
 */
void ArrayList::print() {
  for ( int i = 0 ; i < size ; ++i )
    cout << array[i]<<" ";
  cout << endl;
}

/** 
 * @brief Constructor of ArrayList. 
 * 
 * Initialized the capacity as 1.
 */
ArrayList::ArrayList() {
  arraySize = 1;
  size = 0;
  array = new int[arraySize];
}

/**
 * @brief Destructor of ArrayList.
 * 
 * Delete the allocated array.
 */ 
ArrayList::~ArrayList() {
  delete [] array;
}

/** 
 * @brief Add an integer into the arary
 *
 * @param pos - new integer pos
 */
void ArrayList::add(int pos) {
  if ( size == arraySize ) // need to resize
    resize();
  array[size++] = pos;
  return;
}

/** 
 * @brief Resize the capacity of array 
 *
 * The capacity of array is increased by about 50%
 */
void ArrayList::resize() {
  arraySize = arraySize + arraySize/2 + 1; // increase by about 50%
  int *newarray = new int[arraySize];
  for ( int i = 0 ; i < size ; i++)
    newarray[i] = array[i];
  delete [] array;
  array = newarray;
  return;
}

/** 
 * @brief get the nth pos 
 *
 * @param n - index of request the integer
 * @return The value of the nth integer
 */
int ArrayList::get(int n) {
  if ( n >= size || n < 0 ) return -1;
  return array[n];
}


/** 
 * @brief TreeNode with word and pos[].
 *  
 *  This is node in the Red-Black Tree.
 *  It stores a string type word and ArrayList type integer list.
 */
class TreeNode
{
  public:
    TreeNode(wpp_t newword); //< Constructor
    ~TreeNode(); //< Destructor
    /** color of this node */
    char color;
    /** word in this node */
    string word;  
    /** ArrayList of occurence position*/
    ArrayList* poss;
    /** Left child of this node*/
    TreeNode *left;
    /** Right child of this node*/
    TreeNode *right;
    /** parent node of this node*/
    TreeNode* parent;
};

/** 
 * @brief Constructor of TreeNode 
 *
 * Set the color as red.
 * Create ArrayList poss and save the first occurance.
 * Set the word as the incoming word.
 * 
 * @param newword - wpp_t type word-pos pair
 */
TreeNode::TreeNode(wpp_t newword) {
  word = newword.word;
  poss = new ArrayList;
  poss->add(newword.pos);
  color = 'r';
  parent = NULL;
  left = NULL;
  right = NULL;
}

/** 
 * @brief Destructor of TreeNode 
 *
 * free the ArrayList poss
 * */
TreeNode::~TreeNode() {
  delete poss;
}


/** 
 * @brief implementation of Red-Black Tree 
 *
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
 * @Brief Public method to in-order print tree
 */
void RBTree::printTree() {
  aug_printTree(root);
}

/**
 * @brief Private recursive printTree method
 *
 * Print the tree in-order
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


/** 
 * @brief Constructor of RBTree 
 *
 * Not much to be done.
 * Initialize the root pointer to NULL
 */
RBTree::RBTree() {
  root = NULL;
}

/** 
 * @brief Destructor of RBTree 
 *
 * Rree all nodes
 */
RBTree::~RBTree() {
  deleteNode(root);
}

/**
 * @brief Clear the tree
 *
 * Clear the tree by free and delete all nodes.
 * Root is set to NULL
 */
void RBTree::clear() {
  if ( root ) {
    deleteNode(root);
    root = NULL;
  }
}

/** 
 * @brief Recursively delete the node and all its descendants
 *
 * @param node - node to be delete
 *
 */
void RBTree::deleteNode(TreeNode* node) {
  if ( node == NULL ) return;
  TreeNode *left, *right;
  left = node->left;
  right = node->right;
  delete node;
  deleteNode(left);
  deleteNode(right);
  return;
}

/** 
 * @brief Get the node that stores the word
 *
 * Locate and return the node that saves the occurance of the word
 * @param word - A string word that is needed to locate and return
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


/** 
 * @brief Insert a word-pos pair into the tree
 *
 * Insert new word, either add to existing or create new node
 * @param newword - wpp_t stucture with {word, pos}
 */
void RBTree::insert(wpp_t newword) {
  TreeNode *curr, *parent;
  toLowerCase(newword.word);
  curr = root;
  parent = curr;
  if ( root == NULL ) {
    root = new TreeNode(newword);
    curr = root;
  } else {
    while ( curr!= NULL ){
      parent = curr;
      if ( curr->word == newword.word ) {
        curr->poss->add(newword.pos);
        return; // only add a pos, no change in tree structure
      } else if ( curr->word < newword.word )
        curr = curr->right;
      else
        curr = curr->left;
    }
    curr = new TreeNode(newword);
    curr->parent = parent;
    if (parent->word < curr->word)
      parent->right = curr;
    else
      parent->left = curr;
  }
  balance(curr);
}

/** 
 * @brief Balance the tree by check the RB properties and rotate.
 *
 * Starting from the newly inserted or deleted node,
 * Do it recursively to ancestors until root.
 *
 * @param node The starting node to balance.
 */
void RBTree::balance(TreeNode *node) {
  // Case 1: it's the root!
  if ( node->parent == NULL ) {
    node->color = 'b';
    return;
  }

  // Case 2: parent is black, it is fine..
  if ( node->parent->color == 'b' ) {
    return;
  }

  // Case 3: parent and uncle are red, I can't be red, I need to repaint
  TreeNode* uncle = getUncle(node);
  TreeNode* gp;
  if ( uncle != NULL && uncle->color == 'r' ) {
    gp = getGparent(node);
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
    // left rotate
    gp->left = node;
    node->parent = gp;
    save_p->right = node->left;
    if ( node->left != NULL )
      node->left->parent = save_p;
    node->left = save_p;
    save_p->parent = node;
  } else if ( node == node->parent->left && gp->right == save_p ) {
    // right rotate
    gp->right = node;
    node->parent = gp;
    save_p->left = node->right;
    if ( node->right != NULL ) 
      node->right->parent = save_p;
    node->right = save_p;
    save_p->parent = node;
  } else { // if none of the 4 cases applies, you are fine.
    return;
  }
  node = save_p;

  // Case 5: parent red, uncle black, I am far from uncle
  save_p = node->parent; // p changed, gp not change
  save_p->color = 'b';
  gp->color = 'r';
  if ( save_p == gp->left) {
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

/** 
 * @brief Get the uncle node 
 * 
 * Get the unlce node of current node. 
 * Uncle node is defined as the brother of the node's parent
 *
 * @param node - this node
 * @return uncle if uncle exists, otherwise  NULL
 */
TreeNode* RBTree::getUncle(TreeNode *node) {
  TreeNode *gp = getGparent(node);
  if ( gp == NULL ) // no uncle
    return NULL;
  if ( gp->left == node->parent )
    return gp->right;
  else
    return gp->left;
}

/** 
 * @brief Get the gparent node
 *
 * Return the parent node of the node's parent
 * 
 * @param node - this node
 * @return grandparent node if exists, otherwise NULL
 */
TreeNode* RBTree::getGparent(TreeNode *node) {
  if ( node->parent )
    return node->parent->parent; 
  else
    return NULL;
}

