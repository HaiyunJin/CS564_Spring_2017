
#define DEBUG
// #define DEBUGMORE

/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include "btree.h"
#include "filescan.h"
#include "exceptions/bad_index_info_exception.h"
#include "exceptions/bad_opcodes_exception.h"
#include "exceptions/bad_scanrange_exception.h"
#include "exceptions/no_such_key_found_exception.h"
#include "exceptions/scan_not_initialized_exception.h"
#include "exceptions/index_scan_completed_exception.h"
#include "exceptions/file_not_found_exception.h"
#include "exceptions/end_of_file_exception.h"

// #include "exceptions/file_exists_exception.h"
#include "exceptions/hash_not_found_exception.h"

namespace badgerdb
{

// -----------------------------------------------------------------------------
// BTreeIndex::BTreeIndex -- Constructor
// -----------------------------------------------------------------------------

BTreeIndex::BTreeIndex(const std::string & relationName,
		std::string & outIndexName,
		BufMgr *bufMgrIn,
		const int attrByteOffset,
		const Datatype attrType)
      : bufMgr(bufMgrIn),               // initialized data field
        attributeType(attrType),
        attrByteOffset(attrByteOffset)
{
  {// outIndexName is the name of the index file
   std::ostringstream idxStr;
   idxStr << relationName << '.' << attrByteOffset;
   outIndexName = idxStr.str(); 
  } // idxStr goes out of scope here, so idxStr is automatically closed.

#ifdef DEBUG
  std::cout<<"Create a new BTreeIndex object"<<std::endl;
#endif

  // 1. set up the BTreeIndex data field
  switch  (attributeType ) {
    case INTEGER:
      leafOccupancy = INTARRAYLEAFSIZE;
      nodeOccupancy = INTARRAYNONLEAFSIZE;
      break;
    case DOUBLE:
      leafOccupancy = DOUBLEARRAYLEAFSIZE;
      nodeOccupancy = DOUBLEARRAYNONLEAFSIZE;
      break;
    case STRING:
      leafOccupancy = STRINGARRAYLEAFSIZE;
      nodeOccupancy = STRINGARRAYNONLEAFSIZE;
      break;
    default:
      std::cout<<"Unsupported data type\n";
  }

#ifdef DEBUG
    std::cout<<"Leaf Size: " << leafOccupancy<< std::endl;
    std::cout<<"NonLeaf Size: " << nodeOccupancy<< std::endl;
#endif


  // 2. check if the index file exits. open or create new
  //   if it is new, create metanode, create the root node
  try { // try read old file
#ifdef DEBU
std::cout<<"Reading old index file!!!\n";
#endif
    file = new BlobFile(outIndexName,false);
    Page *headerPage;
    headerPageNum = file->getFirstPageNo();
    bufMgr->readPage(file, headerPageNum, headerPage);
    IndexMetaInfo *metaInfo = (IndexMetaInfo*) headerPage;
//     IndexMetaInfo *metaInfo = reinterpret_cast<IndexMetaInfo *>(headerPage);
    rootPageNum = metaInfo->rootPageNo;
#ifdef DEBUG
  std::cout<<"headerPageNum: "<<headerPageNum<<std::endl;
  std::cout<<"relationName: "<<relationName<<std::endl;
  std::cout<<"metaInfo->relationName: "<<metaInfo->relationName<<std::endl;
  std::cout<<"attrByteOffset: "<<attrByteOffset<<std::endl;
  std::cout<<"metaInfo->attrByteOffset: "<<metaInfo->attrByteOffset<<std::endl;
  std::cout<<"attrType: "<<attrType<<std::endl;
  std::cout<<"metaInfo->attrType: "<<metaInfo->attrType<<std::endl;
  std::cout<<"rootPageNum: "<<rootPageNum<<std::endl;
  std::cout<<"metaInfo->rootPageNo: "<<metaInfo->rootPageNo<<std::endl;
#endif

    bufMgr->unPinPage(file, headerPageNum, false);
    if ( relationName.compare(metaInfo->relationName) != 0
        || metaInfo->attrByteOffset != attrByteOffset
        || metaInfo->attrType != attrType
       ) {
      // meta info does not match in index file, clear and return;
//       delete file;
      std::cout<<"Meta info does not match the index!\n";
      return;
    }
  } catch (FileNotFoundException e ) {
    // This is a new index file, create and contruct the new index file
#ifdef DEBUG
std::cout<<"Creating new index file!!!\n";
#endif
    Page *headerPage, *rootPage;
    file = new BlobFile(outIndexName, true);
    bufMgr->allocPage(file, headerPageNum, headerPage);
    bufMgr->allocPage(file, rootPageNum, rootPage);


    // assign index meta info
//     IndexMetaInfo *metaInfo = (IndexMetaInfo *) headerPage;
    IndexMetaInfo *metaInfo = reinterpret_cast<IndexMetaInfo *>(headerPage);
    std::copy(relationName.begin(), relationName.end(), metaInfo->relationName);
    metaInfo->attrByteOffset = attrByteOffset;
    metaInfo->attrType = attrType;
    metaInfo->rootPageNo = rootPageNum;
    // done with meta page
    bufMgr->unPinPage(file, headerPageNum, true);

    // construct root page
    if ( attributeType == INTEGER ) {
      LeafNodeInt* intRootPage = reinterpret_cast<LeafNodeInt*>(rootPage);
      intRootPage->size = 0;
      intRootPage->parentPageNo = 0;
      intRootPage->rightSibPageNo = 0;
    } else if ( attributeType == DOUBLE ) {
      LeafNodeDouble* doubleRootPage = reinterpret_cast<LeafNodeDouble*>(rootPage);
      doubleRootPage->size = 0;
      doubleRootPage->parentPageNo = 0;
      doubleRootPage->rightSibPageNo = 0;
    } else if ( attributeType == STRING ) {
      LeafNodeString* stringRootPage = reinterpret_cast<LeafNodeString*>(rootPage);
      stringRootPage->size = 0;
      stringRootPage->parentPageNo = 0;
      stringRootPage->rightSibPageNo = 0;
    } else {
      std::cout<<"Unsupported data type\n";
    }
    // done with root page
    bufMgr->unPinPage(file, rootPageNum, true);

#ifdef DEBUG
  std::cout<<"metaInfo->relationName: "<<metaInfo->relationName<<std::endl;
  std::cout<<"metaInfo->attrByteOffset: "<<metaInfo->attrByteOffset<<std::endl;
  std::cout<<"metaInfo->attrType: "<<metaInfo->attrType<<std::endl;
  std::cout<<"metaInfo->rootPageNo: "<<metaInfo->rootPageNo<<std::endl;
#endif
#ifdef DEBUG
  std::cout<<"headerPageNum: "<<headerPageNum<<std::endl;
  std::cout<<"rootPageNum: "<<rootPageNum<<std::endl;
#endif
//     bufMgr->flushFile(file);

    // build B-Tree: insert the record into the B-Tree
    buildBTree(relationName);

  }

#ifdef DEBUG
    std::cout<<"File name of index: "<<file->filename()<<std::endl;
    std::cout<<"Leaf Size: " << leafOccupancy<< std::endl;
    std::cout<<"NonLeaf Size: " << nodeOccupancy<< std::endl;
#endif
}


// -----------------------------------------------------------------------------
// BTreeIndex::buildBTree -- build the initial tree from relation file
// -----------------------------------------------------------------------------

const void BTreeIndex::buildBTree(const std::string & relationName)
{
    FileScan fscan(relationName, bufMgr);
    try {
      RecordId scanRid;
      while(1)
      {
        fscan.scanNext(scanRid);
        //Assuming RECORD.i is our key, lets extract the key, which we know is
        //INTEGER and whose byte offset is also know inside the record. 
        std::string recordStr = fscan.getRecord();
        const char *record = recordStr.c_str();
        void *key = (void *)(record + attrByteOffset);
        insertEntry(key, scanRid);
#ifdef DEBUGMORE
    if ( attributeType == INTEGER ) {
      std::cout << "Extracted : " << *((int*)key) << std::endl;
    }
    else if ( attributeType == DOUBLE ) {
      std::cout << "Extracted : " << *((double*)key) << std::endl;
    }
    else if ( attributeType == STRING ) {
      std::cout << "Extracted : " << ((char*)key) << std::endl;
    } else {
      std::cout<<"Unsupported data type\n";
    }
#endif
      }
    } catch(EndOfFileException e) {
      std::cout << "Read all records" << std::endl;
      std::cout << "BTree initialized" << std::endl;
    }

#ifdef DEBUG
    if ( attributeType == INTEGER ) {
      printTree<int, struct NonLeafNodeInt, struct LeafNodeInt>();
    }
    else if ( attributeType == DOUBLE ) {
      printTree<double, struct NonLeafNodeDouble, struct LeafNodeDouble>();
    }
    else if ( attributeType == STRING ) {
      printTree<char[STRINGSIZE], struct NonLeafNodeString, struct LeafNodeString>();
    } else {
      std::cout<<"Unsupported data type\n";
    }
#endif

}

// -----------------------------------------------------------------------------
// BTreeIndex::~BTreeIndex -- destructor
// -----------------------------------------------------------------------------

BTreeIndex::~BTreeIndex()
{
    //  clean up any state varibles ,
    //  unpinning pages
    //  flush the index file
    //  delete the index file
  
    // delete the rkpair in the BTree  

    scanExecuting = false;
    // unpin
    bufMgr->flushFile(file);
    delete file;
}

// -----------------------------------------------------------------------------
// BTreeIndex::insertEntry
// -----------------------------------------------------------------------------

const void BTreeIndex::insertEntry(const void *key, const RecordId rid) 
{
  // if root is leaf, it has page num of 2
  bool rootIsLeaf = rootPageNum == 2;
  if ( rootIsLeaf ) {
    // insert into the root node, also leaf node
    if ( attributeType == INTEGER ) {
//       RIDKeyPair<int> *rkpair = new RIDKeyPair<int>();
      RIDKeyPair<int> rkpair;
      rkpair.set(rid, *(int*)key);
#ifdef DEBUGMORE
 std::cout<<" Int key to insert is "<< *(int*)key<<std::endl;
#endif
      insertLeafNode<int, struct NonLeafNodeInt, struct LeafNodeInt>(rootPageNum, rkpair);
    } else if ( attributeType == DOUBLE ) {
      RIDKeyPair<double> rkpair;
      rkpair.set(rid, *(double*)key);
#ifdef DEBUGMORE
 std::cout<<" double key to insert is "<< *(double*)key<<std::endl;
#endif
      insertLeafNode<double, struct NonLeafNodeDouble, struct LeafNodeDouble>(rootPageNum, rkpair);
    } else if ( attributeType == STRING ) {
      RIDKeyPair<char[STRINGSIZE]> rkpair;
      strncpy(rkpair.key, (char*)key, STRINGSIZE);
      rkpair.rid = rid;
#ifdef DEBUGMORE
 std::cout<<" string key to insert is "<< (char*)key <<std::endl;
#endif
      insertLeafNode<char[STRINGSIZE], struct NonLeafNodeString, struct LeafNodeString>(rootPageNum, rkpair);
    } else {
      std::cout<<"Unsupported data type\n";
    }
  } else  {
    // determine the type, find the leaf node to insert, and insert
    if ( attributeType == INTEGER ) {
        RIDKeyPair<int> rkpair;
        int intKey = *(int*)key;
        rkpair.set(rid, intKey);
        PageId leafToInsert = 
          findLeafNode<int, struct NonLeafNodeInt>(rootPageNum, intKey);
        insertLeafNode<int, struct NonLeafNodeInt, struct LeafNodeInt>(leafToInsert, rkpair);
    } else if ( attributeType == DOUBLE ) {
        RIDKeyPair<double> rkpair;
        double doubleKey = *(double*)key;
        rkpair.set(rid, doubleKey);
        PageId leafToInsert =
          findLeafNode<double, struct NonLeafNodeDouble>(rootPageNum,doubleKey);
        insertLeafNode<double, struct NonLeafNodeDouble, struct LeafNodeDouble>(leafToInsert, rkpair);
    } else if ( attributeType == STRING ) {
        RIDKeyPair<char[STRINGSIZE]> rkpair;
        strncpy(rkpair.key, (char*)key, STRINGSIZE);
        rkpair.rid = rid;
        PageId leafToInsert = 
          findLeafNode<char[STRINGSIZE], struct NonLeafNodeString>(rootPageNum,rkpair.key);
        insertLeafNode<char[STRINGSIZE], struct NonLeafNodeString, struct LeafNodeString>(leafToInsert, rkpair);
    } else {
        std::cout<<"Unsupported data type\n";
    }
  }

}


// -----------------------------------------------------------------------------
// BTreeIndex::findLeafNode
// -----------------------------------------------------------------------------

template<class T, class T_NonLeafNode>
const PageId BTreeIndex::findLeafNode(PageId pageNo, const T &key)
{
  // return self if it is a leaf node. but how do I know myself is leaf?
  // Good question
 
  if ( rootPageNum == 2 ) { // root is leaf
#ifdef DEBUG
  std::cout << " ROOT IS LEAF!!!!!!" << std::endl;
#endif
    return rootPageNum; 
  }

  // pageNo should points to a non-leaf node page
  Page *tempPage;
  bufMgr->readPage(file, pageNo, tempPage);
  T_NonLeafNode* rootPage = reinterpret_cast<T_NonLeafNode*>(tempPage);
  
  int index = nodeOccupancy-1; // assume last page
  for ( int i = 0 ; i < nodeOccupancy ; ++i ) {
    if ( key < rootPage->keyArray[i] ) {
      index = i;
      break;
    }
  }

#ifdef DEBUGMORE
  std::cout<< "  index in this non-leaf node to insert is " << index << std::endl;
#endif

  PageId leafNode;
  if ( rootPage->level == 0 ) { // next level is non-leaf node
    PageId nextPage = rootPage->pageNoArray[index];
    bufMgr->unPinPage(file, pageNo, false);
    leafNode = findLeafNode<T, T_NonLeafNode>(nextPage, key);
  } else { // next level is leaf node
    leafNode = rootPage->pageNoArray[index];
    bufMgr->unPinPage(file, pageNo, false);
  }
#ifdef DEBUGMORE
  std::cout << " Leaf find by findLeafNode to insert is "<< leafNode<< std::endl;
#endif

  return leafNode;

}



// -----------------------------------------------------------------------------
// BTreeIndex::insertLeafNode
// -----------------------------------------------------------------------------

template<class T, class T_NonLeafNode, class T_LeafNode> 
const void BTreeIndex::insertLeafNode(PageId pageNo, RIDKeyPair<T> rkpair)
{
  // root and leaf do not have to be at least half full

  Page *tempPage; // temperary point, can be reused after cast
  bufMgr->readPage(file, pageNo, tempPage);
  T_LeafNode * thisPage = reinterpret_cast<T_LeafNode*>(tempPage);
  T thisKey;
  copyKey((void*)(&thisKey),(void *)(&(rkpair.key)));
#ifdef DEBUGMORE
  std::cout<<"After copy key, the new value of thisKey is ";
  std::cout<<thisKey<<" and it should be "<<rkpair.key<<std::endl;
#endif


  int size = thisPage->size;
#ifdef DEBUGMORE
  std::cout<<"before insert into leaf, size of the page is "<<size<<std::endl;
#endif
  if ( size < leafOccupancy ) {
#ifdef DEBUGMORE
  std::cout<<"size is smaller than "<<leafOccupancy<<", just insert"<<std::endl;
#endif
    // Still have space, just insert into correct position
    int index = 0;
    // find the index that this record should be
    while ( index <size && thisKey > thisPage->keyArray[index] ) ++index;
#ifdef DEBUGMORE
  std::cout<<"Afte search, the index to insert is "<<index<<std::endl;
#endif

    // shift all keys right by 1
    for ( int i = size ; i > index ; --i ) {
      copyKey((void *)(&(thisPage->keyArray[i])), (void *)(&(thisPage->keyArray[i-1])));
      thisPage->ridArray[i] = thisPage->ridArray[i-1];
    }

    // insert the current key
    copyKey((void *)(&(thisPage->keyArray[index])), (void *)(&thisKey));
#ifdef DEBUGMORE
  std::cout<<"After copy key, the new value of thisPage->keyArray[index] is ";
  std::cout<<thisPage->keyArray[index]<<" and it should be "<<thisKey<<std::endl;
#endif

    thisPage->ridArray[index] = rkpair.rid;

    // increase the size by 1;
    (thisPage->size)++;
    bufMgr->unPinPage(file, pageNo, true);
  } else {
#ifdef DEBUGMORE
  std::cout<<"size is larger than "<<leafOccupancy<<", need split"<<std::endl;
#endif
    // not enough space, need to split

    // determine where this rkpair should go
    int midIndex = leafOccupancy/2 + leafOccupancy%2;
    bool insertLeftNode = thisKey < thisPage->keyArray[midIndex];
#ifdef DEBUG
  std::cout<<"insertLeftNode?? "<<insertLeftNode<<std::endl;
#endif

    // close the page to split
    bufMgr->unPinPage(file, pageNo, false);

    // split the node and get the newly allocated PageId
    PageId rightPageNo = splitLeafNode<T, T_NonLeafNode, T_LeafNode>(pageNo);
#ifdef DEBUG
  std::cout<<"rightPageNo is "<<rightPageNo<<std::endl;
#endif

    // insert to proper page
    PageId pageNoToInsert = pageNo;
    if ( !insertLeftNode ) // insert second page
      pageNoToInsert = rightPageNo;
    insertLeafNode<T, T_NonLeafNode, T_LeafNode>(pageNoToInsert, rkpair);

  }
}


// -----------------------------------------------------------------------------
// BTreeIndex::copyKey
// -----------------------------------------------------------------------------

// const void BTreeIndex::copyKey( Page * dest, int desIndex, 
//                                 Page * scr, int scrIndex)

const void BTreeIndex::copyKey( void * dest, void * scr)
{
  if ( attributeType == INTEGER ) {
    int *intDest = (int*)dest;
    *intDest = *((int*)scr);
  } else if ( attributeType == DOUBLE ) {
    double *doubleDest = (double*)dest;
    *doubleDest =  *((double*)scr);
  } else if ( attributeType == STRING ) {
    strncpy((char*)dest, (char*)scr, STRINGSIZE);
  }
}
// further test:  use strncpy for all (  sizeof(int) )



// -----------------------------------------------------------------------------
// BTreeIndex::splitLeafNode
// -----------------------------------------------------------------------------

template<class T, class T_NonLeafNode, class T_LeafNode>
const PageId BTreeIndex::splitLeafNode(PageId pageNo)
{
    Page *tempPage; // can be reused
    PageId firstPageNo = pageNo;
    bufMgr->readPage(file, firstPageNo, tempPage);
    T_LeafNode* firstPage = reinterpret_cast<T_LeafNode*>(tempPage);
    PageId secondPageNo;
#ifdef DEBUG
  std::cout<<"secondPageNo before init is "<<secondPageNo<<std::endl;
#endif
    bufMgr->allocPage(file, secondPageNo, tempPage);
#ifdef DEBUG
  std::cout<<"secondPageNo after init is "<<secondPageNo<<std::endl;
#endif
    T_LeafNode* secondPage = reinterpret_cast<T_LeafNode*>(tempPage);



#ifdef DEBUG
  if ( secondPageNo > 1000 ) {
    std::cout<<" Second page not set properly"<<std::endl;
    exit(1);
  }
  std::cout<<std::endl;
  std::cout<<"   --------------------------- Split Leaf Node, this node No is "<< firstPageNo<<std::endl;
  std::cout<<"   ---------------------------  new node no is "<<secondPageNo<<std::endl;
  std::cout<<std::endl;
#endif


    // set the parent properly
    secondPage->parentPageNo = firstPage->parentPageNo;

    // set the sibling properly
    secondPage->rightSibPageNo = firstPage->rightSibPageNo;
    firstPage->rightSibPageNo = secondPageNo;

    // middle one, as the new key in parent
    // firstPage has more keys if odd size
    // copy the second half keys to secondPage
    int midIndex = leafOccupancy/2 + leafOccupancy%2;
    for ( int i1=midIndex, i2=0 ; i1 < leafOccupancy ; ++i1,++i2) {
      copyKey((void *)(&(secondPage->keyArray[i2])),
              (void *)(&( firstPage->keyArray[i1])));
      secondPage->ridArray[i2] = firstPage->ridArray[i1];
    }
    firstPage->size = midIndex;
    secondPage->size = leafOccupancy - midIndex;

    T copyUpKey;
    copyKey((void*)(&copyUpKey), (void*)(&(firstPage->keyArray[midIndex])));
#ifdef DEBUGMORE
  std::cout<<"After copy key, the new value of copyUpKey is ";
  std::cout<<copyUpKey<<" and it should be "<<firstPage->keyArray[midIndex]<<std::endl;
#endif

//     bool rootIsLeaf = firstPage->parentPageNo < 1;
    bool rootIsLeaf = rootPageNum == pageNo;

    // done with first and second page
    bufMgr->unPinPage(file, firstPageNo, true);
    bufMgr->unPinPage(file, secondPageNo, true);

    // check if here is parent
    //
    // I think, as long as the parent page no is not -1, it is not root 
    // but we can do extra check, TODO
    if ( !rootIsLeaf ) { // normal parent, insert new key and pageNo
      insertNonLeafNode<T, T_NonLeafNode>(firstPage->parentPageNo, copyUpKey,secondPageNo);
    } else {  // it is the root leaf node
      // allocate new parent node
      PageId parentPageNo;
      bufMgr->allocPage(file, parentPageNo, tempPage);
      T_NonLeafNode* parentPage = reinterpret_cast<T_NonLeafNode*>(tempPage);

      // update in class field
      rootPageNum = parentPageNo;
      // update the meta info
      bufMgr->readPage(file, headerPageNum, tempPage);
      IndexMetaInfo* metaPage = reinterpret_cast<IndexMetaInfo*>(tempPage);
      metaPage->rootPageNo = parentPageNo;
      bufMgr->unPinPage(file, headerPageNum, true);

      // update child pages' parent
      firstPage->parentPageNo = parentPageNo;
      secondPage->parentPageNo = parentPageNo;

      // construct the parent node
      parentPage->level = 1;  // just above leaf
      parentPage->size = 1;   // one key, points to first and second page
//       parentPage->keyArray[0] = copyUpKey;
      copyKey((void *)(&(parentPage->keyArray[0])),(void *)(&(copyUpKey)));
#ifdef DEBUGMORE
  std::cout<<"After copy key, the new value of parentPage->keyArray[0] is ";
  std::cout<<parentPage->keyArray[0]<<" and it should be "<<copyUpKey<<std::endl;
#endif
      parentPage->pageNoArray[0] = firstPageNo;
      parentPage->pageNoArray[1] = secondPageNo;

      // done with parentPage, save changes
      bufMgr->unPinPage(file, parentPageNo, true);
    }





    // return the second page number created
    return secondPageNo;
}


// -----------------------------------------------------------------------------
// BTreeIndex::insertNonLeafNode
// -----------------------------------------------------------------------------

template<class T, class T_NonLeafNode>
// const void BTreeIndex::insertNonLeafNode(PageId pageNo, PageKeyPair<T> pkpair)
const void BTreeIndex::insertNonLeafNode(PageId pageNo, T key, PageId childPageNo)
{

    Page *tempPage;
    bufMgr->readPage(file, pageNo, tempPage);
    T_NonLeafNode* thisPage = reinterpret_cast<T_NonLeafNode*>(tempPage);
    T thisKey;
    copyKey((void*)(&thisKey), (void *)(&key));
#ifdef DEBUGMORE
  std::cout<<"After copy key, the new value of thisKey is ";
  std::cout<<thisKey<<" and it should be "<<key<<std::endl;
#endif
    int size = thisPage->size;
    if ( size < nodeOccupancy ) {
      // still have space, just insert
      int index = 0;
      // find the correct position to insert the key
      while ( thisKey > thisPage->keyArray[index] ) ++index;
      // shift all keys and pageNos right by 1 position
      for ( int i = size ; i > index ; --i ) {
        copyKey((void *)(&(thisPage->keyArray[i])), (void *)(&(thisPage->keyArray[i-1])));
        thisPage->pageNoArray[i+1] = thisPage->pageNoArray[i];
      }
      // insert the current key
      copyKey((void *)(&(thisPage->keyArray[index])), (void *)(&thisKey));
#ifdef DEBUGMORE
  std::cout<<"After copy key, the new value of thisPage->keyArray[index] is ";
  std::cout<<thisPage->keyArray[index]<<" and it should be "<<thisKey<<std::endl;
#endif
      thisPage->pageNoArray[index+1] = childPageNo;
      (thisPage->size)++;
      // done, close the file
      bufMgr->unPinPage(file, pageNo, true);
    } else {
      // need to split this non-leaf node, potentially propagate up

      // you know where to insert before split
      int midIndex = nodeOccupancy/2+nodeOccupancy%2;
      bool insertLeftNode = thisKey < thisPage->keyArray[midIndex];

      // close the file for split
      bufMgr->unPinPage(file, pageNo, false);

      // split the non-leaf node, potentially affect upwards to the root
      
      PageId rightPageNo = splitNonLeafNode<T, T_NonLeafNode>(pageNo);

      // insert to the proper page
      PageId pageNoToInsert = pageNo;
      if ( !insertLeftNode ) // insert second page
        pageNoToInsert = rightPageNo;
      insertNonLeafNode<T, T_NonLeafNode>(pageNoToInsert, key, childPageNo);
    }
    
}


// -----------------------------------------------------------------------------
// BTreeIndex::splitNonLeafNode
// -----------------------------------------------------------------------------

template<class T, class T_NonLeafNode>
const PageId BTreeIndex::splitNonLeafNode(PageId pageNo)
{
    Page *tempPage; // can be reused
    PageId firstPageNo = pageNo;
    bufMgr->readPage(file,  firstPageNo, tempPage);
    T_NonLeafNode * firstPage = reinterpret_cast<T_NonLeafNode*>(tempPage);
    PageId secondPageNo;
    bufMgr->allocPage(file, secondPageNo, tempPage);
    T_NonLeafNode* secondPage = reinterpret_cast<T_NonLeafNode*>(tempPage);


#ifdef DEBUG
  std::cout<<std::endl;
  std::cout<<"   -----NON LEAF SPLIT-------- Split NON  Leaf Node, this node No is "<< firstPageNo<<std::endl;
  std::cout<<"   ---------------------------  new node no is "<<secondPageNo<<std::endl;
  std::cout<<std::endl;
#endif


    // set the parent
    secondPage->parentPageNo = firstPage->parentPageNo;
  
    // set the level
    secondPage->level = firstPage->level;

    // middle index will be push to parent
    int midIndex = nodeOccupancy/2+nodeOccupancy%2;

    // split keyArray
    for ( int i1 = midIndex+1 , i2 = 0; i1 < nodeOccupancy; ++i1, ++i2) {
      // +1 because the midIndex key will not be copied, it will be push up
      copyKey((void *)(&(secondPage->keyArray[i2])),
              (void *)(&( firstPage->keyArray[i1])));
    }
    // split pageNoArray
    for ( int i1 = midIndex+1 , i2 = 0 ; i1 < nodeOccupancy+1 ; ++i1, ++i2) {
      secondPage->pageNoArray[i2] = firstPage->pageNoArray[i1];
    }
    firstPage->size = midIndex;
    secondPage->size = nodeOccupancy - midIndex -1; // -1 bcz mid is pushed up

    T pushUpKey;
    copyKey((void*)(&pushUpKey), (void*)(&(firstPage->keyArray[midIndex])));
#ifdef DEBUGMORE
  std::cout<<"After copy key, the new value of pushUpKey is ";
  std::cout<<pushUpKey<<" and it should be "<<firstPage->keyArray[midIndex]<<std::endl;
#endif

    bool currentNodeIsRoot = rootPageNum == firstPageNo;

    // done with this two pages
    bufMgr->unPinPage(file, firstPageNo, true);
    bufMgr->unPinPage(file, secondPageNo, true);

    if ( ! currentNodeIsRoot ) { // not root 
      // done with this two pages
      bufMgr->unPinPage(file, firstPageNo, true);
      bufMgr->unPinPage(file, secondPageNo, true);
      // insert the pushUpKey to parent
      insertNonLeafNode<T, T_NonLeafNode>(firstPage->parentPageNo, pushUpKey, secondPageNo);
    } else { // this is the root to be splited, create new root
      PageId parentPageNo;
      bufMgr->allocPage(file, parentPageNo, tempPage);
      T_NonLeafNode* parentPage = reinterpret_cast<T_NonLeafNode*>(tempPage);

      // update class fields
      rootPageNum = parentPageNo;
      // update meta info
      bufMgr->readPage(file, headerPageNum, tempPage);
      IndexMetaInfo* metaPage = reinterpret_cast<IndexMetaInfo*>(tempPage);
      metaPage->rootPageNo = parentPageNo;
      bufMgr->unPinPage(file, headerPageNum, true);


      // update child pages' parent
      firstPage->parentPageNo = parentPageNo;
      secondPage->parentPageNo = parentPageNo;
      // done with this two pages
      bufMgr->unPinPage(file, firstPageNo, true);
      bufMgr->unPinPage(file, secondPageNo, true);

      // construct the parent page
      parentPage->level = 0;
      parentPage->size = 1;
//       parentPage->keyArray[0] = pushUpKey;
      copyKey((void *)(&(parentPage->keyArray[0])),(void *)(&(pushUpKey)));
#ifdef DEBUGMORE
  std::cout<<"After copy key, the new value of parentPage->keyArray[0] is ";
  std::cout<<parentPage->keyArray[0]<<" and it should be "<<pushUpKey<<std::endl;
#endif
      parentPage->pageNoArray[0] = firstPageNo;
      parentPage->pageNoArray[1] = secondPageNo;
      
      // done with parentPage
      bufMgr->unPinPage(file, parentPageNo, true);
      
    }

#ifdef DEBUG
  std::cout<<"Root is "<<rootPageNum<<std::endl;
#endif


    // return newly alloc secondPage number
    return secondPageNo;


}

template <class T, class T_NonLeafNode, class T_LeafNode>
const void BTreeIndex::printTree() {

  Page *tempPage;
  PageId currNo = rootPageNum;

  bool rootIsLeaf = rootPageNum == 2;
  bufMgr->readPage(file, currNo, tempPage);

  int lineSize = 20;


  if ( rootIsLeaf ) {
    // print the root
    T_LeafNode *currPage = reinterpret_cast<T_LeafNode*>(tempPage);
    int size = currPage->size;
std::cout<<" Size of the root: "<<size<<std::endl;
    for ( int i = 0 ; i < size ; ++i) {
      if ( i%lineSize == 0 ) std::cout<<std::endl<<i<<": ";
      std::cout<<currPage->keyArray[i]<<" ";
    }
    std::cout<<std::endl<<" Root Leaf BTree printed"<<std::endl;
    bufMgr->unPinPage(file, currNo, false);
  } else {
    T_NonLeafNode *currPage = reinterpret_cast<T_NonLeafNode*>(tempPage);
    // travease down to the 1 level above leaf
    while ( currPage->level != 1 ) {
      PageId nextPageNo = currPage->pageNoArray[0];
      bufMgr->unPinPage(file, currNo, false);
      currNo = nextPageNo;
      bufMgr->readPage(file, currNo, tempPage);
      currPage = reinterpret_cast<T_NonLeafNode*>(tempPage);
    }

    PageId leafNo = currPage->pageNoArray[0];
    bufMgr->unPinPage(file, currNo, false);

    // print the tree
    currNo = leafNo;
    bufMgr->readPage(file, currNo, tempPage);
    T_LeafNode *currLeafPage = reinterpret_cast<T_LeafNode*>(tempPage);
    while ( 1 ) {
      int size = currLeafPage->size;
#ifdef DEBUG
  std::cout<<" Size of this node is " << size<<std::endl;
  if ( size > leafOccupancy )  {
    std::cout<<"abnormal size "<<size<<std::endl;
    std::cout<<" currNo "<< currNo<< std::endl;
    exit(0);
  }
#endif

      for ( int i = 0 ; i < size ; ++i) {
        if ( i%lineSize == 0 ) std::cout<<std::endl<<i<<": ";
        std::cout<<currLeafPage->keyArray[i]<<" ";
      }
      std::cout<<std::endl;
      bufMgr->unPinPage(file, currNo, false);
#ifdef DEBUG
  std::cout<<" next sibling page number is "<<currLeafPage->rightSibPageNo<<std::endl;
#endif
      if ( currLeafPage->rightSibPageNo == 0 ) break;
      currNo = currLeafPage->rightSibPageNo;
      bufMgr->readPage(file, currNo, tempPage);
      currLeafPage = reinterpret_cast<T_LeafNode*>(tempPage);
    }
    std::cout<<std::endl<<" BTree printed"<<std::endl;
    bufMgr->unPinPage(file, currNo, false);
  }
  

}


// -----------------------------------------------------------------------------
// BTreeIndex::startScan
// -----------------------------------------------------------------------------

const void BTreeIndex::startScan(const void* lowValParm,
				   const Operator lowOpParm,
				   const void* highValParm,
				   const Operator highOpParm)
{
  if ( scanExecuting == true ) {
    // error, only one scan at a time
    std::cout<< "Scan Already started\n";
  } else {
    scanExecuting = true;
  }


//   /**
//    * True if an index scan has been started.
//    */
// 	bool		scanExecuting;
// 
//   /**
//    * Index of next entry to be scanned in current leaf being scanned.
//    */
// 	int		nextEntry;
// 
//   /**
//    * Page number of current page being scanned.
//    */
// 	PageId	currentPageNum;
// 
//   /**
//    * Current Page being scanned.
//    */
// 	Page		*currentPageData;
// 
//   /**
//    * Low INTEGER value for scan.
//    */
// 	int		lowValInt;
// 
//   /**
//    * Low DOUBLE value for scan.
//    */
// 	double	lowValDouble;
// 
//   /**
//    * Low STRING value for scan.
//    */
// 	std::string	lowValString;
// 
//   /**
//    * High INTEGER value for scan.
//    */
// 	int		highValInt;
// 
//   /**
//    * High DOUBLE value for scan.
//    */
// 	double	highValDouble;
// 
//   /**
//    * High STRING value for scan.
//    */
// 	std::string highValString;
// 	
//   /**
//    * Low Operator. Can only be GT(>) or GTE(>=).
//    */
// 	Operator	lowOp;
// 
//   /**
//    * High Operator. Can only be LT(<) or LTE(<=).
//    */
// 	Operator	highOp;






}

// -----------------------------------------------------------------------------
// BTreeIndex::scanNext
// -----------------------------------------------------------------------------

const void BTreeIndex::scanNext(RecordId& outRid) 
{
  // make sure the scan state is true;
  if ( scanExecuting == false) {
    std::cout<<"No scann started\n";
    return;
  }



  // if scan reach the end of the list
//   throw IndexScanCompletedException;
}

// -----------------------------------------------------------------------------
// BTreeIndex::endScan
// -----------------------------------------------------------------------------
//
const void BTreeIndex::endScan() 
{
  // change the scan state.
  // if there isn't as startScan
  if ( scanExecuting == false ) {
    throw ScanNotInitializedException();
  } else {
    scanExecuting = false;
  }

  // end the scan, unpin all pages have been pinned
  //

}

}


