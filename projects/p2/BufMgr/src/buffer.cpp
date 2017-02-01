/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include <memory>
#include <iostream>
#include "buffer.h"
#include "exceptions/buffer_exceeded_exception.h"
#include "exceptions/page_not_pinned_exception.h"
#include "exceptions/page_pinned_exception.h"
#include "exceptions/bad_buffer_exception.h"
#include "exceptions/hash_not_found_exception.h"

namespace badgerdb { 

BufMgr::BufMgr(std::uint32_t bufs)
	: numBufs(bufs) {
	bufDescTable = new BufDesc[bufs];

  for (FrameId i = 0; i < bufs; i++) 
  {
  	bufDescTable[i].frameNo = i;
  	bufDescTable[i].valid = false;
  }

  bufPool = new Page[bufs];

  int htsize = ((((int) (bufs * 1.2))*2)/2)+1;
  hashTable = new BufHashTbl (htsize);  // allocate the buffer hash table

  clockHand = bufs - 1;
}


BufMgr::~BufMgr() {
  // 1. Flush all dirty pages to disk
  // 2. deallocate buffer pool
  // 3. deallocate BufDesc table

  // 1. TODO
  // 2.
  delete hashTable; // ?? not mentioned in the pdf
  delete bufPool;
  // 3.
  delete bufDescTable;


}

void BufMgr::advanceClock()
{ // advance the clock hand to the next frame in the buffer pool
  clockHand = (clockHand+1)%bufs;
}

void BufMgr::allocBuf(FrameId & frame) 
{
  // |-->Advance clock
  // |     |
  // |     v
  // |    check valid bit --------------------------------------|
  // |     | Yes                      No                        |
  // |Yes  v                                                    |
  // |-<- check refbit                                          |
  // |     | No                                                 |
  // |Yes  v                                                    |
  // -< page pinned?                                            |
  //       | No                                                 |
  //       v                                                    |
  //    dirty bit set? --> if yes, flush to disk                |
  //       | No                     |                           |
  //       v                        v                           |
  //    call set() <----------------J---------------------------J
  //       |
  //       v
  //    Use it.
  /**
   * frames that checked
   */
  std::uint32_t numPinned = 0 ;

  while ( numPinned < numBufs ) {
    advanceClock();
    if ( ! bufDescTable[clockHand]->valid ) 
      break;
    if ( bufDescTable[clockHand]->refbit )
      continue;
    if ( bufDescTable[clockHand]->pinCnt > 0 ){
      numPinned++;
      continue;
    }
    if ( bufDescTable[clockHand]->dirty ) {
      // flush into disk, then
      break;
    }
  }
  if ( numPinned == numBufs ) 
    throw BufferExceededException();

  frame = bufDescTable[clockHand].frameNo; //  Id == No ?? YES
  
  // remove the entry if the frame is valid before
  if ( bufDescTable[clockHand]->valid ) {
    hashTable->remove(bufDescTable[clockHand]->file,
                      bufDescTable[clockHand]->pageNo);
  }
}

	
void BufMgr::readPage(File* file, const PageId pageNo, Page*& page)
{
  FrameId frameNo;
  if ( hashTable->lookup(file, pageNo, frameNo)) { // found
    bufDescTable[frameNo]->refbit = true;
    bufDescTable[frameNo]->pinCnt++;
  } else { // not found
    allocBuf(frameNo);
    *bufPool[frameNo] = file->readPage(pageNo);
    hashTable->insert(file, pageNo, frameNo);
    bufDescTable[frameNo]->Set(file, pageNo);
  }
  page = bufPool[frameNo];
}


void BufMgr::unPinPage(File* file, const PageId pageNo, const bool dirty) 
{
}

void BufMgr::flushFile(const File* file) 
{
}

void BufMgr::allocPage(File* file, PageId &pageNo, Page*& page) 
{
}

void BufMgr::disposePage(File* file, const PageId PageNo)
{
    
}

void BufMgr::printSelf(void) 
{
  BufDesc* tmpbuf;
	int validFrames = 0;
  
  for (std::uint32_t i = 0; i < numBufs; i++)
	{
  	    tmpbuf = &(bufDescTable[i]);
		std::cout << "FrameNo:" << i << " ";
		tmpbuf->Print();

  	if (tmpbuf->valid == true)
    	validFrames++;
  }

	std::cout << "Total Number of Valid Frames:" << validFrames << "\n";
}

}
