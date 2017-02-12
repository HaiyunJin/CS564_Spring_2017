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

// std::cout<<"before flush\n";
  // 1. Flush all dirty pages to disk
  for ( std::uint32_t i = 0 ; i < numBufs; ++i ) {
    if (bufDescTable[i].dirty) {
      bufDescTable[i].file->writePage(bufPool[i]);
    }
  }

// std::cout<<"flushed\n";
  // 2. deallocate buffer pool
  delete hashTable; // ?? not mentioned in the pdf
  delete [] bufPool;
  // 3. deallocate BufDesc table
  delete [] bufDescTable;


}

void BufMgr::advanceClock()
{ // advance the clock hand to the next frame in the buffer pool
  clockHand = (clockHand+1)%numBufs;
}

// TOBETEST BufMgr::allocBuf
void BufMgr::allocBuf(FrameId & frame) 
{
  //    |------->Advance clock
  //    |          |
  //    |          v
  //    |         check valid bit --------------------------------------|
  //    |          | Yes                      No                        |
  //    |clear,Y   v                                                    |
  //    |------<- check refbit                                          |
  //    |          | No                                                 |
  //    |     Yes  v                                                    |
  //    ----- < page pinned?                                            |
  //               | No                                                 |
  //               v                                                    |
  //            dirty bit set? --> if yes, flush to disk                |
  //               | No                     |                           |
  //               v                        v                           |
  //            call set() <----------------J---------------------------J
  //               |
  //               v
  //            Use it.
  /**
   * frames that checked
   */
  BufDesc* tmpbuf;
  std::uint32_t i = 0 ;

  // Scan two rounds for replacable buffer
  for ( i  = 0 ; i < numBufs*2; ++i ) {
    advanceClock();
    tmpbuf = &bufDescTable[clockHand];
    if ( ! tmpbuf->valid ) 
      goto found;
    if ( tmpbuf->refbit ) {
      tmpbuf->refbit = false; // clear the refbit
      continue;
    }
    if ( tmpbuf->pinCnt > 0 )
      continue;

    if ( tmpbuf->dirty )  // flush into disk
      tmpbuf->file->writePage(bufPool[clockHand]);

    // remove the entry if the frame is valid before
    hashTable->remove(tmpbuf->file, tmpbuf->pageNo);
    goto found;
  }
  
  throw BufferExceededException();

found:
  frame = tmpbuf->frameNo; //  Id == No ?? YES
  tmpbuf->Clear();
}

// TOBETEST BufMgr::readPage
void BufMgr::readPage(File* file, const PageId pageNo, Page*& page)
{
  FrameId frameNo;
  if ( hashTable->lookup(file, pageNo, frameNo) ) { // found
    bufDescTable[frameNo].refbit = true;
    bufDescTable[frameNo].pinCnt++;
  } else { // not found
    allocBuf(frameNo);
    bufPool[frameNo] = file->readPage(pageNo);
    hashTable->insert(file, pageNo, frameNo);
    bufDescTable[frameNo].Set(file, pageNo);
  }
  page = &bufPool[frameNo];
}

// TOBETEST BufMgr::unPinPage
void BufMgr::unPinPage(File* file, const PageId pageNo, const bool dirty) 
{
  FrameId frameNo;
  if ( hashTable->lookup(file, pageNo, frameNo)) { // found
    if ( dirty ) bufDescTable[frameNo].dirty = true;
    if ( bufDescTable[frameNo].pinCnt > 0 ) {
      bufDescTable[frameNo].pinCnt--;
    } else {
      throw PageNotPinnedException(file->filename(), pageNo, frameNo);
    }
  }

}

// TOBETEST BufMgr::flushFile
void BufMgr::flushFile(const File* file) 
{

  BufDesc* tmpbuf;
  for (std::uint32_t i = 0 ; i < numBufs ; ++i ) {
    tmpbuf = &bufDescTable[i];
    if ( tmpbuf->file == file ) { //Is this the way to check if belong to same file?
      if ( tmpbuf->pinCnt > 0 )  {
        throw PagePinnedException(file->filename(), 
                                tmpbuf->pageNo,
                                tmpbuf->frameNo);
      }
      if ( ! tmpbuf->valid ) {
          throw BadBufferException(tmpbuf->frameNo,
                                   tmpbuf->dirty,
                                   tmpbuf->valid,
                                   tmpbuf->refbit);
      }
      if ( tmpbuf->dirty )
        tmpbuf->file->writePage(bufPool[i]);
      hashTable->remove(tmpbuf->file, tmpbuf->pageNo);
      tmpbuf->Clear();

    }
  }

}

// TOBETEST BufMgr::allocPage
void BufMgr::allocPage(File* file, PageId &pageNo, Page*& page) 
{
  FrameId frameNo;
  allocBuf(frameNo);
  bufPool[frameNo] = file->allocatePage();
  pageNo = bufPool[frameNo].page_number();
// fprintf(stderr, "info: %s +%d %s\n", __FILE__, __LINE__, __func__);

  hashTable->insert(file, pageNo, frameNo);
  bufDescTable[frameNo].Set(file, pageNo);
  page = &bufPool[frameNo];
}

// TOBETEST BufMgr::disposePage  
// I think there is a problem with the file->deletePage() method
void BufMgr::disposePage(File* file, const PageId PageNo)
{
  FrameId frameNo;
  if ( hashTable->lookup(file, PageNo, frameNo) ) {
    BufDesc* tmpbuf = &bufDescTable[frameNo];
    hashTable->remove(tmpbuf->file, tmpbuf->pageNo);
    tmpbuf->Clear();
  }
//   std::cout<<" just before delete\n";
  file->deletePage(PageNo);
//   std::cout<<" after delete\n";

}

// TOBETEST BufMgr::printSelf
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
