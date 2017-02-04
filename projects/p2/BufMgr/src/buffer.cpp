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
  for ( std::uint32_t i = 0 ; i < numBufs; ++i ) {
    if (bufDescTable[i].dirty) {
      bufDescTable[i].file->writePage(bufPool[i]);
    }
  }
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

  for ( i  = 0 ; i < numBufs*2; ++i ) {
    advanceClock();
    tmpbuf = &bufDescTable[clockHand];
    if ( ! tmpbuf->valid ) 
      goto invalid;
    if ( tmpbuf->refbit ) {
      tmpbuf->refbit = false; // clear the refbit
      continue;
    }
    if ( tmpbuf->pinCnt > 0 )
      continue;

    if ( tmpbuf->dirty )  // flush into disk
      tmpbuf->file->writePage(bufPool[clockHand]);
    goto replace;
  }
  
  throw BufferExceededException();

replace:
  // remove the entry if the frame is valid before
  hashTable->remove(tmpbuf->file, tmpbuf->pageNo);

invalid:
  frame = tmpbuf->frameNo; //  Id == No ?? YES
  tmpbuf->Clear();
}


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

void BufMgr::flushFile(const File* file) 
{

  BufDesc* tmpbuf;
  for (unsigned int i = 0 ; i < numBufs ; ++i ) {
    tmpbuf = &bufDescTable[i];
    if ( tmpbuf->file == file ) {
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

void BufMgr::allocPage(File* file, PageId &pageNo, Page*& page) 
{
  FrameId frameNo;
  Page new_page;
//  std::cout<<" Allocate page in bufmgr, call file->allocatepage\n";
  new_page = file->allocatePage();
//  std::cout<<" Allocate page succeuss\n";
  pageNo = new_page.page_number();

// fprintf(stderr, "info: %s +%d %s\n", __FILE__, __LINE__, __func__);
  allocBuf(frameNo);
// fprintf(stderr, "info: %s +%d %s\n", __FILE__, __LINE__, __func__);
  bufPool[frameNo] = new_page;

  hashTable->insert(file, pageNo, frameNo);
  bufDescTable[frameNo].Set(file, pageNo);
  page = &bufPool[frameNo];
}

void BufMgr::disposePage(File* file, const PageId PageNo)
{
  // TODO: error check is not finished
  FrameId frameNo;
  if ( hashTable->lookup(file, PageNo, frameNo) ) {
    file->deletePage(PageNo);
    bufDescTable[frameNo].Clear();
  }

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
