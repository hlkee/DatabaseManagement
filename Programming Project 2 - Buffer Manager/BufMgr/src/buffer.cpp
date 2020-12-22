/**
 *@author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 *@section LICENSE
 *Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

/**
* Emilie Rajka erajka 9077672112
* Jacqueline McNutt jmcnutt 9077517283
* Hock Lye Kee kee3 9080308456
* Purpose statement: Manages frames of the buffer pool
  and the pages that reside within them using the clock
  algorithm and hashtable 
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
    //----------------------------------------
    // Constructor of the class BufMgr
    //----------------------------------------
    
    BufMgr::BufMgr(std::uint32_t bufs): numBufs(bufs) {
        bufDescTable = new BufDesc[bufs];
    
        for (FrameId i = 0; i < bufs; i++) {
            bufDescTable[i].frameNo = i;
            bufDescTable[i].valid = false;
        }
    
        bufPool = new Page[bufs];
    
        int htsize = ((((int)(bufs * 1.2)) * 2) / 2) + 1;
        hashTable = new BufHashTbl(htsize); // allocate the buffer hash table
    
        clockHand = bufs - 1;
    }
    
    BufMgr::~BufMgr() {
        // cycle through all the buffer pool to look for
        // dirty pages to flush to disk
        BufDesc * tmpbuf;
        for (std::uint32_t i = 0; i < numBufs; i++) {
            tmpbuf = & (bufDescTable[i]);
            if (tmpbuf -> valid == true && tmpbuf -> dirty == true) {
                tmpbuf -> file -> writePage(bufPool[i]);
            }
        }
    
        //delete the buffer pool and bufdesc table
        delete hashTable;
        delete[] bufDescTable;
        delete[] bufPool;
    }
    
    void BufMgr::advanceClock() {
        clockHand += 1;
    
        //To make sure clockHand is not > numBufs
        clockHand = clockHand % numBufs;
    
    }
    
    void BufMgr::allocBuf(FrameId & frame) {
        // Loops through the bufPool
        uint32_t counter = 0; // Make sure within the number of frames
        uint32_t pinCounter = 0; // To check if all the frames are pinned
    
        while (counter <= numBufs) {
            counter++;
            //1) Checks if its valid
            if (bufDescTable[clockHand].valid == false) {
                // Set the frame
                frame = clockHand;
                bufDescTable[clockHand].Clear();
                break;
            }
    
            //2) Checks the ref bit
            if (bufDescTable[clockHand].refbit == true) {
                bufDescTable[clockHand].refbit = false;
                advanceClock();
                continue;
            }
    
            //3) Checks if its pinned
            if (bufDescTable[clockHand].pinCnt > 0) {
                pinCounter++;
                advanceClock();
                continue;
            }
    
            //This frame is available
            //4) Flush the file in case its dirty
            // will remove entry from hashtable 
            // if valid
            flushFile(bufDescTable[clockHand].file);
    
            //5) Set the frame
            frame = clockHand;
            break;
        }
    
        // Throw a BufferExceededException if all frames are pinned
        if (pinCounter >= numBufs) {
            throw BufferExceededException();
        }
    
    }
    
    void BufMgr::readPage(File * file,
                          const PageId pageNo, Page * & page) {
        FrameId frameNo;
        try {
            // Check if the file, pageNo is in the buffer pool
            hashTable -> lookup(file, pageNo, frameNo);
    
            // if no exception: page is in buffer pool
            bufDescTable[frameNo].refbit = 1;
            bufDescTable[frameNo].pinCnt += 1;
    
        } catch (HashNotFoundException & e) {
            // add to buf pool if not found
            allocBuf(frameNo);
            bufPool[frameNo] = file -> readPage(pageNo);
    
            // Insert entry to ht and set frame
            hashTable -> insert(file, pageNo, frameNo);
            bufDescTable[frameNo].Set(file, pageNo);
        }
    
        // return page
        page = & bufPool[frameNo];
    
    }
    
    void BufMgr::unPinPage(File * file,
                           const PageId pageNo,
                           const bool dirty) {
        //Variable to store FrameID
        FrameId temp;
    
        //Lookup the frame number in the buffer manager using hash table
        hashTable -> lookup(file, pageNo, temp);
    
        //Look in the bufDescTable to check the pin
        //Throws exception if pinCnt is 0, decrement pinCnt otherwise
        if (bufDescTable[temp].pinCnt < 1) {
            throw PageNotPinnedException(file -> filename(), pageNo, temp);
        } else {
            bufDescTable[temp].pinCnt--;
        }
    
        //Mark the page as dirty if its dirty
        if (dirty == true) {
            bufDescTable[temp].dirty = true;
        }
    }
    
    void BufMgr::allocPage(File * file, PageId & pageNo, Page * & page) {
        // add a page to the file and get the pageNo
        Page new_page = file -> allocatePage();
        //file.writePage(new_page);
        pageNo = new_page.page_number();
    
    
        // obtain buffer pool frame
        FrameId new_frame;
        allocBuf(new_frame);
    
        // Insert entry to ht and set frame
        hashTable -> insert(file, pageNo, new_frame);
        bufPool[new_frame] = new_page; // copy page to bufPool
        bufDescTable[new_frame].Set(file, pageNo);
    
        // pointer to buffer frame with page
        page = & bufPool[new_frame]; // assign pointer to page?
    
    }
    
    void BufMgr::flushFile(const File * file) {
        BufDesc * tmpbuf;
        // cycle through the buffer Desc Table
        for (std::uint32_t i = 0; i < numBufs; i++) {
            tmpbuf = & (bufDescTable[i]);
            // check which pages belong to the file
            if (tmpbuf -> file == file) {
                // throws PagePinnedException when page is pinned
                if (tmpbuf -> pinCnt > 0) {
                    throw PagePinnedException(file -> filename(), tmpbuf -> pageNo, tmpbuf -> frameNo);
                }
                // throws BadBufferException if an invalid page is encountered
                if (tmpbuf -> valid == false) {
                    throw BadBufferException(tmpbuf -> frameNo, tmpbuf -> dirty, tmpbuf -> valid, tmpbuf -> refbit);
                }
                // if the page is dirty call file.writePage() then set dirtybit to false
                if (tmpbuf -> dirty == true) {
                    tmpbuf -> file -> writePage(bufPool[i]); // need to check params for writePage()
                    tmpbuf -> dirty = 0;
                }
                // then remove the page from the hashtable and call Clear() in BufDesc
                hashTable -> remove(file, tmpbuf -> pageNo);
                tmpbuf -> Clear();
            }
        }
    }
    
    void BufMgr::disposePage(File * file,
                             const PageId PageNo) {
        FrameId FrameNo = numBufs;
        // first look for page by # then see if that page is
        // allocated a frame in the buffer pool,
        hashTable -> lookup(file, PageNo, FrameNo);
        if (FrameNo != numBufs) {
            // If so, free it and remove if exists
            bufDescTable[FrameNo].Clear();
            hashTable -> remove(file, PageNo);
        }
        // deletes a particular page from the file
        file -> deletePage(PageNo);
    }
    
    void BufMgr::printSelf(void) {
        BufDesc * tmpbuf;
        int validFrames = 0;
    
        for (std::uint32_t i = 0; i < numBufs; i++) {
            tmpbuf = & (bufDescTable[i]);
            std::cout << "FrameNo:" << i << " ";
            tmpbuf -> Print();
    
            if (tmpbuf -> valid == true)
                validFrames++;
        }
    
        std::cout << "Total Number of Valid Frames:" << validFrames << "\n";
    }
}