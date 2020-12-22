/**
* @author See Contributors.txt for code contributors and overview of BadgerDB.
*
* @section LICENSE
* Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
*
* Group 28:
* Emilie Rajka erajka@wisc.edu
* Jacqueline McNutt jmcnutt@wisc.edu
* Hock Lee kee3@wisc.edu
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
#include "exceptions/file_exists_exception.h"

//#define DEBUG

namespace badgerdb {

// -----------------------------------------------------------------------------
// BTreeIndex::BTreeIndex -- Constructor
// -----------------------------------------------------------------------------

BTreeIndex::BTreeIndex(const std::string& relationName, std::string& outIndexName, BufMgr* bufMgrIn, const int attrByteOffset, const Datatype attrType)
{

    bufMgr = bufMgrIn;

    attributeType = attrType;

    this->attrByteOffset = attrByteOffset;

    scanExecuting = false;

    leafOccupancy = INTARRAYLEAFSIZE;

    nodeOccupancy = INTARRAYNONLEAFSIZE;

    Page* metaPage;

    std::ostringstream idxStr;

    idxStr << relationName << '.' << attrByteOffset;

    outIndexName = idxStr.str();

    try {

        file = new BlobFile(outIndexName, false);

        // file found so just need to read

        // the first page is the IndexMetaInfo data

        headerPageNum = file->getFirstPageNo();

        bufMgr->readPage(file, headerPageNum, metaPage);

        IndexMetaInfo* metaData = (IndexMetaInfo*)metaPage;

        rootPageNum = metaData->rootPageNo;
    }
    catch (FileNotFoundException& e) {

        file = new BlobFile(outIndexName, true);

        // means file not found so one was created if reach here

        PageId firstId;

        bufMgr->allocPage(file, firstId, *&metaPage);

        IndexMetaInfo* metaData = (IndexMetaInfo*)metaPage;

        char* name = (char*)relationName.c_str();

        strcpy(metaData->relationName, name);

        metaData->attrByteOffset = attrByteOffset;

        metaData->attrType = attributeType;

        metaData->rootPageNo = firstId;

        headerPageNum = firstId;
    }

    bufMgr->unPinPage(file, headerPageNum, false);

    Page* rootPage;

    bufMgr->allocPage(file, rootPageNum, rootPage);

    LeafNodeInt* rootNode = (LeafNodeInt*)rootPage;

    rootNode->level = -1;

    rootNode->rightSibPageNo = Page::INVALID_NUMBER;

    rootNode->numKeys = 0;

    bufMgr->unPinPage(file, rootPageNum, true);

    FileScan fileScan(relationName, bufMgr);

    try {

        RecordId rid;

        while (1) {

            fileScan.scanNext(rid);

            std::string record = fileScan.getRecord();

            const int* key = (int*)(record.c_str() + attrByteOffset);

            insertEntry(key, rid);
        }
    }
    catch (EndOfFileException& exception) {

        // reach end of file to break the loop

        bufMgr->flushFile(file);
    }
}

// -----------------------------------------------------------------------------
// BTreeIndex::~BTreeIndex -- destructor
// -----------------------------------------------------------------------------

BTreeIndex::~BTreeIndex()
{

    /**

         * BTreeIndex Destructor.

         * End any initialized scan, flush index file, after unpinning any pinned pages, from the buffer manager

         * and delete file instance thereby closing the index file.

         * Destructor should not throw any exceptions. All exceptions should be caught in here itself.

         * */

    if (scanExecuting) {

        endScan();

        scanExecuting = false;
    }

    bufMgr->flushFile(file);

    delete file;
}

// -----------------------------------------------------------------------------
// BTreeIndex::insertEntry
// -----------------------------------------------------------------------------

void BTreeIndex::insertEntry(const void* key, const RecordId rid)
{

    //Create a pair for the newNode

    RIDKeyPair<int> newNode;

    newNode.set(rid, *(int*)key);

    //Get the pointer to the root node

    Page* root;

    bufMgr->readPage(file, rootPageNum, root);

    recursionInsert(root, isLeaf(root), rootPageNum, newNode);

    bufMgr->unPinPage(file, rootPageNum, true);
}

void BTreeIndex::split(int isLeaf, Page* nodePage, PageId nodePageId, RIDKeyPair<int> newNode)
{

    if (isLeaf == 1) {

        LeafNodeInt* curNode = (LeafNodeInt*)nodePage;

        //Create a new leaf page

        Page* newLeafPage;

        PageId newPageNum;

        bufMgr->allocPage(file, newPageNum, newLeafPage);

        LeafNodeInt* newLeafNode = (LeafNodeInt*)newLeafPage;

        //Split half of the previous leaf node into newLeafNode

        int midIndex = leafOccupancy / 2;

        //Add half to the newLeafNode and remove from the curLeafNode

        for (int i = midIndex; i < leafOccupancy; i++) {

            newLeafNode->keyArray[i - midIndex] = curNode->keyArray[i];

            curNode->keyArray[i] = 0;

            newLeafNode->ridArray[i - midIndex] = curNode->ridArray[i];

            curNode->ridArray[i].page_number = 0;

            newLeafNode->numKeys += 1;

            curNode->numKeys -= 1;
        }

        //Then add the newNode into one of them

        if (newNode.key < newLeafNode->keyArray[0]) {

            findIndexAndInsertLeaf(curNode, newNode);
        }
        else {

            findIndexAndInsertLeaf(newLeafNode, newNode);
        }

        //Update the siblings

        newLeafNode->rightSibPageNo = curNode->rightSibPageNo;

        curNode->rightSibPageNo = newPageNum;

        newLeafNode->level = -1;

        //Set the global variable for the new NonLeafNode

        //Use the first index of the key array of the  newLeafNode

        newNonLeafNodePageNum = newPageNum;

        newNonLeafNodeKey = newLeafNode->keyArray[0];

        //Check if root needs to be updated

        if (nodePageId == rootPageNum) {

            Page* newRootPage;

            PageId newRootId;

            bufMgr->allocPage(file, newRootId, newRootPage);

            NonLeafNodeInt* newRoot = (NonLeafNodeInt*)newRootPage;

            newRoot->level = 1;

            // the middle value is passed up

            newRoot->pageNoArray[0] = nodePageId;

            newRoot->pageNoArray[1] = newPageNum;

            newRoot->keyArray[0] = newNonLeafNodeKey;

            newRoot->numKeys = 1;

            bufMgr->unPinPage(file, nodePageId, 1); //unpin OLD ROOT

            rootPageNum = newRootId;

            Page* metaDataPage;

            bufMgr->readPage(file, headerPageNum, metaDataPage);

            IndexMetaInfo* metaData = (IndexMetaInfo*)metaDataPage;

            metaData->rootPageNo = newRootId;

            bufMgr->unPinPage(file, headerPageNum, false);

        }

        bufMgr->unPinPage(file, newPageNum, true);
    }

    //Non-Leaf Node
    else {

        NonLeafNodeInt* curNode = (NonLeafNodeInt*)nodePage;

        //Create a new non leaf page

        Page* newNonLeafPage;

        PageId newPageNum;

        bufMgr->allocPage(file, newPageNum, newNonLeafPage);

        NonLeafNodeInt* newNonLeafNode = (NonLeafNodeInt*)newNonLeafPage;

        //Split half of the previous non leaf node into newNonLeafNode

        int midIndex = nodeOccupancy / 2;

        //Add half the keys to the newNonLeafNode and remove from the curNode

        for (int i = midIndex; i < nodeOccupancy; i++) {

            newNonLeafNode->keyArray[i - midIndex] = curNode->keyArray[i];

            newNonLeafNode->pageNoArray[i - midIndex] = curNode->pageNoArray[i];

            newNonLeafNode->numKeys += 1;

            curNode->keyArray[i - midIndex] = 0;

            curNode->pageNoArray[i - midIndex] = 0;

            curNode->numKeys -= 1;
        }

        //Since pageNoArray has one extra index:

        newNonLeafNode->pageNoArray[nodeOccupancy - midIndex - 1] = curNode->pageNoArray[nodeOccupancy];

        curNode->pageNoArray[nodeOccupancy] = Page::INVALID_NUMBER;

        int pushUpKey;

        //Select from the nodes that will have more keys to push up

        if (newNode.key < curNode->keyArray[midIndex - 1]) {

            pushUpKey = curNode->keyArray[midIndex - 1];

            curNode->keyArray[midIndex - 1] = 0;

            curNode->numKeys -= 1;

            //Update the newNonLeafNode to be pushed up

            newNonLeafNodePageNum = newPageNum;

            newNonLeafNodeKey = pushUpKey;

            //Insert the new non leaf node

            findIndexAndInsertNonLeaf(curNode);
        }
        else {

            pushUpKey = newNonLeafNode->keyArray[0];

            //Shift the index

            for (int i = 0; i < newNonLeafNode->numKeys - 1; i++) {

                newNonLeafNode->keyArray[i] = newNonLeafNode->keyArray[i + 1];
            }

            newNonLeafNode->keyArray[midIndex - 1] = 0;

            newNonLeafNode->numKeys -= 1;

            //Update the newNonLeafNode to be pushed up

            newNonLeafNodePageNum = newPageNum;

            newNonLeafNodeKey = pushUpKey;

            //Insert the new non leaf node

            findIndexAndInsertNonLeaf(newNonLeafNode);
        }

        if (nodePageId == rootPageNum) {

            Page* newRootPage;

            PageId newRootId;

            bufMgr->allocPage(file, newRootId, newRootPage);

            NonLeafNodeInt* newRoot = (NonLeafNodeInt*)newRootPage;

            newRoot->level = 1;

            newRoot->pageNoArray[0] = nodePageId;

            newRoot->pageNoArray[1] = newNonLeafNodePageNum;

            newRoot->keyArray[0] = newNonLeafNodeKey;

            newRoot->numKeys = 1;

            rootPageNum = newRootId;

            Page* metaDataPage;

            bufMgr->readPage(file, headerPageNum, metaDataPage);

            IndexMetaInfo* metaData = (IndexMetaInfo*)metaDataPage;

            metaData->rootPageNo = newRootId;

            bufMgr->unPinPage(file, headerPageNum, true);

            bufMgr->unPinPage(file, newRootId, true);
        }

        bufMgr->unPinPage(file, newPageNum, false);
    }
}

void BTreeIndex::findIndexAndInsertLeaf(LeafNodeInt* curNode, RIDKeyPair<int> newNode)
{

    //Loop to look for the index to insert

    if (curNode->numKeys == 0) {

        curNode->keyArray[0] = newNode.key;

        curNode->ridArray[0] = newNode.rid;

        curNode->numKeys += 1;
    }
    else {

        for (int i = 0; i <= curNode->numKeys; i++) {

            if (i == curNode->numKeys) {

                curNode->keyArray[i] = newNode.key;

                curNode->ridArray[i] = newNode.rid;

                curNode->numKeys += 1;

                break;
            }
            else if (curNode->keyArray[i] < newNode.key) {

                continue;
            }
            else {

                //Shift the index after

                for (int j = curNode->numKeys - 1; j >= i; j--) {

                    curNode->keyArray[j + 1] = curNode->keyArray[j];

                    curNode->ridArray[j + 1] = curNode->ridArray[j];
                }

                curNode->keyArray[i] = newNode.key;

                curNode->ridArray[i] = newNode.rid;

                curNode->numKeys += 1;

                break;
            }
        }
    }
}

void BTreeIndex::findIndexAndInsertNonLeaf(NonLeafNodeInt* curNode)
{

    for (int i = 0; i <= curNode->numKeys; i++) {

        if (curNode->keyArray[i] == newNonLeafNodeKey) {

            break;
        }
        else if (i == curNode->numKeys && curNode->numKeys < nodeOccupancy - 1) {

            curNode->keyArray[i] = newNonLeafNodeKey;

            curNode->pageNoArray[i + 1] = newNonLeafNodePageNum;

            curNode->numKeys += 1;

            //Reset the global variables

            newNonLeafNodePageNum = Page::INVALID_NUMBER;

            newNonLeafNodeKey = 0;
        }
        else if (curNode->keyArray[i] < newNonLeafNodeKey) {

            continue;
        }
        else {

            //Shift the index after

            for (int j = curNode->numKeys; j > i; j--) {

                curNode->keyArray[j] = curNode->keyArray[j - 1];

                curNode->pageNoArray[j + 1] = curNode->pageNoArray[j];
            }

            curNode->keyArray[i] = newNonLeafNodeKey;

            curNode->pageNoArray[i + 1] = newNonLeafNodePageNum;

            curNode->numKeys += 1;

            //Reset the global variables

            newNonLeafNodePageNum = Page::INVALID_NUMBER;

            newNonLeafNodeKey = 0;

            break;
        }
    }

    curNode->level = 1;
}

void BTreeIndex::recursionInsert(Page* nodePage, int leaf, PageId nodePageId, RIDKeyPair<int> newNode)
{

    //If node is not leaf

    if (leaf == 0) {

        NonLeafNodeInt* curNode = (NonLeafNodeInt*)nodePage;

        PageId nextPage;

        //Find the index to use in the keyArray

        for (int i = 0; i < curNode->numKeys; i++) {

            if (curNode->keyArray[i] < newNode.key) {

                if (i + 1 == curNode->numKeys) {

                    nextPage = curNode->pageNoArray[i + 1];
                }

                continue;
            }

            nextPage = curNode->pageNoArray[i];

            break;
        }

        Page* child;

        bufMgr->readPage(file, nextPage, child);

        recursionInsert(child, isLeaf(child), nextPage, newNode);

        // Check if need to split for nonLeafNode if there is a split

        // in the leaf node. If there is a split in the leafNode,

        // newNonLeafNodePageNum and newNonLeafNodeKey is not null

        if (newNonLeafNodePageNum != Page::INVALID_NUMBER &&

            newNonLeafNodeKey != 0) {

            //Insert the new NonLeafNode into the curNode's pageNoArray

            //Check if there is space in the curNode's pageNoArray

            if (curNode->numKeys < nodeOccupancy) {

                findIndexAndInsertNonLeaf(curNode);
            }

            //Otherwise split the curNode if there is no space
            else {

                split(0, nodePage, nextPage, newNode);

            }
        }

        bufMgr->unPinPage(file, nextPage, 0);
    }

    //If node is leaf
    else {

        LeafNodeInt* curNode = (LeafNodeInt*)nodePage;

        //Checks if there is space in the node

        if (curNode->numKeys < leafOccupancy) {

            findIndexAndInsertLeaf(curNode, newNode);
        }

        //Othewise split the leaf page
        else {

            split(1, nodePage, nodePageId, newNode);
        }
    }
}

void BTreeIndex::setNextScan(PageId nextPage)
{

    nextEntry = 0;

    currentPageNum = nextPage;

    Page* curPage;

    bufMgr->readPage(file, currentPageNum, curPage);

    currentPageData = curPage;
}

int BTreeIndex::isLeaf(Page* page)
{

    LeafNodeInt* node = (LeafNodeInt*)page;

    if (node->level == -1) {

        return true;
    }
    else {

        return false;
    }
}

void BTreeIndex::recurScan()
{

    // determine if current node is leaf or nonleaf

    bool leaf_bool = isLeaf(currentPageData);

    if (leaf_bool) {

        LeafNodeInt* curPage = (LeafNodeInt*)currentPageData;

        while (true) {

            if (curPage->keyArray[nextEntry] < inclLow) {

                // if at end of array: go to next leaf if possible, otherwise throw exception

                if (nextEntry == curPage->numKeys - 1) {

                    if (curPage->rightSibPageNo != Page::INVALID_NUMBER) {

                        PageId old_page = currentPageNum;

                        setNextScan(curPage->rightSibPageNo);

                        bufMgr->unPinPage(file, old_page, false); // unpin; not dirty
                    }
                    else {

                        throw NoSuchKeyFoundException();
                    }
                }

                // if not at end of array, just go to next entry in node
                else {

                    nextEntry++;
                }
            }
            else if (curPage->keyArray[nextEntry] >= inclLow) {

                // in range!

                if (curPage->keyArray[nextEntry] <= inclHigh) {

                    return; // scanNext() can be called by user
                }

                // not in range
                else {

                    throw NoSuchKeyFoundException();
                }
            }
        }
    }

    // recursive case: if cur page is a nonleaf
    else {

        NonLeafNodeInt* curPage = (NonLeafNodeInt*)currentPageData;

        while (true) {

            // case1: check next entry in array

            if (curPage->keyArray[nextEntry] < inclLow) {

                // case that all values in node are less than target, go right

                if (nextEntry == curPage->numKeys - 1) {

                    PageId old_page = currentPageNum;

                    setNextScan(curPage->pageNoArray[nextEntry + 1]);

                    bufMgr->unPinPage(file, old_page, false); // unpin; not dirty

                    recurScan();

                    break;
                }

                nextEntry++;
            }

            // case2: go right
            else if (curPage->keyArray[nextEntry] == inclLow) {

                bufMgr->unPinPage(file, currentPageNum, false);

                setNextScan(curPage->pageNoArray[nextEntry + 1]);

                recurScan();

                break;
            }

            // case3: go left
            else if (curPage->keyArray[nextEntry] > inclLow) {

                bufMgr->unPinPage(file, currentPageNum, false);

                setNextScan(curPage->pageNoArray[nextEntry]);

                recurScan();

                break;
            }
        }
    }
}

// -----------------------------------------------------------------------------
// BTreeIndex::startScan
// -----------------------------------------------------------------------------

void BTreeIndex::startScan(const void* lowValParm, const Operator lowOpParm,
    const void* highValParm, const Operator highOpParm)
{

    // check that opcode parameters are valid

    if (lowOpParm != GT and lowOpParm != GTE) {

        throw BadOpcodesException();
    }

    if (highOpParm != LT and highOpParm != LTE) {

        throw BadOpcodesException();
    }

    // check that range values are valid

    if (*(int*)lowValParm > *(int*)highValParm) {

        throw BadScanrangeException();
    }

    // end scan if one is already executing?

    if (scanExecuting) {

        endScan();
    }

    // set vars for scan
    scanExecuting = true;
    setNextScan(rootPageNum);
    lowValInt = *(int*)lowValParm;
    highValInt = *(int*)highValParm;
    lowOp = lowOpParm;
    highOp = highOpParm;

    if (lowOp == GTE) {

        inclLow = lowValInt;
    }

    if (lowOp == GT) {

        inclLow = lowValInt + 1;
    }

    if (highOp == LTE) {

        inclHigh = highValInt;
    }

    if (highOp == LT) {

        inclHigh = highValInt - 1;
    }

    // scan
    recurScan();
}

// -----------------------------------------------------------------------------
// BTreeIndex::scanNext
// -----------------------------------------------------------------------------

void BTreeIndex::scanNext(RecordId& outRid)
{

    if (!scanExecuting) {

        throw ScanNotInitializedException();
    }

    // get current page

    LeafNodeInt* curPage = (LeafNodeInt*)currentPageData;

    // if no more records

    if (curPage->keyArray[nextEntry] > inclHigh or nextEntry == -1) {

        throw IndexScanCompletedException();
    }

    // there are more records: return id

    outRid = curPage->ridArray[nextEntry];

    // set up for next scanNext

    // go to next node

    if (nextEntry == curPage->numKeys - 1) {

        // there is a next node

        if (curPage->rightSibPageNo != Page::INVALID_NUMBER) {

            PageId old_page = currentPageNum;

            setNextScan(curPage->rightSibPageNo);

            bufMgr->unPinPage(file, old_page, false); // unpin; not dirty
        }

        // there is no next node
        else {
            // set nextEntry = -1 to signal no more records to scan
            nextEntry = -1;
        }
    }
    // go to next entry in same node
    else {
        nextEntry++;
    }
}

// -----------------------------------------------------------------------------
// BTreeIndex::endScan
// -----------------------------------------------------------------------------
//

void BTreeIndex::endScan()
{

    /**
         * Terminate the current scan. Unpin any pinned pages. Reset scan specific variables.
         * @throws ScanNotInitializedException If no scan has been initialized.
         **/

    if (!scanExecuting) {

        throw ScanNotInitializedException();
    }

    scanExecuting = false;

    bufMgr->unPinPage(file, currentPageNum, false);

    currentPageNum = Page::INVALID_NUMBER;

    currentPageData = nullptr;

    nextEntry = -1;
}

int BTreeIndex::height(PageId cur)
{

    Page* curPage;

    bufMgr->readPage(file, cur, curPage);

    bool leaf_bool = isLeaf(curPage);

    if (leaf_bool) {

        bufMgr->unPinPage(file, cur, false);

        return 0;
    }
    else {

        NonLeafNodeInt* curNode = (NonLeafNodeInt*)curPage;

        int h = 1 + height(curNode->pageNoArray[0]);

        bufMgr->unPinPage(file, cur, false);

        return h;
    }
}

void BTreeIndex::printLevel(PageId cur, int level)
{

    Page* curPage;

    bufMgr->readPage(file, cur, curPage);

    bool leaf_bool = isLeaf(curPage);

    if (level == 0) {

        //print node

        std::cout << "printing node" << std::endl;

        if (leaf_bool) {

            LeafNodeInt* curNode = (LeafNodeInt*)curPage;

            for (int i = 0; i < curNode->numKeys; i++) {

                std::cout << curNode->keyArray[i] << " ";
            }

            std::cout << std::endl;
        }
        else {

            NonLeafNodeInt* curNode = (NonLeafNodeInt*)curPage;

            for (int i = 0; i < curNode->numKeys; i++) {

                std::cout << curNode->keyArray[i] << std::endl;
            }
        }

        bufMgr->unPinPage(file, cur, false);
    }
    else if (level > 0) {

        if (leaf_bool) {

            return;
        }
        else {

            NonLeafNodeInt* curNode = (NonLeafNodeInt*)curPage;

            for (int i = 0; i <= curNode->numKeys; i++) {

                printLevel(curNode->pageNoArray[i], level - 1);
            }
        }
    }
}

void BTreeIndex::printTree()
{

    int h = height(rootPageNum);

    for (int i = 0; i <= h; i++) {

        printLevel(rootPageNum, i);
    }
}
}