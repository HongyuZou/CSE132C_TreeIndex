/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include "btree.h"
#include "filescan.h"
#include "file.h"
#include "exceptions/bad_index_info_exception.h"
#include "exceptions/bad_opcodes_exception.h"
#include "exceptions/bad_scanrange_exception.h"
#include "exceptions/no_such_key_found_exception.h"
#include "exceptions/scan_not_initialized_exception.h"
#include "exceptions/index_scan_completed_exception.h"
#include "exceptions/file_not_found_exception.h"
#include "exceptions/end_of_file_exception.h"
#include <string>
#include <unordered_map>

//#define DEBUG

namespace badgerdb
{

// -----------------------------------------------------------------------------
// BTreeIndex::BTreeIndex -- Constructor
// -----------------------------------------------------------------------------

BTreeIndex::BTreeIndex(const std::string & relationName,
		std::string & outIndexName,
		BufMgr *bufMgrIn,
		const int attrByteOffset,
		const Datatype attrType) {
	
	// check if index file exists
	std::ostringstream idxStr;
	idxStr << relationName << '.' << attrByteOffset;
	std::string indexName = idxStr.str(); 
	File* newFile = new BlobFile(indexName, true);
	
	// assign class members
	this->file = newFile;
	this->attributeType = attrType;
	this->attrByteOffset = attrByteOffset;
	this->bufMgr = bufMgrIn;

	// init root node
	PageId rootPageId;
	Page* rootPage;
	bufMgr->allocPage(this->file, rootPageId, rootPage);
	this->rootPageNum = rootPageId;

	if(this->attributeType == INTEGER) {
		this->leafOccupancy = INTARRAYLEAFSIZE;
		this->nodeOccupancy = INTARRAYNONLEAFSIZE;
		struct NonLeafNodeInt root = {.level = 1};
		struct NonLeafNodeInt* temp = (NonLeafNodeInt*)rootPage;
		*temp = root;
		this->bufMgr->unPinPage(this->file, rootPageId, true);
	} else if(this->attributeType == DOUBLE) { 
		this->leafOccupancy = DOUBLEARRAYLEAFSIZE;
		this->nodeOccupancy = DOUBLEARRAYNONLEAFSIZE;
		struct NonLeafNodeDouble root = {.level = 1};
		struct NonLeafNodeDouble* temp = (NonLeafNodeDouble*)rootPage;
		*temp = root;
		this->bufMgr->unPinPage(this->file, rootPageId, true);
	} else {
		this->leafOccupancy = STRINGARRAYLEAFSIZE;
		this->nodeOccupancy = STRINGARRAYNONLEAFSIZE;
		struct NonLeafNodeString root = {.level = 1};
		struct NonLeafNodeString* temp = (NonLeafNodeString*)rootPage;
		*temp = root;
		this->bufMgr->unPinPage(this->file, rootPageId, true);
	}

	// build index meta info
	const char* tempName = relationName.c_str();
	struct IndexMetaInfo metaInfo = {.attrByteOffset = attrByteOffset, .attrType = attrType, .rootPageNo = -1};
	strcpy(metaInfo.relationName, tempName);
	
	// write meta info to index file
	PageId metaPageId;
	Page* headerPage;
	bufMgr->allocPage(newFile, metaPageId, headerPage);
	struct IndexMetaInfo* temp = (IndexMetaInfo*)headerPage;
	*temp = metaInfo;
	this->headerPageNum = metaPageId;
	this->bufMgr->unPinPage(newFile, metaPageId, true);

	// scan relation and insert one by one
	FileScan* fileScan = new FileScan(relationName, bufMgrIn);

	while(true) {
		try {
			RecordId record_id;
			fileScan->scanNext(record_id);
			std::string record = fileScan->getRecord();

			// pick key according to datatype
			switch(attrType) {
				case INTEGER:
				{
					// convert string to int
					std::string key = record.substr(attrByteOffset, 4);
					char temp[4];
					strcpy(temp, key.c_str());
					int key_ptr;;
					memcpy(&key_ptr, temp, 4);

					insertEntry(&key_ptr, record_id); 
					break;
				}
				case DOUBLE:
				{	
					std::string key = record.substr(attrByteOffset, 8);
					char temp[8];
					strcpy(temp, key.c_str());
					double key_ptr;
					memcpy(&key_ptr, temp, 8);

					insertEntry(&key_ptr, record_id);
					break;
				}
				case STRING:
				{
					std::string key = record.substr(attrByteOffset, 10);
					insertEntry(key.c_str(), record_id);
					break;
				}
			}
		} catch(EndOfFileException& e) {
			break;
		}
	}

	// store outindex name
	outIndexName = indexName;
}


// -----------------------------------------------------------------------------
// BTreeIndex::~BTreeIndex -- destructor
// -----------------------------------------------------------------------------

BTreeIndex::~BTreeIndex()
{
}

const PageId BTreeIndex::findPageNoInNonLeaf(Page* node, const void* key) {
	if(this->attributeType == INTEGER) {
		struct NonLeafNodeInt* temp = (NonLeafNodeInt*)(node);

		int i;
		for(i = 0; i < INTARRAYNONLEAFSIZE; i ++) {
			if(*(int*)key < temp->keyArray[i]) {
				return temp->pageNoArray[i];
			}

			if(*(int*)key == temp->keyArray[i]) {
				return temp->pageNoArray[i + 1];
			}
		}

		return temp->pageNoArray[i + 1];
		
	} else if(this->attributeType == DOUBLE) {
		struct NonLeafNodeDouble* temp = (NonLeafNodeDouble*)(node);

		int i;
		for(i = 0; i < DOUBLEARRAYNONLEAFSIZE; i ++) {
			if(*(double*)key < temp->keyArray[i]) {
				return temp->pageNoArray[i];
			}

			if(*(double*)key == temp->keyArray[i]) {
				return temp->pageNoArray[i + 1];
			}
		}

		return temp->pageNoArray[i + 1];
	} else {
		struct NonLeafNodeString* temp = (NonLeafNodeString*)(node);
		std::string strKey((char*)key);

		int i;
		for(i = 0; i < STRINGARRAYNONLEAFSIZE; i ++) {
			std::string nodeKey(temp->keyArray[i]);
			if(strKey < nodeKey) {
				return temp->pageNoArray[i];
			}

			if(strKey == nodeKey) {
				return temp->pageNoArray[i + 1];
			}
		}

		return temp->pageNoArray[i + 1];
	}
}

/* insert key to leaf*/
void insertStringKeyToLeaf(LeafNodeString* leaf, const void* key, const RecordId rid) {
	// find place to insert
	for(int i = 0; i < leaf->keyArrLength; i ++) {
		if(std::string(leaf->keyArray[i]) > std::string((char*)key)) {
			char temp[11] = {'\0'};
			strcpy(temp, leaf->keyArray[i]);
			strcpy(leaf->keyArray[i], (char*)key);

			// shift key to right
			for(int j = i + 1; j < leaf->keyArrLength; j ++) {
				char curr[11] = {'\0'};
				strcpy(curr, leaf->keyArray[j]);
				strcpy(leaf->keyArray[j], temp);
				strcpy(temp, curr);
			}
			strcpy(leaf->keyArray[leaf->keyArrLength], temp);
			
			// insert rid and shift to right
			RecordId temp_rid = leaf->ridArray[i];
			leaf->ridArray[i] = rid;
			for(int j = i + 1; j < leaf->keyArrLength; j ++) {
				RecordId curr = leaf->ridArray[j];
				leaf->ridArray[j] = temp_rid;
				temp_rid = curr;
			}
			leaf->ridArray[leaf->keyArrLength] = temp_rid;

			leaf->keyArrLength ++;
			return;
		}
	}

	// insert at last place
	strcpy(leaf->keyArray[leaf->keyArrLength], (char*)key);
	leaf->ridArray[leaf->keyArrLength] = rid;
	leaf->keyArrLength ++;
}

void insertDoubleKeyToLeaf(LeafNodeDouble* leaf, const void* key, const RecordId rid) {
	// find place to insert
	for(int i = 0; i < leaf->keyArrLength; i ++) {
		if(leaf->keyArray[i] > *(double*)key) {
			double temp = leaf->keyArray[i];
			leaf->keyArray[i] = *(double*)key;

			// shift key to right
			for(int j = i + 1; j < leaf->keyArrLength; j ++) {
				double curr = leaf->keyArray[j];
				leaf->keyArray[j] = temp;
				temp = curr;
			}
			leaf->keyArray[leaf->keyArrLength] = temp;
			
			// insert rid and shift to right
			RecordId temp_rid = leaf->ridArray[i];
			leaf->ridArray[i] = rid;
			for(int j = i + 1; j < leaf->keyArrLength; j ++) {
				RecordId curr = leaf->ridArray[j];
				leaf->ridArray[j] = temp_rid;
				temp_rid = curr;
			}
			leaf->ridArray[leaf->keyArrLength] = temp_rid;

			leaf->keyArrLength ++;
			return;
		}
	}

	// insert at last place
	leaf->keyArray[leaf->keyArrLength] = *(double*)key;
	leaf->ridArray[leaf->keyArrLength] = rid;
	leaf->keyArrLength ++;
}

void insertIntKeyToLeaf(LeafNodeInt* leaf, const void* key, const RecordId rid) {
	// find place to insert
	for(int i = 0; i < leaf->keyArrLength; i ++) {
		if(leaf->keyArray[i] > *(int*)key) {
			int temp = leaf->keyArray[i];
			leaf->keyArray[i] = *(int*)key;

			// shift key to right
			for(int j = i + 1; j < leaf->keyArrLength; j ++) {
				int curr = leaf->keyArray[j];
				leaf->keyArray[j] = temp;
				temp = curr;
			}
			leaf->keyArray[leaf->keyArrLength] = temp;
			
			// insert rid and shift to right
			RecordId temp_rid = leaf->ridArray[i];
			leaf->ridArray[i] = rid;
			for(int j = i + 1; j < leaf->keyArrLength; j ++) {
				RecordId curr = leaf->ridArray[j];
				leaf->ridArray[j] = temp_rid;
				temp_rid = curr;
			}
			leaf->ridArray[leaf->keyArrLength] = temp_rid;

			leaf->keyArrLength ++;
			return;
		}
	}

	// insert at last place
	leaf->keyArray[leaf->keyArrLength] = *(int*)key;
	leaf->ridArray[leaf->keyArrLength] = rid;
	leaf->keyArrLength ++;
}

int cmpfuncInt (const void * a, const void * b) {
   return ( *(int*)a - *(int*)b );
}

/* split leaf node*/
const PageId BTreeIndex::splitLeafNodeInt(struct LeafNodeInt* node, int* key, const RecordId rid) {
	
	// allocate a new leaf
	Page* newNodePage;
	PageId newNodePageId;
	this->bufMgr->allocPage(this->file, newNodePageId, newNodePage);

	int leftCnt = (this->leafOccupancy + 1) / 2;
	bool flag = false;
	int stored_key;
	RecordId stored_rid;

	// left node is the original node
	for (int i = 0; i < leftCnt ; i++) {
		if (*key < node->keyArray[i] && !flag) {
			stored_key = node->keyArray[i];
			stored_rid = node->ridArray[i];
			node->keyArray[i] = *key;
			node->ridArray[i] = rid;
			flag = true;
		} else if (flag) {
			int temp_key = node->keyArray[i];
			RecordId temp_rid = node->ridArray[i];
			node->keyArray[i] = stored_key;
			node->ridArray[i] = stored_rid;
			stored_key = temp_key;
			stored_rid = temp_rid;
		}
	}
	node->keyArrLength = leftCnt;

	// right node is new node
	struct LeafNodeInt* newNode = (LeafNodeInt*) newNodePage;
	if (flag) {
		newNode->keyArray[0] = stored_key;
		newNode->ridArray[0] = stored_rid;
		for (int i = leftCnt; i < this->leafOccupancy ; i++) {
			newNode->keyArray[i - leftCnt + 1] = newNode->keyArray[i];
			newNode->ridArray[i - leftCnt + 1] = newNode->ridArray[i];
		}
	} else {
		for (int i = 0; i < this->leafOccupancy - leftCnt+1 ; i++) {
			if (*key < newNode->keyArray[i] && !flag) {
				stored_key = newNode->keyArray[i];
				stored_rid = newNode->ridArray[i];
				newNode->keyArray[i] = *key;
				newNode->ridArray[i] = rid;
				flag = true;
			} else if (flag) {
				int temp_key = newNode->keyArray[i];
				RecordId temp_rid = newNode->ridArray[i];
				newNode->keyArray[i] = stored_key;
				newNode->ridArray[i] = stored_rid;
				stored_key = temp_key;
				stored_rid = temp_rid;
			}
		}
		newNode->keyArray[this->leafOccupancy - leftCnt+1] = stored_key;
		newNode->ridArray[this->leafOccupancy - leftCnt+1] = stored_rid;
	}
	newNode->keyArrLength = leafOccupancy - leftCnt + 1;
	
	// set left node value to be 0 after copy
	memset(node->keyArray + leftCnt, 0, sizeof(int) * this->leafOccupancy - leftCnt); 

	// sibling pointer
	newNode->rightSibPageNo = node->rightSibPageNo;
	node->rightSibPageNo = newNodePageId; 

	return newNodePageId;
}


// -----------------------------------------------------------------------------
// BTreeIndex::insertRecursive
// -----------------------------------------------------------------------------
// remember to unpin pages
const std::pair<PageId, PageId> BTreeIndex::insertRecursive(PageId root, const void *key, 
                                       const RecordId rid, int lastLevel) {
	// check if current node is leaf or not
	Page* node;
	this->bufMgr->readPage(this->file, root, node);

	if(this->attributeType == INTEGER) {
		if(lastLevel == 1) {
			// is a leaf
			struct LeafNodeInt* currNode = (LeafNodeInt*)node;
			
			// check whether to split
			if(currNode->keyArrLength < this->leafOccupancy) {
				insertIntKeyToLeaf(currNode, key, rid);
				return std::pair<PageId, PageId>(root, -1);
			} else {
				// split
				PageId newRightNode = splitLeafNodeInt(currNode, (int*)key, rid);
			    return std::pair<PageId, PageId>(root, newRightNode);
			}
		} else {
			// not a leaf
			PageId nextNodeId = findPageNoInNonLeaf(node, key);
			struct NonLeafNodeInt* currNode = (NonLeafNodeInt*)node;
			std::pair<PageId, PageId> children = insertRecursive(nextNodeId, key, rid, currNode->level);

			// return from recursive call, propagate up
		}
	} else if(this->attributeType == DOUBLE) {
		if(lastLevel == 1) {
			// is a leaf
			struct LeafNodeDouble* currNode = (LeafNodeDouble*)node;
			
			// check whether to split
			if(currNode->keyArrLength < this->leafOccupancy) {
				insertDoubleKeyToLeaf(currNode, key, rid);
			} else {
				// split
			
			}
		} else {
			// not a leaf
			PageId nextNodeId = findPageNoInNonLeaf(node, key);
			struct NonLeafNodeInt* currNode = (NonLeafNodeInt*)node;
			insertRecursive(nextNodeId, key, rid, currNode->level);
		}
	} else {
		if(lastLevel == 1) {
			// is a leaf
			struct LeafNodeString* currNode = (LeafNodeString*)node;
			
			// check whether to split
			if(currNode->keyArrLength < this->leafOccupancy) {
				insertStringKeyToLeaf(currNode, key, rid);
			} else {
				// split
			
			}
		} else {
			// not a leaf
			PageId nextNodeId = findPageNoInNonLeaf(node, key);
			struct NonLeafNodeString* currNode = (NonLeafNodeString*)node;
			insertRecursive(nextNodeId, key, rid, currNode->level);
		}
	}
}

// -----------------------------------------------------------------------------
// BTreeIndex::insertEntry
// -----------------------------------------------------------------------------

const void BTreeIndex::insertEntry(const void *key, const RecordId rid) {
	Page* rootPage;
	this->bufMgr->readPage(this->file, this->rootPageNum, rootPage);

	if(this->attributeType == INTEGER) {
		struct NonLeafNodeInt* rootNode = (NonLeafNodeInt*) rootPage;
		if(rootNode->keyArrLength == 0) {
			rootNode->keyArray[0] = *((int*)key);
			
			// allocate leaf
			Page* leafPage;
			PageId leafPageId;
			this->bufMgr->allocPage(this->file, leafPageId, leafPage);
			struct LeafNodeInt* leafNode = (LeafNodeInt*)leafPage;
		}
	} else if(this->attributeType == DOUBLE) {
		struct NonLeafNodeDouble* rootNode = (NonLeafNodeDouble*) rootPage;
		if(rootNode->keyArrLength == 0) {
			rootNode->keyArray[0] = *((double*)key);
			
			// allocate leaf
			Page* leafPage;
			PageId leafPageId;
			this->bufMgr->allocPage(this->file, leafPageId, leafPage);
			struct LeafNodeDouble* leafNode = (LeafNodeDouble*)leafPage;
		}

	} else {
		struct NonLeafNodeString* rootNode = (NonLeafNodeString*) rootPage;
		if(rootNode->keyArrLength == 0) {
			strcpy(rootNode->keyArray[0], (char*)key);
			
			// allocate leaf
			Page* leafPage;
			PageId leafPageId;
			this->bufMgr->allocPage(this->file, leafPageId, leafPage);
			struct LeafNodeString* leafNode = (LeafNodeString*)leafPage;
		}
	}

	// recursively insert
	insertRecursive(this->rootPageNum, key, rid, 0);
}

// -----------------------------------------------------------------------------
// BTreeIndex::startScan
// -----------------------------------------------------------------------------

const void BTreeIndex::startScan(const void* lowValParm,
				   const Operator lowOpParm,
				   const void* highValParm,
				   const Operator highOpParm)
{

}

// -----------------------------------------------------------------------------
// BTreeIndex::scanNext
// -----------------------------------------------------------------------------

const void BTreeIndex::scanNext(RecordId& outRid) 
{

}

// -----------------------------------------------------------------------------
// BTreeIndex::endScan
// -----------------------------------------------------------------------------
//
const void BTreeIndex::endScan() 
{

}

}
