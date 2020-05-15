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
	struct IndexMetaInfo metaInfo = {.attrByteOffset = attrByteOffset, .attrType = attrType, .rootPageNo = rootPageId};
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
					int key_ptr;
					memcpy(&key_ptr, temp, 4);
					printf("int value: %d\n", key_ptr);
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
		for(i = 0; i < temp->keyArrLength; i ++) {
			if(*(int*)key < temp->keyArray[i]) {
				return temp->pageNoArray[i];
			}

			if(*(int*)key == temp->keyArray[i]) {
				return temp->pageNoArray[i + 1];
			}
		}

		return temp->pageNoArray[i];
		
	} else if(this->attributeType == DOUBLE) {
		struct NonLeafNodeDouble* temp = (NonLeafNodeDouble*)(node);

		int i;
		for(i = 0; i < temp->keyArrLength; i ++) {
			if(*(double*)key < temp->keyArray[i]) {
				return temp->pageNoArray[i];
			}

			if(*(double*)key == temp->keyArray[i]) {
				return temp->pageNoArray[i + 1];
			}
		}

		return temp->pageNoArray[i];
	} else {
		struct NonLeafNodeString* temp = (NonLeafNodeString*)(node);
		std::string strKey((char*)key);

		int i;
		for(i = 0; i < temp->keyArrLength; i ++) {
			std::string nodeKey(temp->keyArray[i]);
			if(strKey < nodeKey) {
				return temp->pageNoArray[i];
			}

			if(strKey == nodeKey) {
				return temp->pageNoArray[i + 1];
			}
		}

		return temp->pageNoArray[i];
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
	//printf("leaf key2 %d\n", *(int*)key);
	leaf->keyArrLength ++;
}

void insertIntKeyToNonLeaf(NonLeafNodeInt* node, const void* key, const PageId rightPage) {
	// find place to insert
	for(int i = 0; i < node->keyArrLength; i ++) {
		if(node->keyArray[i] > *(int*)key) {
			int temp = node->keyArray[i];
			node->keyArray[i] = *(int*)key;

			// shift key to right
			for(int j = i + 1; j < node->keyArrLength; j ++) {
				int curr = node->keyArray[j];
				node->keyArray[j] = temp;
				temp = curr;
			}
			node->keyArray[node->keyArrLength] = temp;
			
			// insert pageId and shift to right
			PageId temp_pageId = node->pageNoArray[i + 1];
			node->pageNoArray[i + 1] = rightPage;
			for(int j = i + 2; j < node->keyArrLength + 1; j ++) {
				PageId curr = node->pageNoArray[j];
				node->pageNoArray[j] = temp_pageId;
				temp_pageId = curr;
			}
			node->pageNoArray[node->keyArrLength + 1] = temp_pageId;

			node->keyArrLength ++;
			return;
		}
	}

	// insert at last place
	node->keyArray[node->keyArrLength] = *(int*)key;
	node->pageNoArray[node->keyArrLength + 1] = rightPage;
	node->keyArrLength ++;

	for(int i = 0; i < 4; i ++) {
		printf("%d ", node->keyArray[i]);
	}
	printf("\n");
}

/* split leaf node*/
const std::pair<PageId, int*> BTreeIndex::splitLeafNodeInt(struct LeafNodeInt* node, int* key, const RecordId rid) {
	// allocate a new leaf
	Page* newNodePage;
	PageId newNodePageId;
	this->bufMgr->allocPage(this->file, newNodePageId, newNodePage);

	struct LeafNodeInt* newNode = (LeafNodeInt*) newNodePage;
	memset(newNode->keyArray, 0, sizeof(newNode->keyArray));
	memset(newNode->ridArray, 0, sizeof(newNode->ridArray));
	int leftCnt = (this->leafOccupancy + 1) / 2;

	// map key to pageId
	std::unordered_map<int, RecordId> map;
	for(int i = 0; i < this->leafOccupancy; i ++) {
		map.insert({node->keyArray[i], node->ridArray[i]});
	}
	map.insert({*key, rid});

	int tempKey[this->leafOccupancy + 1];
	bool inserted = false;
	for(int i = 0; i < this->leafOccupancy; i ++) {
		if(*key < node->keyArray[i] && !inserted) {
			tempKey[i] = *key;
			inserted = true;
			continue;
		}

		if(!inserted) {
			tempKey[i] = node->keyArray[i];
		} else {
			tempKey[i] = node->keyArray[i - 1];
		}
	}
	if(!inserted) {
		tempKey[this->leafOccupancy] = *(int*)key;
	} else {
		tempKey[this->leafOccupancy] = node->keyArray[this->leafOccupancy - 1];
	}
	

	for(int i = 0; i < this->leafOccupancy - leftCnt + 1; i ++) {
		newNode->keyArray[i] = tempKey[i + leftCnt];
		newNode->ridArray[i] = map[tempKey[i + leftCnt]];
	}
 
	// clear left node
	memset(node->keyArray + leftCnt, 0, (this->leafOccupancy - leftCnt) * sizeof(int));
	memset(node->ridArray + leftCnt, 0, (this->leafOccupancy - leftCnt) * sizeof(int));
	node->keyArrLength = leftCnt;
	newNode->keyArrLength = this->leafOccupancy - leftCnt + 1;

	// set sibling ptr
	newNode->rightSibPageNo = node->rightSibPageNo;
	node->rightSibPageNo = newNodePageId;
	int* ret_key = &newNode->keyArray[0];

	for(int i = 0; i < 5; i ++) {
		printf("%d ", tempKey[i]);
	}
	printf("\n");
	
	this->bufMgr->unPinPage(this->file, newNodePageId, true);
	return std::pair<PageId, int*>(newNodePageId, ret_key);
}

const std::pair<PageId, int*> BTreeIndex::splitNonLeafNodeInt(struct NonLeafNodeInt* node, PageId left, PageId right, int* key) {
	// allocate a new leaf
	Page* newNodePage;
	PageId newNodePageId;
	this->bufMgr->allocPage(this->file, newNodePageId, newNodePage);

	struct NonLeafNodeInt* newNode = (NonLeafNodeInt*) newNodePage;
	memset(newNode->keyArray, 0, sizeof(newNode->keyArray));
	memset(newNode->pageNoArray, 0, sizeof(newNode->pageNoArray));
	int leftCnt = (this->nodeOccupancy) / 2;

	// map key to pageId
	std::unordered_map<int, PageId> map;
	for(int i = 0; i < this->nodeOccupancy; i ++) {
		map.insert({node->keyArray[i], node->pageNoArray[i + 1]});
	}
	map.insert({*key, right});

	int tempKey[this->nodeOccupancy + 1];
	bool inserted = false;
	for(int i = 0; i < this->nodeOccupancy; i ++) {
		if(*key < node->keyArray[i] && !inserted) {
			tempKey[i] = *key;
			inserted = true;
			continue;
		}

		if(!inserted) {
			tempKey[i] = node->keyArray[i];
		} else {
			tempKey[i] = node->keyArray[i - 1];
		}
	}
	if(!inserted) {
		tempKey[this->leafOccupancy] = *(int*)key;
	} else {
		tempKey[this->leafOccupancy] = node->keyArray[this->leafOccupancy - 1];
	}

	for(int i = 0; i < this->nodeOccupancy - leftCnt; i ++) {
		newNode->keyArray[i] = tempKey[i + leftCnt + 1];
	}

	for(int i = 0; i < leftCnt + 1; i ++) {
		newNode->pageNoArray[i] = map[tempKey[i + leftCnt + 1]];
	}

	// clear left node
	memset(node->keyArray + leftCnt, 0, (this->nodeOccupancy - leftCnt) * sizeof(int));
	memset(node->pageNoArray + leftCnt + 1, 0, (this->nodeOccupancy - leftCnt) * sizeof(int));
	node->keyArrLength = leftCnt;
	newNode->keyArrLength = this->nodeOccupancy - leftCnt;
	int* ret_key = &newNode->keyArray[0];
	printf("%d\n", newNode->keyArray[0]);
	this->bufMgr->unPinPage(this->file, newNodePageId, true);
	return std::pair<PageId, int*>(newNodePageId, ret_key);
}

// -----------------------------------------------------------------------------
// BTreeIndex::insertRecursive
// -----------------------------------------------------------------------------
// remember to unpin pages
const std::pair<std::pair<PageId, PageId>, void*> BTreeIndex::insertRecursive(PageId root, const void *key, 
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
				this->bufMgr->unPinPage(this->file, root, true);
				//printf("hehe9\n");
				return std::pair<std::pair<PageId, PageId>, void*>(std::pair<PageId, PageId>(root, 0), NULL);
			} else {
				// split
				std::pair<PageId, int*> res = splitLeafNodeInt(currNode, (int*)key, rid);
				this->bufMgr->unPinPage(this->file, root, true);
			    return std::pair<std::pair<PageId, PageId>, void*>(std::pair<PageId, PageId>(root, res.first), 
																  (void*)res.second);
			}
		} else {
			// not a leaf
			PageId nextNodeId = findPageNoInNonLeaf(node, key);
			struct NonLeafNodeInt* currNode = (NonLeafNodeInt*)node;
			int cur_level = currNode->level;
			this->bufMgr->unPinPage(this->file, root, false);
			//printf("hehe10\n");
			std::pair<std::pair<PageId, PageId>, void*> insertRes = insertRecursive(nextNodeId, key, rid, cur_level);
			//printf("hehe11 %d\n", (int*)insertRes.second);

			// read page
			this->bufMgr->readPage(this->file, root, node);
			currNode = (NonLeafNodeInt*)node; 

			// return from recursive call, propagate up
			if(insertRes.second != NULL) {
				if(currNode->keyArrLength < this->nodeOccupancy) {
					insertIntKeyToNonLeaf(currNode,(int*)insertRes.second, 
					                      insertRes.first.second);
					this->bufMgr->unPinPage(this->file, root, true);
					return std::pair<std::pair<PageId, PageId>, void*>(std::pair<PageId, PageId>(root, 0), NULL);
				} else {
					std::pair<PageId, int*> res = splitNonLeafNodeInt(currNode, insertRes.first.first, 
					                                                  insertRes.first.second, (int*)insertRes.second);
																	  
					this->bufMgr->unPinPage(this->file, root, true);
					return std::pair<std::pair<PageId, PageId>, void*>(std::pair<PageId, PageId>(root, res.first), 
																      (void*)res.second);
				}
			}
			this->bufMgr->unPinPage(this->file, root, false);
			return std::pair<std::pair<PageId, PageId>, void*>(std::pair<PageId, PageId>(root, 0), NULL);
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
	Page* leafPage;
	PageId leafPageId;

	if(this->attributeType == INTEGER) {
		struct NonLeafNodeInt* rootNode = (NonLeafNodeInt*) rootPage;
		if(rootNode->keyArrLength == 0) {
			rootNode->keyArray[0] = *((int*)key);
			
			// allocate leaf
			this->bufMgr->allocPage(this->file, leafPageId, leafPage);
			rootNode->pageNoArray[1] = leafPageId;
			rootNode->keyArrLength ++;
			//printf("leaf page id1 %d\n", leafPageId);
		}
	} else if(this->attributeType == DOUBLE) {
		struct NonLeafNodeDouble* rootNode = (NonLeafNodeDouble*) rootPage;
		if(rootNode->keyArrLength == 0) {
			rootNode->keyArray[0] = *((double*)key);
			
			// allocate leaf
			this->bufMgr->allocPage(this->file, leafPageId, leafPage);
			rootNode->pageNoArray[1] = leafPageId;
			rootNode->keyArrLength ++;
		}

	} else {
		struct NonLeafNodeString* rootNode = (NonLeafNodeString*) rootPage;
		if(rootNode->keyArrLength == 0) {
			strcpy(rootNode->keyArray[0], (char*)key);
			
			// allocate leaf
			this->bufMgr->allocPage(this->file, leafPageId, leafPage);
			rootNode->pageNoArray[1] = leafPageId;
			rootNode->keyArrLength ++;
		}
	}

	//printf("hehe2\n");
	// recursively insert
	std::pair<std::pair<PageId, PageId>, void*> res = insertRecursive(this->rootPageNum, key, rid, 0);
	
	//printf("leaf page id2 %d\n", leafPageId);
	//printf("hehe3\n");
	// root is split
	if(res.second != NULL) {
		Page* newRootPage;
		PageId newRootPageId;
		this->bufMgr->allocPage(this->file, newRootPageId, newRootPage);
		
		if(this->attributeType == INTEGER) {
			NonLeafNodeInt* rootNode = (NonLeafNodeInt*)newRootPage;
			//printf("%d\n", *((int*)res.second));
			rootNode->keyArray[0] = *((int*)res.second);
			rootNode->pageNoArray[0] = res.first.first;
			rootNode->pageNoArray[1] = res.first.second;
			rootNode->level = 0;
			rootNode->keyArrLength = 1;
			this->rootPageNum = newRootPageId;
			
			//printf("hehe5\n");
			// set index meta page
			Page* headerPage;
			this->bufMgr->readPage(this->file, this->headerPageNum, headerPage);
			struct IndexMetaInfo* temp = (IndexMetaInfo*)headerPage;
			temp->rootPageNo = newRootPageId;	
		}
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
