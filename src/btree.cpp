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
	this->rootPageNum = -1;

	// build index meta info
	const char* tempName = relationName.c_str();
	struct IndexMetaInfo metaInfo = {.attrByteOffset = attrByteOffset, .attrType = attrType, .rootPageNo = -1};
	strcpy(metaInfo.relationName, tempName);
	
	// write meta info to index file
	PageId metaPageId;
	Page* headerPage;
	bufMgr->allocPage(newFile, metaPageId, headerPage);
	struct IndexMetaInfo* temp = reinterpret_cast <struct IndexMetaInfo*>(headerPage);
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
		struct NonLeafNodeInt* temp = reinterpret_cast <struct NonLeafNodeInt*>(node);

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
		struct NonLeafNodeDouble* temp = reinterpret_cast <struct NonLeafNodeDouble*>(node);

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
		struct NonLeafNodeString* temp = reinterpret_cast <struct NonLeafNodeString*>(node);
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

// -----------------------------------------------------------------------------
// BTreeIndex::insertRecursive
// -----------------------------------------------------------------------------
const void BTreeIndex::insertRecursive(PageId root, const void *key, const RecordId rid) {
	// check if current node is leaf or not
	Page* node;
	this->bufMgr->readPage(this->file, root, node);

	if(this->attributeType == INTEGER) {
		if(true/* is a leaf*/) {
			PageId nextNodeId = findPageNoInNonLeaf(node, key);
			insertRecursive(nextNodeId, key, rid);
		} else {
			// not a leaf
		}
	} else if(this->attributeType == DOUBLE) {
		if(true/* is a leaf*/) {
			PageId nextNodeId = findPageNoInNonLeaf(node, key);
			insertRecursive(nextNodeId, key, rid);
		} else {
			// not a leaf
		}
	} else {
		if(true/* is a leaf*/) {
			PageId nextNodeId = findPageNoInNonLeaf(node, key);
			insertRecursive(nextNodeId, key, rid);
		} else {
			// not a leaf 
		}
	}
	
}

// -----------------------------------------------------------------------------
// BTreeIndex::insertEntry
// -----------------------------------------------------------------------------

const void BTreeIndex::insertEntry(const void *key, const RecordId rid) {
	// if no root node, insert root node
	if(this->rootPageNum == -1) {
		PageId rootPageId;
		Page* rootPage;
		bufMgr->allocPage(this->file, rootPageId, rootPage);
		this->rootPageNum = rootPageId;

		// create root node
		if(this->attributeType == INTEGER) {
			struct LeafNodeInt rootNode = {.rightSibPageNo = -1};
			rootNode.keyArray[0] = *((int*)key);
			rootNode.ridArray[0] = rid;
			struct LeafNodeInt* temp = reinterpret_cast <struct LeafNodeInt*>(rootPage);
			*temp = rootNode;
			this->bufMgr->unPinPage(this->file, rootPageId, true);
			
		} else if(this->attributeType == DOUBLE) {
			struct LeafNodeDouble rootNode = {.rightSibPageNo = -1};
			rootNode.keyArray[0] = *((double*)key);
			rootNode.ridArray[0] = rid;
			struct LeafNodeDouble* temp = reinterpret_cast <struct LeafNodeDouble*>(rootPage);
			*temp = rootNode;
			this->bufMgr->unPinPage(this->file, rootPageId, true);

		} else {
			struct LeafNodeString rootNode = {.rightSibPageNo = -1};
			strcpy(rootNode.keyArray[0], (char*)key);
			rootNode.ridArray[0] = rid;
			struct LeafNodeString* temp = reinterpret_cast <struct LeafNodeString*>(rootPage);
			*temp = rootNode;
			this->bufMgr->unPinPage(this->file, rootPageId, true);
		}
	}

	// recursively insert
	insertRecursive(this->rootPageNum, key, rid);
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
