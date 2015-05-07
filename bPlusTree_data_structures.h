/*
 * bmTree_data_structures.h
 *
 *  Created on: Apr 28, 2015
 *      Author: vjonnala and team
 */

#ifndef BPLUSTREE_DATA_STRUCTURES_H_
#define BPLUSTREE_DATA_STRUCTURES_H_
#include "btree_mgr.h"
#include "buffer_mgr.h"
#include "storage_mgr.h"

#define IDXID 30 //define the max length of idxid
#define ISLEAF 2 //if the node is a leaf

typedef struct BPlus_TreeMgr
{
	int noOfNodes;
	int noOfEntries;
	int n;
	int root;
	BM_BufferPool* bm;
	BM_PageHandle *bm_ph;
}BPlus_TreeMgr;

typedef struct BPlus_TreeScanMgr
{
	// current node page number
    int node;
    // current entry number
    int entry;
}BPlus_TreeScanMgr;

typedef struct BPlus_Node
{
	//page number of this node
	int pageNum;
	int numKey;
	bool isRoot;
	//page number of next node
	int next;
	//page number of parent node
	int parent;
	int* children;
	int* keys;
	RID* rids;
}BPlus_Node;

typedef struct PoolManager
{
	SM_FileHandle fHandle;
	int maxFlag;//store the maximum of time stamp of frames list
	PageNumber *frameContent;
    bool *dirty;
	int *fixCount;
	//Frame* head;
}PoolMgr;

//serialize and deserialize
char* serializeTreeEntries(BTreeHandle* btree);
void deserializeTreeEntries(BTreeHandle *bt, char* data);
char* serializeNode(BPlus_Node* node);
void deserializeNode(BPlus_Node* node, char* data);

//used for building tree
BPlus_Node* createNode(BTreeHandle* tree);
void freeNode(BPlus_Node* node);


#endif /* BPLUSTREE_DATA_STRUCTURES_H_ */
