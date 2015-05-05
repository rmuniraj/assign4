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
void initRoot(BTreeHandle* tree);
int findChild(BPlus_Node* node, int target);
int findEntry(BPlus_Node* node, int target);
int findBucket(BTreeHandle* tree, int target,bool* isExist);
BPlus_Node* createNode(BTreeHandle* tree);
BPlus_Node* loadNode(BTreeHandle* tree, int pageNum);
void saveNode(BTreeHandle* tree, BPlus_Node* node);
void freeNode(BPlus_Node* node);

//used for insertion
bool isFull(BTreeHandle* const tree, BPlus_Node* const node);
void addEntry(BPlus_Node* node, int target, RID rid);
void addChildren(BPlus_Node* node, int target, BPlus_Node* child);
void redistributeLeaf(BPlus_Node* node, BPlus_Node* newNode);
int redistributeNonLeaf(BPlus_Node* node, BPlus_Node* newNode);
void insertLeaf(BTreeHandle* tree, BPlus_Node* node, int target, RID rid);
void insertNonLeaf(BTreeHandle* tree, BPlus_Node* node, BPlus_Node* child, int target);
BPlus_Node* splitLeaf(BTreeHandle* tree, BPlus_Node* node, int target, RID rid);
BPlus_Node* splitNonLeaf(BTreeHandle* tree, BPlus_Node* node, BPlus_Node* child, int target, int* separator);

//used for deletion
/*
void deleteLeaf(BTreeHandle* tree, BPlus_Node* node, int target);
bool hasMoreThanHalf(BTreeHandle* tree, BPlus_Node* node);
void removeEntry(BPlus_Node* node, int target);
bool hasLeftSib(BTreeHandle* tree, BPlus_Node* node);
bool hasRightSib(BTreeHandle* tree, BPlus_Node* node);
void borrowFromLeafSib(BTreeHandle* tree, BPlus_Node* sender, BPlus_Node* receiver);
void mergeLeaf(BTreeHandle* tree, BPlus_Node* mergedNode, BPlus_Node* node, int target);
BPlus_Node* loadLeftSib(BTreeHandle* tree, BPlus_Node* node);
BPlus_Node* loadRightSib(BTreeHandle* tree, BPlus_Node* node);
void deleteNonLeaf(BTreeHandle* tree, BPlus_Node* node, BPlus_Node* child, int target);
void removeChildren(BPlus_Node* node, BPlus_Node* child, int target);
*/

#endif /* BPLUSTREE_DATA_STRUCTURES_H_ */
