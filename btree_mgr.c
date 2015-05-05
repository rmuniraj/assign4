#include "buffer_mgr.h"
#include "storage_mgr.h"
#include "node_linked_list.h"
#include "bPlusTree_data_structures.h"
#include "btree_mgr.h"
#include "dt.h"
#include "dberror.h"

#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <string.h>
#include <fcntl.h>

typedef struct btree_node {
	int keys1;
	int keys2;
	RID rID1;
	RID rID2;
	RID rID3;
	bool is_leaf;
	int num_keys;
	DataType keyType;
	struct btree_node * parent;
	struct btree_node * middle;
	struct btree_node * next; // Used for queue.
} btree_node;

// init and shutdown index manager
RC initIndexManager(void *mgmtData) {

	return RC_OK;
}
RC shutdownIndexManager() {

	return RC_OK;
}

// create, destroy, open, and close an btree index
RC createBtree(char *idxId, DataType keyType, int n) {
	//printf("------------[START]------IN CREATE B TREE----------------------\n");
	//create a file named of tree name
	CHECK(createPageFile(idxId));

	//open the buffer pool to r/w file
	BM_BufferPool *bm = MAKE_POOL();
	BM_PageHandle *bm_ph = MAKE_PAGE_HANDLE();
	CHECK(initBufferPool(bm, idxId, 3, RS_LRU, NULL));

	//store the tree information in the first page
	int i = 0;
	CHECK(pinPage(bm, bm_ph, i)); //read the empty first page into memory

	//init tree mgr
	BPlus_TreeMgr *bPlus_tm = (BPlus_TreeMgr*) malloc(sizeof(BPlus_TreeMgr));
	bPlus_tm->noOfEntries = 0;
	bPlus_tm->noOfNodes = 0;
	bPlus_tm->n = n;
	bPlus_tm->root = 1; //store the first root in page 1
	//init tree handle
	BTreeHandle *bt = (BTreeHandle*) malloc(sizeof(BTreeHandle));
	bt->keyType = keyType;
	bt->idxId = idxId;
	bt->mgmtData = bPlus_tm;

	char* result = serializeTreeEntries(bt); //we use serialize to write tree info
	memcpy(bm_ph->data, result, strlen(result)); //write the result into first page of page file

	CHECK(markDirty(bm, bm_ph));
	CHECK(unpinPage(bm, bm_ph));
	CHECK(shutdownBufferPool(bm));    //close the buffer pool
	free(result);
	result = NULL;
	return RC_OK;

}

RC openBtree(BTreeHandle **tree, char *idxId) {

	//open the buffer pool to r/w file
	BM_BufferPool *bm = MAKE_POOL();
	BM_PageHandle *bm_ph = MAKE_PAGE_HANDLE();
	CHECK(initBufferPool(bm, idxId, 3, RS_LRU, NULL));

	int i = 0;
	CHECK(pinPage(bm, bm_ph, i));    //read the first page into memory

	//malloc space for tree
	*tree = (BTreeHandle *) malloc(sizeof(BTreeHandle));
	BPlus_TreeMgr *bPlus_tm = (BPlus_TreeMgr*) malloc(sizeof(BPlus_TreeMgr));
	bPlus_tm->bm_ph = bm_ph;
	bPlus_tm->bm = bm;
	(*tree)->mgmtData = bPlus_tm;
	(*tree)->idxId = (char*) malloc(sizeof(char) * IDXID);

	//deserialize to fill up tree structure
	//store the tree information in the first page
	deserializeTreeEntries(*tree, bm_ph->data);
	CHECK(unpinPage(bm, bm_ph));
	//initRoot(*tree);
	return RC_OK;
}
RC closeBtree(BTreeHandle *tree) {

	//free every malloc space in tree
	BPlus_TreeMgr *bPlus_tm = (BPlus_TreeMgr*) tree->mgmtData;
	//CHECK(shutdownBufferPool(bPlus_tm->bm));
	free(bPlus_tm->bm_ph);
	free(bPlus_tm->bm);
	free(tree->mgmtData);
	free(tree->idxId);
	free(tree);
	return RC_OK;
}
RC deleteBtree(char *idxId) {

	remove(idxId);
	return RC_OK;
}

// access information about a b-tree
RC getNumNodes(BTreeHandle *tree, int *result) {

	int nodes = getsize(tree->idxId);
	*result = nodes;
	return RC_OK;
}
RC getNumEntries(BTreeHandle *tree, int *result) {

	int entries = (getsize(tree->idxId) - 1) * 2;
	*result = entries;
	return RC_OK;
}
RC getKeyType(BTreeHandle *tree, DataType *result) {

	return RC_OK;
}

RID deserialisedata(char* result,Value *key){
	RID rid;
	char *temp_buff[2];int i=0;int j=0;int k=0;int m =0;int n=0;char *t1[2];char *t2[2];
	char *r1[2];char *r2[2];
	char *buff = strtok(result, ";");
	while (buff != NULL) {
		//printf(buff);
		temp_buff[i] = buff;
		buff = strtok(NULL, ";");
		i++;
	}
	char *buff1 = strtok(temp_buff[0],",");
	while (buff1 != NULL) {
		//printf(buff);
		t1[j] = buff1;
		buff1 = strtok(NULL, ",");
		j++;
	}
	char *buff2 = strtok(temp_buff[1],",");
		while (buff2 != NULL) {
			//printf(buff);
			t2[k] = buff2;
			buff2 = strtok(NULL, ",");
			k++;
		}
		if(atoi(t1[1]) == key->v.intV){
			char *buff3 = strtok(t1[0],".");
			while (buff3 != NULL) {
					//printf(buff);
					r1[m] = buff3;
					buff3 = strtok(NULL, ".");
					m++;
				}
			rid.page = atoi(r1[0]);rid.slot = atoi(r1[1]);
		}
		if(atoi(t2[1]) == key->v.intV){
			char *buff4 = strtok(t2[0],".");
						while (buff4 != NULL) {
								//printf(buff);
								r2[n] = buff4;
								buff4 = strtok(NULL, ".");
								n++;
			}
						rid.page = atoi(r2[0]);rid.slot = atoi(r2[1]);
		}
return rid;
}
// index access
RC findKey (BTreeHandle *tree, Value *key, RID *result)
{
	BPlus_TreeMgr* treeMgr = (BPlus_TreeMgr*)tree->mgmtData;
	BM_BufferPool *bm = treeMgr->bm;
	BM_PageHandle *bm_ph = treeMgr->bm_ph;
	int noOfPages,i;
	getNumNodes(tree,&noOfPages);
	char *arr ;char *temp = malloc(2);char * sd;
	arr  = malloc(40 * sizeof(char *));
	for(i=1;i<noOfPages;i++){
		CHECK(pinPage(bm,bm_ph,i));
		strcpy(arr,bm_ph->data);
		sprintf(temp,"%d",key->v.intV);
		//temp = itoa(key->v.intV,temp,10);
		sd = strstr(arr, temp);
		if(sd != NULL){
			*result = deserialisedata(arr,key);
			break;
		}
	}
	//free(arr);free(temp);free(sd);
	return RC_OK;
}
int getsize(char *name){

	FILE *fpGS = fopen(name,"r+");
	struct stat status;
    	fstat(fileno(fpGS),&status);
	/*Get the file size using built-in function fstat()*/
	int FILE_SIZE = status.st_size;
	int numberOfBlocks;
	if(FILE_SIZE <= PAGE_SIZE){
	numberOfBlocks = 0;  // If the file size is less than zero , it will be the 0th Block.
	}
	else{
	numberOfBlocks = (FILE_SIZE / PAGE_SIZE) ;
	}
	//fclose(fpGS);
	return numberOfBlocks;
 }

char * serialisedata(char* result,Value *key,RID rid){
	char* page = malloc(1); char* slot = malloc(1);
	char *intv = malloc(2);
	sprintf(page, "%d", rid.page);sprintf(slot, "%d", rid.slot);sprintf(intv, "%d", key->v.intV);
	strcat(result, page);
	strcat(result, ".");
	strcat(result, slot);
	strcat(result, ",");
	strcat(result, intv);
	strcat(result, ";");
//	free(page);free(slot);free(intv);
	//strcp
	return result;
}
char * serialiseRootdata(char* result,Value *key,int nodePage){
	char* page = malloc(1);char *intv = malloc(2);sprintf(intv, "%d", key->v.intV);
	sprintf(page, "%d", nodePage);
	strcat(result,";");strcat(result,page);strcat(result,",");strcat(result,intv);
	//free(page);
	return result;
}
int numKeys=0;int pageNumber = 1;
RC insertKey(BTreeHandle *tree, Value *key, RID rid) {
	if(numKeys>0 && numKeys%2 == 0){
		pageNumber = pageNumber + 1;
		//insert into ROOT.
	}

	BPlus_TreeMgr* treeMgr = (BPlus_TreeMgr*)tree->mgmtData;
	BM_BufferPool *bm = treeMgr->bm;
	BM_PageHandle *bm_ph = treeMgr->bm_ph;
	SM_FileHandle* fHandle = (SM_FileHandle *)malloc(sizeof(SM_FileHandle *));

	fHandle->curPagePos=pageNumber;
	fHandle->fileName = tree->idxId;
	if(getsize(tree->idxId)<=fHandle->curPagePos){
		appendEmptyBlock(fHandle);
		if(pageNumber>1){
		CHECK(pinPage(bm,bm_ph,0));

		char *data = serialiseRootdata(bm_ph->data,key,pageNumber-1);
		bm_ph->pageNum = 0;
		CHECK(unpinPage(bm,bm_ph));
		}
	}

	CHECK(pinPage(bm,bm_ph,pageNumber));

	BPlus_Node* node=createNode(tree);
	char *data = serialisedata(bm_ph->data,key,rid);
	bm_ph->pageNum = pageNumber;
	CHECK(unpinPage(bm,bm_ph));
/*	insertLeaf(tree, node, 0, rid);
	saveNode(tree,node);*/


	freeNode(node);
	numKeys = numKeys + 1;
	return RC_OK;
}

RC deleteKey(BTreeHandle *tree, Value *key) {

	return RC_OK;
}
RC openTreeScan(BTreeHandle *tree, BT_ScanHandle **handle) {

	return RC_OK;
}
RC nextEntry(BT_ScanHandle *handle, RID *result) {

	return RC_OK;
}
RC closeTreeScan(BT_ScanHandle *handle) {

	return RC_OK;
}

// debug and test functions
char *printTree(BTreeHandle *tree) {

	return RC_OK;
}
