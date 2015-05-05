/*
 * bPlusTree_data_structures.c
 *
 *
 *  Created on: Apr 28, 2015
 *      Author: vjonnala and team
 */

#include<stdio.h>
#include<stdlib.h>
#include<stdbool.h>
#include<string.h>
#include "bPlusTree_data_structures.h"
#include "buffer_mgr.h"
#include "dberror.h"
#include "btree_mgr.h"

char* serializeTreeEntries(BTreeHandle* btree)
{
	int offset=0;
	char* result=(char*)calloc(PAGE_SIZE, sizeof(char));

	//serialize idxId
	memcpy(result,btree->idxId,strlen(btree->idxId));
	result+=IDXID;
	offset+=IDXID;

	//serialize keyType
	memcpy(result,&btree->keyType,4);
	result+=4;
	offset+=4;

	//serialize info in the TreeMgr
	BPlus_TreeMgr* tm=(BPlus_TreeMgr*)btree->mgmtData;

	//serialize n
	memcpy(result,&tm->n,4);
	result+=4;
	offset+=4;

	//serialize num of entries
	memcpy(result,&tm->noOfEntries,4);
	result+=4;
	offset+=4;

	//serialize num of nodes
	memcpy(result,&tm->noOfNodes,4);
	result+=4;
	offset+=4;

	//serialize page num where root stores
	memcpy(result,&tm->root,4);
	result+=4;
	offset+=4;

	result-=offset;
	printf("Result = [%s]\n",result);
	return result;
}

void deserializeTreeEntries(BTreeHandle *bt, char* data)
{
	memcpy(bt->idxId,data,IDXID);
	data+=IDXID;

	memcpy(&bt->keyType,data,4);
	data+=4;

	BPlus_TreeMgr* tm=(BPlus_TreeMgr*)bt->mgmtData;
	memcpy(&tm->n,data,4);
	data+=4;

	memcpy(&tm->noOfEntries,data,4);
	data+=4;

	memcpy(&tm->noOfNodes,data,4);
	data+=4;

	memcpy(&tm->root,data,4);
}

char* serializeNode(BPlus_Node* node)
{
	int offset=0;
	char* result=(char*)calloc(PAGE_SIZE, sizeof(char));

	//serialize id
	memcpy(result,&node->pageNum,4);
	result+=4;
	offset+=4;

	//serialize numKey
	memcpy(result,&node->numKey,4);
	result+=4;
	offset+=4;

	//serialize isLeaf
	memcpy(result,&node->isRoot, ISLEAF);
	result+=ISLEAF;
	offset+=ISLEAF;

	//serialize next
	memcpy(result,&node->next,4);
	result+=4;
	offset+=4;

	//serialize parent
	memcpy(result,&node->parent,4);
	result+=4;
	offset+=4;

	//serialize keys
	int i;
	for(i=0;i<node->numKey;i++)
	{
		memcpy(result,&node->keys[i],4);
		result+=4;
		offset+=4;
	}

	//serialize entries
	if(node->isRoot)
	{
		for(i=0;i<node->numKey;i++)
		{
			memcpy(result,&node->rids[i].page,4);
			result+=4;
			offset+=4;
			memcpy(result,&node->rids[i].slot,4);
			result+=4;
			offset+=4;
		}
	}
	else//serialize children
	{
		if(node->numKey!=0)
			for(i=0;i<node->numKey+1;i++)
			{
				memcpy(result,&node->children[i],4);
				result+=4;
				offset+=4;
			}
	}

	result-=offset;

	return result;
}

void deserializeNode(BPlus_Node* node, char* data)
{
	memcpy(&node->pageNum,data,4);
	data+=4;

	memcpy(&node->numKey,data,4);
	data+=4;

	memcpy(&node->isRoot,data,ISLEAF);
	data+=ISLEAF;

	memcpy(&node->next,data,4);
	data+=4;

	memcpy(&node->parent,data,4);
	data+=4;

	int i;
	for(i=0;i<node->numKey;i++)
	{
		memcpy(&node->keys[i],data,4);
		data+=4;
	}

	if(node->isRoot)
	{
		for(i=0;i<node->numKey;i++)
		{
			memcpy(&node->rids[i].page,data,4);
			data+=4;
			memcpy(&node->rids[i].slot,data,4);
			data+=4;
		}
	}
	else
	{
		if(node->numKey!=0)
			for(i=0;i<node->numKey+1;i++)
			{
				memcpy(&node->children[i],data,4);
				data+=4;
			}
	}
}


BPlus_Node* createNode(BTreeHandle* tree)
{
	BPlus_TreeMgr* treeMgr = (BPlus_TreeMgr*)tree->mgmtData;
	BPlus_Node* node=(BPlus_Node*)malloc(sizeof(BPlus_Node));
	node->keys=(int*)malloc(sizeof(int)*treeMgr->n);
	node->rids=(RID*)malloc(sizeof(RID)*treeMgr->n);
	node->children=(int*)malloc(sizeof(int)*(treeMgr->n+1));
	node->pageNum=-1;
	node->isRoot=FALSE;
	node->next=-1;
	node->numKey=0;
	node->parent=-1;
	return node;
}

void saveNode(BTreeHandle* tree, BPlus_Node* node)
{
	//save the memory node into disk
	BPlus_TreeMgr* treeMgr = (BPlus_TreeMgr*)tree->mgmtData;
	BM_BufferPool *bm = treeMgr->bm;
	BM_PageHandle *bm_ph = treeMgr->bm_ph;
	PoolMgr *poolMgr = (PoolMgr*)bm->mgmtData;
	SM_FileHandle* fHandle = (SM_FileHandle *)malloc(sizeof(SM_FileHandle *));

	fHandle->curPagePos=node->pageNum;
	fHandle->fileName = tree->idxId;
	if(fHandle->totalNumPages<=fHandle->curPagePos)
		appendEmptyBlock(fHandle);

	CHECK(pinPage(bm,bm_ph,node->pageNum));
	bm_ph->data=serializeNode(node);
	printf("Data in Save Node Function is :- %s \n",bm_ph->data);
	CHECK(markDirty(bm, bm_ph));
	CHECK(unpinPage(bm, bm_ph));
}

void freeNode(BPlus_Node* node)
{
	free(node->keys);
	free(node->rids);
	free(node->children);
	free(node);
}


void initRoot(BTreeHandle* tree)
{
	BPlus_TreeMgr* treeMgr=(BPlus_TreeMgr*)tree->mgmtData;
	if(treeMgr->noOfNodes==0)
	{
		BPlus_Node* root=createNode(tree);
		root->pageNum=1;
		root->isRoot=TRUE;
		saveNode(tree,root);
		freeNode(root);
		treeMgr->noOfNodes++;
	}
}

BPlus_Node* loadNode(BTreeHandle* tree, int pageNum)
{
	//load the disk page node into memory node
	BPlus_TreeMgr* treeMgr = (BPlus_TreeMgr*)tree->mgmtData;
	BM_BufferPool *bm = treeMgr->bm;
	BM_PageHandle *bm_ph = treeMgr->bm_ph;

	BPlus_Node* node=createNode(tree);

	CHECK(pinPage(bm,bm_ph,pageNum));
	deserializeNode(node,bm_ph->data);
	CHECK(unpinPage(bm, bm_ph));

	return node;
}

int findChild(BPlus_Node* node, int target)
{
	int i,pos=0;
	for(i=0;i<node->numKey;i++)
	{
		if(target>=node->keys[i])
			pos=i+1;
	}
	return pos;
}

int findEntry(BPlus_Node* node, int target)
{
	//find the entry according to key
	//if is found, return the position of entry
	//else return -1
	int i,pos=-1;
	for(i=0;i<node->numKey;i++)
	{
		if(target==node->keys[i])
			pos=i;
	}
	return pos;
}

int findBucket(BTreeHandle* tree, int target, bool* isExist)
{
	//find the bucket that target should be inserted into
	//if key is already in the tree, return isExist =TRUE
	//return the bucket page number

	BPlus_TreeMgr* treeMgr = (BPlus_TreeMgr*)tree->mgmtData;
	int currentNode=treeMgr->root;
	int bucket=0;

	while(currentNode!=-1)
	{
		BPlus_Node* node=loadNode(tree,currentNode);
		int pos;
		if(!node->isRoot)
		{
			pos=findChild(node,target);
			currentNode=node->children[pos];
		}
		else
		{
			pos=findEntry(node,target);
			bucket=node->pageNum;
			if(pos!=-1)
				*isExist=TRUE;
			freeNode(node);
			break;
		}

	}

	return bucket;
}


void insertLeaf(BTreeHandle* tree, BPlus_Node* node, int target, RID rid)
{
	BPlus_TreeMgr* treeMgr=(BPlus_TreeMgr*)tree->mgmtData;

    if(!isFull(tree, node))
    {
        addEntry(node,target,rid);
    }
    else
    {
    	BPlus_Node* newNode=splitLeaf(tree,node,target,rid);
        treeMgr->noOfNodes++;//add node for the new splitted one
        if(node->parent!=-1)//has parent
        {
        	BPlus_Node* parent=loadNode(tree,node->parent);
            int target=newNode->keys[0];
            insertNonLeaf(tree,parent,newNode,target);
            saveNode(tree,parent);
            saveNode(tree,newNode);
            freeNode(parent);
            freeNode(newNode);
        }
        else
        {
        	BPlus_Node* root=createNode(tree);
            treeMgr->noOfNodes++;//add node for the new root
            treeMgr->root=treeMgr->noOfNodes;
            root->pageNum=treeMgr->noOfNodes;
            root->keys[0]=newNode->keys[0];
            root->numKey=1;
            root->children[0]=node->pageNum;
            root->children[1]=newNode->pageNum;
            node->parent=root->pageNum;
            newNode->parent=root->pageNum;
            saveNode(tree, root);
            saveNode(tree,newNode);
            freeNode(root);
            freeNode(newNode);
        }
    }
    treeMgr->noOfEntries++;
}

void insertNonLeaf(BTreeHandle* tree, BPlus_Node* node, BPlus_Node* child, int target)
{
	BPlus_TreeMgr* treeMgr=(BPlus_TreeMgr*)tree->mgmtData;

    if(!isFull(tree, node))
    {
        addChildren(node, target, child);
    }
    else
    {
        int separator=0;
        BPlus_Node* newNode=splitNonLeaf(tree,node,child, target, &separator);
        treeMgr->noOfNodes++;
        if(node->parent!=-1)
        {
        	BPlus_Node* parent=loadNode(tree, node->parent);
            insertNonLeaf(tree, parent, newNode, target);
            addChildren(node,target, child);
            saveNode(tree,parent);
            saveNode(tree,newNode);
            freeNode(parent);
            freeNode(newNode);
        }
        else
        {
        	BPlus_Node* root=createNode(tree);
            treeMgr->noOfNodes++;//add node for the new root
            treeMgr->root=treeMgr->noOfNodes;
            root->pageNum=treeMgr->noOfNodes;
            root->keys[0]=newNode->keys[0];
            root->numKey=1;
            root->children[0]=node->pageNum;
            root->children[1]=newNode->pageNum;
            node->parent=root->pageNum;
			newNode->parent=root->pageNum;
			saveNode(tree, root);
			saveNode(tree,newNode);
            freeNode(root);
            freeNode(newNode);
        }
    }
}
BPlus_Node* splitLeaf(BTreeHandle* tree, BPlus_Node* node, int target, RID rid)
{
	BPlus_TreeMgr* treeMgr=(BPlus_TreeMgr*)tree->mgmtData;
	BPlus_Node* newNode=createNode(tree);
	newNode->pageNum=treeMgr->noOfNodes+1;
    newNode->keys[0]=target;
    newNode->rids[0]=rid;
    newNode->isRoot = TRUE;
    redistributeLeaf(node,newNode);
	return newNode;
}
bool isFull(BTreeHandle* const tree, BPlus_Node* const node)
{
	BPlus_TreeMgr* treeMgr = (BPlus_TreeMgr*)tree->mgmtData;
	if(node->numKey==treeMgr->n)
		return TRUE;
	else return FALSE;
}

void addEntry(BPlus_Node* node, int target, RID rid)
{
	int pos=findChild(node,target);
	int i;
	//insert key and entry
	for(i=node->numKey-1;i>=pos;i--)
	{
		node->keys[i+1]=node->keys[i];
		node->rids[i+1]=node->rids[i];
	}
	node->keys[pos]=target;
	node->rids[pos]=rid;
	node->numKey++;
}

void redistributeLeaf(BPlus_Node* node, BPlus_Node* newNode)
{
	//move half the bucket's elements to the new bucket.
	int* key1=node->keys;
	int* key2=newNode->keys;
	int n=node->numKey+1;
	int i=0;
	int* keys = (int*)malloc(sizeof(int)*n);
	for (i = 0; i < n-1; i++)
	{
		keys[i] = key1[i];
	}
	keys[n-1] = key2[0];
	RID* rid1=node->rids;
	RID* rid2=newNode->rids;
	RID* rids = (RID*)malloc(sizeof(RID)*n);
	for (i = 0; i < n-1; i++)
	{
		rids[i]=rid1[i];
	}
	rids[n-1] = rid2[0];
	i = n-2;
	while (i >= 0)
	{
		if (keys[i] > keys[i+1])
		{
			int tempKey = keys[i];
			keys[i] = keys[i+1];
			keys[i+1] = tempKey;
			RID tempRid = rids[i];
			rids[i] = rids[i+1];
			rids[i+1] = tempRid;
			i--;
		} else
		{
			break;
		}
	}
	int middle=0;
	if(n%2)
		middle=n/2+1;
	else middle=n/2;
	for(i=0;i<n;i++)
	{
		if(i<middle)
		{
			node->keys[i]=keys[i];
			node->rids[i]=rids[i];
		}
		else
		{
			newNode->keys[i-middle]=keys[i];
			newNode->rids[i-middle]=rids[i];
		}
	}
	newNode->next=node->next;
	node->next=newNode->pageNum;
	node->numKey=middle;
	newNode->numKey=n-middle;
	free(keys);
	free(rids);
}
void addChildren(BPlus_Node* node, int target, BPlus_Node* child)
{
	//Insert the new leaf's smallest key and address into the parent.
    int pos=findChild(node, target);
    int i;
    //insert key and child
    for(i=node->numKey-1;i>=pos;i--)
    {
        node->keys[i+1]=node->keys[i];
        node->children[i+1]=node->children[i];
    }
    node->keys[pos]=target;
    node->children[pos+1]=child->pageNum;
    node->numKey++;
}

BPlus_Node* splitNonLeaf(BTreeHandle* tree, BPlus_Node* node, BPlus_Node* child, int target, int* separator)
{
	BPlus_Node* newNode=createNode(tree);
    newNode->keys[0]=target;
    newNode->children[0]=child->pageNum;
    int pos=findChild(node, target)+1;
    int i=node->numKey;
    for(;i>=pos;i--)
    {
        newNode->children[i-pos+1]=node->children[i];
        node->children[i]=-1;
    }
    *separator=redistributeNonLeaf(node, newNode);
    return newNode;
}

int redistributeNonLeaf(BPlus_Node* node, BPlus_Node* newNode)
{
    int* key1=node->keys;
    int* key2=newNode->keys;
    int n=node->numKey+1;
    int i=0;
    int* keys = (int*)malloc(sizeof(int)*n);
    for (i = 0; i < n-1; i++)
    {
        keys[i] = key1[i];
    }
    keys[n] = key2[0];
    i = n-1;
    while (i >= 0)
    {
        if (keys[i] > keys[i+1])
        {
            int tempKey = keys[i];
            keys[i] = keys[i+1];
            keys[i+1] = tempKey;
            i--;
        } else
        {
            break;
        }
    }
    int middle=n/2;
    int separator=0;
    for(i=0;i<n;i++)
    {
        if(i<middle)
        {
            node->keys[i]=keys[i];
        }
        else if(i==middle)
        {
            separator=keys[middle];
        }
        else
        {
            newNode->keys[i-middle]=keys[i];
        }
    }
    free(keys);
    return separator;
}
