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



void freeNode(BPlus_Node* node)
{
	free(node->keys);
	free(node->rids);
	free(node->children);
	free(node);
}



