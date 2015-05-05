/*
#include "buffer_mgr.h"
#include "storage_mgr.h"
#include "node_linked_list.h"
#include "btree_mgr.h"
#include "dt.h"
#include "dberror.h"
typedef struct btree_node {
	int keys1;
	int keys2;
	RID rID1;RID rID2;RID rID3;
	bool is_leaf;
	int num_keys;
	DataType keyType;
	struct btree_node * parent;
	struct btree_node * middle;
	struct btree_node * next; // Used for queue.
} *btree_node_root,*btree_node_leaf1,*btree_node_leaf2,*btree_node_leaf3,*btree_node_leaf4;
bool inserKey_into_Leaf(struct btree_node *btree_node_leaf1, Value *key, RID rid);
// init and shutdown index manager
RC initIndexManager(void *mgmtData) {

	return RC_OK;
}
RC shutdownIndexManager() {

	return RC_OK;
}
// create, destroy, open, and close an btree index
RC createBtree(char *idxId, DataType keyType, int n) {
	printf("------------[START]------IN CREATE B TREE----------------------\n");
	struct btree_node *btree_node_root;
	btree_node_root = (struct btree_node *) malloc(sizeof(struct btree_node));
	if (btree_node_root == NULL) {
		return 303;
	}
	btree_node_root->keyType = keyType;
	btree_node_root->keys1 = -1;
	btree_node_root->keys2 = -1;
	BM_BufferPool *bm = MAKE_POOL();
	BM_PageHandle *bm_ph = MAKE_PAGE_HANDLE();

	createPageFile(idxId);
	initBufferPool(bm, idxId, 3, RS_FIFO, NULL);

	pinPage(bm, bm_ph, 0);
	markDirty(bm, bm_ph);

	unpinPage(bm, bm_ph);
	free(bm);
	free(bm_ph);
	printf("-----------[END]-------IN CREATE B TREE----------------------\n");
	return RC_OK;
}
RC openBtree(BTreeHandle **tree, char *idxId) {

	return RC_OK;
}
RC closeBtree(BTreeHandle *tree) {

	return RC_OK;
}
RC deleteBtree(char *idxId) {

	return RC_OK;
}

// access information about a b-tree
RC getNumNodes(BTreeHandle *tree, int *result) {
	int i = 4;
	result = &i;
	printf("----%d-----\n", *result);
	return RC_OK;
}
RC getNumEntries(BTreeHandle *tree, int *result) {

	return RC_OK;
}
RC getKeyType(BTreeHandle *tree, DataType *result) {

	return RC_OK;
}

// index access
RC findKey(BTreeHandle *tree, Value *key, RID *result) {

	return RC_OK;
}
void display_btree_leafs(struct btree_node *btree_node_leaf1) {

	struct btree_node *new_node = btree_node_leaf1;

	 while (new_node != NULL){
		printf("KEY 1= [%d] and SLOT [%d] and PAGE [%d] \n",new_node->keys1, new_node->rID1.slot, new_node->rID1.page);
		printf("KEY 2= [%d] and SLOT [%d] and PAGE [%d] \n",new_node->keys2, new_node->rID2.slot, new_node->rID2.page);
		new_node = new_node->next;
	};

}
bool inserKey_into_Leaf(struct btree_node *btree_node_leaf1, Value *key, RID rid){
	if(btree_node_leaf1->num_keys != 2){
			if(btree_node_leaf1->num_keys != 1){
				btree_node_leaf1->keys1 = key->v.intV;
				btree_node_leaf1->num_keys = 1;
				btree_node_leaf1->keyType = DT_INT;
				btree_node_leaf1->rID1=rid;
			}
			else if(btree_node_leaf1->num_keys != 2){
				btree_node_leaf1->keys2 = key->v.intV;
				btree_node_leaf1->num_keys = 2;
				btree_node_leaf1->keyType = DT_INT;
				btree_node_leaf1->rID2=rid;
			}
			btree_node_leaf1->next = NULL;
			printf("LEAF1 :- KEY1 = %d\n",btree_node_leaf1->keys1);
			printf("LEAF1 :- KEY2 = %d\n",btree_node_leaf1->keys2);

			return FALSE;
	}
		else{
			//splitNonLeaf();
			printf("LEAF1 Full !! Split LEAF1 !!\n");

			return TRUE;
		}

}
inserKey_into_Root(struct btree_node *btree_node_root, Value *key, RID rid){
	if(btree_node_root->num_keys != 2){
				if(btree_node_root->num_keys != 1){
					btree_node_root->num_keys = 1;
					btree_node_root->keys1 = key->v.intV;
					btree_node_root->keyType = DT_INT;
					btree_node_root->rID1=rid;
				}
				else if(btree_node_root->num_keys != 2){
					btree_node_root->keys2 = key->v.intV;
					btree_node_root->num_keys = 2;
					btree_node_root->keyType = DT_INT;
					btree_node_root->rID2=rid;
				}
				printf("LEAF2 :- KEY1 = %d\n",btree_node_root->keys1);
				printf("LEAF2 :- KEY2 = %d\n",btree_node_root->keys2);
		}
			else{
				//splitNonLeaf();
				printf("ROOT Full !! Split ROOT !!\n");

			}

}
int splitLeaf(struct btree_node *btree_node_root,struct btree_node *btree_node_leaf, Value *key, RID rid){
	if(btree_node_leaf->num_keys != 2){
			if(btree_node_leaf->num_keys != 1){
				btree_node_leaf->keys1 = key->v.intV;
				btree_node_leaf->num_keys = 1;
				btree_node_leaf->keyType = DT_INT;
				btree_node_leaf->rID1=rid;
				inserKey_into_Root(btree_node_root,key,rid);
				printf("LEAF2 :- KEY1 = %d\n",btree_node_leaf->keys1);
			}
			else if(btree_node_leaf->num_keys != 2){
				btree_node_leaf->keys2 = key->v.intV;
				btree_node_leaf->num_keys = 2;
				btree_node_leaf->keyType = DT_INT;
				btree_node_leaf->rID2=rid;
				printf("LEAF2 :- KEY2 = %d\n",btree_node_leaf->keys2);
			}
			btree_node_leaf->next = NULL;
			return btree_node_leaf->num_keys;
	}
		else{
			//splitNonLeaf();
			printf("LEAF2 Full !! Split LEAF2 !!\n");
			return 0;
		}
}
RC insertKey(BTreeHandle *tree, Value *key, RID rid) {
	struct btree_node *btree_node_leaf1;struct btree_node *btree_node_leaf2;

	struct btree_node *btree_node_leaf3;struct btree_node *btree_node_root;
	if(btree_node_leaf3 == NULL || btree_node_root == NULL || btree_node_leaf1 == NULL || btree_node_leaf2 == NULL){
	btree_node_leaf3 = malloc(sizeof(struct btree_node *));btree_node_leaf1 = malloc(sizeof(struct btree_node *));
	btree_node_root = malloc(sizeof(struct btree_node *));btree_node_leaf2 = malloc(sizeof(struct btree_node *));
	}

	bool leaf1full = inserKey_into_Leaf(btree_node_leaf1,key,rid);
	printf("LEAF 1 FULL/NOT = %d\n",leaf1full);
	if(leaf1full){
		printf("Leaf 1 Full ! Split Leaf !!!\n");
		btree_node_leaf1->next = btree_node_leaf2;
		int leaf2full = splitLeaf(btree_node_root,btree_node_leaf2,key,rid);
		if(leaf2full == 0){
			printf("Leaf 2 is also full !! Inserted into ROOT also!!!\n");
			btree_node_leaf2->next = btree_node_leaf3;
			int leaf3full = splitLeaf(btree_node_root,btree_node_leaf3,key,rid);
			if(leaf3full == 0){
				printf("Leaf 3 is also full !! Inserted into ROOT also!!!\n");
				btree_node_root->parent=btree_node_leaf1;
				btree_node_root->middle=btree_node_leaf2;
				btree_node_root->next=btree_node_leaf3;
			}
		}

	}
	display_btree_leafs(btree_node_leaf1);
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
*/
