#include "dberror.h"
#include "expr.h"
#include "tables.h"
#include "buffer_mgr.h"
#include "record_mgr.h"
#include "storage_mgr.h"
#include <stdlib.h>
#include <string.h>

#define FIELD_DELIMITER ","
#define TUPLE_DELIMITER ";"

// table and manager
RC initRecordManager(void *mgmtData) {
	return RC_OK;
}
RC shutdownRecordManager() {
	return RC_OK;
}
char* serialize_data(char* schema_info, Schema *schema) {
	char* numberofAttributes = (char *) malloc(1);
	// Convert the integer to char* using sprintnf
	sprintf(numberofAttributes, "%d", schema->numAttr);
	// Append both the values using strcat
	strcat(schema_info, numberofAttributes);
	strcat(schema_info, "\n");
	strcat(schema_info, schema->attrNames[0]);
	strcat(schema_info, ",");
	strcat(schema_info, schema->attrNames[1]);
	strcat(schema_info, ",");
	strcat(schema_info, schema->attrNames[2]);
	strcat(schema_info, "\n");
	sprintf(numberofAttributes, "%d", schema->dataTypes[0]);
	strcat(schema_info, numberofAttributes);
	strcat(schema_info, ",");
	sprintf(numberofAttributes, "%d", schema->dataTypes[1]);
	strcat(schema_info, numberofAttributes);
	strcat(schema_info, ",");
	sprintf(numberofAttributes, "%d", schema->dataTypes[2]);
	strcat(schema_info, numberofAttributes);
	strcat(schema_info, "\n");
	sprintf(numberofAttributes, "%d", schema->typeLength[0]);
	strcat(schema_info, numberofAttributes);
	strcat(schema_info, ",");
	sprintf(numberofAttributes, "%d", schema->typeLength[1]);
	strcat(schema_info, numberofAttributes);
	strcat(schema_info, ",");
	sprintf(numberofAttributes, "%d", schema->typeLength[2]);
	strcat(schema_info, numberofAttributes);

	return schema_info;
}

RC createTable(char *name, Schema *schema) {
	printf("----START----[CREATE TABLE]------------\n");

	BM_BufferPool *bm = MAKE_POOL();
	BM_PageHandle *bm_ph = MAKE_PAGE_HANDLE();

	createPageFile(name);
	initBufferPool(bm, name, 3, RS_FIFO, NULL);

	pinPage(bm, bm_ph, 0);
	markDirty(bm, bm_ph);

	printf(
			"Schema[0] values are attrNames[%s] , dataTypes[%d] , typeLength[%d] \n",
			schema->attrNames[0], schema->dataTypes[0], schema->typeLength[0]);
	printf(
			"Schema[1] values are attrNames[%s] , dataTypes[%d] , typeLength[%d] \n",
			schema->attrNames[1], schema->dataTypes[1], schema->typeLength[1]);
	printf(
			"Schema[2] values are attrNames[%s] , dataTypes[%d] , typeLength[%d] \n",
			schema->attrNames[2], schema->dataTypes[2], schema->typeLength[2]);

	char* schema_info = NULL;
	schema_info = (char *) malloc(strlen(name));
	memcpy(schema_info, name, strlen(name));
	strcat(schema_info, "\n");

	// Call the Serialize Method to convert all the data types to character pointer and store it in the first page.
	schema_info = serialize_data(schema_info, schema);

	//Copy the resulting Schema info to BM Page Handle Data.
	memcpy(bm_ph->data, schema_info, strlen(schema_info));
	printf("Data [%s] and Info [%s] \n", bm_ph->data, schema_info);
	/*	free(schema_info);
	 */unpinPage(bm, bm_ph);

	printf("----END----[CREATE TABLE]------------\n");
	return RC_OK;
}

RM_TableData* deserialize_data(RM_TableData *rel, char *data, Schema *schema) {

	//strcpy(data,"test_table_t\n3\na,b,c\n0,1,0\n0,4,0");
	printf("Test data = [%s] \n",data);
	int i = 0, j = 0, k = 0, l = 0;
	char *temp_buff[5];
	char *temp_buff2[3];
	DataType temp_buff3[3];
	int temp_buff4[3];
	char *buff = strtok(data, "\n");
	while (buff != NULL) {
		//printf(buff);
		temp_buff[i] = buff;
		buff = strtok(NULL, "\n");
		i++;
	}
	char *buff2 = strtok(temp_buff[2], ",");
	while (buff2 != NULL) {
		//printf(buff);
		temp_buff2[j] = buff2;
		buff2 = strtok(NULL, ",");
		j++;
	}
	char *buff3 = strtok(temp_buff[3], ",");
	while (buff3 != NULL) {
		//printf(buff);
		temp_buff3[k] = atoi(buff3);
		buff3 = strtok(NULL, ",");
		k++;
	}
	char *buff4 = strtok(temp_buff[4], ",");
	while (buff4 != NULL) {
		//printf(buff);
		temp_buff4[l] = atoi(buff4);
		buff4 = strtok(NULL, ",");
		l++;
	}

	int m;
	char **cpNames1 = (char **) malloc(sizeof(char*) * 3);
	for (m = 0; m < 3; m++) {
		cpNames1[m] = (char *) malloc(2);
		strcpy(cpNames1[m], temp_buff2[m]);
	}

	//Schema *schema;
	schema->attrNames = cpNames1;

	char *asl = temp_buff[1];
	int z = atoi(asl);
	schema->numAttr = z;

	int *cpSizes1 = (int *) malloc(sizeof(int) * 3);
	memcpy(cpSizes1, temp_buff4, sizeof(int) * 3);
	schema->typeLength = cpSizes1;

	DataType *cpDt1 = (DataType *) malloc(sizeof(DataType) * 3);
	memcpy(cpDt1, temp_buff3, sizeof(DataType) * 3);
	schema->dataTypes = cpDt1;

	schema->keySize = 0;

	rel->schema = schema;

	return rel;
}

RC openTable(RM_TableData *rel, char *name) {
	printf("----START----[OPEN TABLE]------------\n");
	BM_BufferPool *bm = MAKE_POOL();
	BM_PageHandle *bm_ph = MAKE_PAGE_HANDLE();

	initBufferPool(bm, name, 3, RS_FIFO, NULL);

	pinPage(bm, bm_ph, 0);
	markDirty(bm, bm_ph);
	//sRM_TableData *rel1;
	Schema *schema;
	rel = deserialize_data(rel, bm_ph->data, schema);
	//rel->schema = rel1->schema;
	rel->name = name;
	rel->mgmtData = (void *) bm;
	printf(
			"Schema[0] values are attrNames[%s] and NoAttributes[%d] and TypeLength = cpSizes[%d] and DataTypes = cpDt1[%d]\n",
			rel->schema->attrNames[0], rel->schema->numAttr,
			rel->schema->typeLength[0], rel->schema->dataTypes[0]);
	printf(
			"Schema[1] values are attrNames[%s] and NoAttributes[%d] and TypeLength = cpSizes[%d] and DataTypes = cpDt1[%d]\n",
			rel->schema->attrNames[1], rel->schema->numAttr,
			rel->schema->typeLength[1], rel->schema->dataTypes[1]);
	printf(
			"Schema[2] values are attrNames[%s] and NoAttributes[%d] and TypeLength = cpSizes[%d] and DataTypes = cpDt1[%d]\n",
			rel->schema->attrNames[2], rel->schema->numAttr,
			rel->schema->typeLength[2], rel->schema->dataTypes[2]);

	printf("----END----[OPEN TABLE]------------\n");
	return RC_OK;
}
RC closeTable(RM_TableData *rel) {
	printf("----START----[CLOSE TABLE]------------\n");
	BM_BufferPool *bm = (BM_BufferPool *) rel->mgmtData;
	/*shutdownBufferPool(bm);
	SM_FileHandle *fh;
	fh->fileName=bm->pageFile;
	closePageFile(fh);*/

	printf("----END----[CLOSE TABLE]------------\n");
	return RC_OK;
}
RC deleteTable(char *name) {
	printf("----START----[DELETE TABLE]------------\n");

	printf("----END----[DELETE TABLE]------------\n");
	return RC_OK;
}
int getNumTuples(RM_TableData *rel) {
	printf("----START----[GET NUM TUPLES]------------\n");

	printf("----END----[GET NUM TUPLES]------------\n");
	return RC_OK;
}

// handling records in a table
RC insertRecord(RM_TableData *rel, Record *record) {
	printf("----START----[INSERT RECORD]------------\n");
	BM_BufferPool *bm = (BM_BufferPool *) rel->mgmtData;
	printf("File Name is [%s] and Number [%d] and strat [%d] Data is [%s]\n",
			bm->pageFile, bm->numPages, bm->strategy, record->data);
	BM_PageHandle *bm_ph = MAKE_PAGE_HANDLE();
	pinPage(bm, bm_ph, 1);
	printf("Before Append [%s]\n", bm_ph->data);
	bm_ph->data = strcat(bm_ph->data, record->data);
	printf("After Append [%s]\n", bm_ph->data);
	markDirty(bm, bm_ph);
	unpinPage(bm, bm_ph);

	printf("----END----[INSERT RECORD]------------\n");
	return RC_OK;
}
RC deleteRecord(RM_TableData *rel, RID id) {
	printf("----START----[DELETE RECORD]------------\n");

	printf("----END----[DELETE RECORD]------------\n");
	return RC_OK;
}
RC updateRecord(RM_TableData *rel, Record *record) {
	printf("----START----[UPDATE RECORD]------------\n");

	printf("----END----[UPDATE RECORD]------------\n");
	return RC_OK;
}
RC getRecord(RM_TableData *rel, RID id, Record *record) {
	printf("----START----[GET RECORD]------------\n");

	printf("----END----[GET RECORD]------------\n");
	return RC_OK;
}

// scans
RC startScan(RM_TableData *rel, RM_ScanHandle *scan, Expr *cond) {
	printf("----START----[START SCAN]------------\n");

	printf("----END----[START SCAN]------------\n");
	return RC_OK;
}
RC next(RM_ScanHandle *scan, Record *record) {
	printf("----START----[NEXT]------------\n");

	printf("----END----[NEXT]------------\n");
	return RC_OK;
}
RC closeScan(RM_ScanHandle *scan) {
	printf("----START----[CLOSE SCAN]------------\n");

	printf("----END----[CLOSE SCAN]------------\n");
	return RC_OK;
}

// dealing with schemas
int getRecordSize(Schema *schema) {
	printf("----START----[GET RECORD SIZE]------------\n");

	int record_size = 0, i = 0;
	// based on number of attributes
	for (i = 0; i < schema->numAttr; i++) {
		// Decide on type length and add that to schema
		if (schema->typeLength[i] == 0)
			record_size = record_size + sizeof(DT_INT);
		else
			record_size = record_size + schema->typeLength[i];
	}
	return record_size;

	printf("----END----[GET RECORD SIZE]------------\n");
	return RC_OK;
}
Schema *createSchema(int numAttr, char **attrNames, DataType *dataTypes,
		int *typeLength, int keySize, int *keys) {
	printf("----START----[CREATE SCHEMA]------------\n");
	Schema* schema = (Schema*) malloc(sizeof(Schema));

	// Assign all attributes of the schema passed
	schema->numAttr = numAttr;
	schema->attrNames = attrNames;
	schema->dataTypes = dataTypes;
	schema->typeLength = typeLength;
	schema->keyAttrs = keys;
	schema->keySize = keySize;

	printf("----END----[CREATE SCHEMA]------------\n");
	// return schema
	return schema;
}
RC freeSchema(Schema *schema) {
	printf("----START----[FREE SCHEMA]------------\n");

	printf("----END----[FREE SCHEMA]------------\n");
	return RC_OK;
}

// dealing with records and attribute values
RC createRecord(Record **record, Schema *schema) {
	printf("----START----[CREATE RECORD]------------\n");

	int rec_size;
	(*record) = (Record*) malloc(sizeof(Record));
	rec_size = getRecordSize(schema);

	(*record)->data = (char*) malloc(sizeof(char)*rec_size);
	char *x = ";";
	strcpy((*record)->data, x);

	printf("----END----[CREATE RECORD]------------\n");
	return RC_OK;
}
RC freeRecord(Record *record) {
	printf("----START----[FREE RECORD]------------\n");

	printf("----END----[FREE RECORD]------------\n");
	return RC_OK;
}
RC getAttr(Record *record, Schema *schema, int attrNum, Value **value) {
	printf("----START----[GET ATTR]------------\n");

	printf("----END----[GET ATTR]------------\n");
	return RC_OK;
}
RC setAttr(Record *record, Schema *schema, int attrNum, Value *value) {
	printf("----START----[SET ATTR]------------\n");

	int temp_size;
	char* temp_value = malloc(16);
	switch (value->dt) {
	case DT_INT:
		sprintf(temp_value, "%i", value->v.intV);
		temp_size = sizeof(value->v.intV);
		printf("DT_INT == Temp Size  [%d] for Temp Value [%s]\n", temp_size,
				temp_value);
		break;
	case DT_STRING:
		strcpy(temp_value, value->v.stringV);
		temp_size = sizeof(value->v.stringV);
		printf("DT_STRING == Temp Size  [%d] for Temp Value [%s]\n", temp_size,
				temp_value);
		break;
	case DT_FLOAT:
		sprintf(temp_value, "%f", value->v.floatV);
		temp_size = sizeof(value->v.floatV);
		printf("DT_FLOAT == Temp Size  [%d] for Temp Value [%s]\n", temp_size,
				temp_value);
		break;
	case DT_BOOL:
		sprintf(temp_value, "%i", value->v.boolV);
		temp_size = sizeof(value->v.boolV);
		printf("DT_BOOL == Temp Size  [%d] for Temp Value [%s]\n", temp_size,
				temp_value);
		break;
	}
	if (attrNum != schema->numAttr - 1) {
		temp_value = strcat(temp_value, FIELD_DELIMITER);
	} else {
		temp_value = strcat(temp_value, TUPLE_DELIMITER);
	}

	strcat(record->data, temp_value);
	//memcpy(record->data,temp_value,temp_size);
	printf("Data in Record [%s] \n", record->data);
	printf("----END----[SET ATTR]------------\n");
	return RC_OK;
}
