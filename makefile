CC=gcc
CFLAGS=-I.
OBJ=test_assign4_1.o storage_mgr.o dberror.o buffer_mgr.o buffer_mgr_stat.o expr.o record_mgr.o node_linked_list.o rm_serializer.o btree_mgr.o bPlusTree_data_structures.o
HEADERS=dberror.h storage_mgr.h test_helper.h buffer_mgr.h buffer_mgr_stat.h dt.h expr.h record_mgr.h node_linked_list.h rm_serializer.h btree_mgr.h bPlusTree_data_structures.h

%.o: %.c $(HEADERS)
	$(CC) -c -o -w $@ $< $(CFLAGS)

test_assign4: $(OBJ)
	$(CC) -o $@ $^ $(CFLAGS)

clean:
	rm -f test_assign4 $(OBJ)

