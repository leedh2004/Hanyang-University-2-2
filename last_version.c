/*
 *  bpt.c  
 */
#define Version "1.14"
/*
 *
 *  bpt:  B+ Tree Implementation
 *  Copyright (C) 2010-2016  Amittai Aviram  http://www.amittai.com
 *  All rights reserved.
 *  Redistribution and use in source and binary forms, with or without
 *  modification, are permitted provided that the following conditions are met:
 *
 *  1. Redistributions of source code must retain the above copyright notice, 
 *  this list of conditions and the following disclaimer.
 *
 *  2. Redistributions in binary form must reproduce the above copyright notice, 
 *  this list of conditions and the following disclaimer in the documentation 
 *  and/or other materials provided with the distribution.
 
 *  3. Neither the name of the copyright holder nor the names of its 
 *  contributors may be used to endorse or promote products derived from this 
 *  software without specific prior written permission.
 
 *  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 *  AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE 
 *  IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE 
 *  ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE 
 *  LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR 
 *  CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF 
 *  SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS 
 *  INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN 
 *  CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) 
 *  ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE 
 *  POSSIBILITY OF SUCH DAMAGE.
 
 *  Author:  Amittai Aviram 
 *    http://www.amittai.com
 *    amittai.aviram@gmail.edu or afa13@columbia.edu
 *  Original Date:  26 June 2010
 *  Last modified: 17 June 2016
 *
 *  This implementation demonstrates the B+ tree data structure
 *  for educational purposes, includin insertion, deletion, search, and display
 *  of the search path, the leaves, or the whole tree.
 *  
 *  Must be compiled with a C99-compliant C compiler such as the latest GCC.
 *
 *  Usage:  bpt [order]
 *  where order is an optional argument
 *  (integer MIN_ORDER <= order <= MAX_ORDER)
 *  defined as the maximal number of pointers in any node.
 *
 */

#include "bpt.h"

// GLOBALS.

/* The order determines the maximum and minimum
 * number of entries (keys and pointers) in any
 * node.  Every node has at most order - 1 keys and
 * at least (roughly speaking) half that number.
 * Every leaf has as many pointers to data as keys,
 * and every internal node has one more pointer
 * to a subtree than the number of keys.
 * This global variable is initialized to the
 * default value.
 */
int order = DEFAULT_ORDER;

/* The queue is used to print the tree in
 * level order, starting from the root
 * printing each entire rank on a separate
 * line, finishing with the leaves.
 */
node * queue = NULL;

/* The user can toggle on and off the "verbose"
 * property, which causes the pointer addresses
 * to be printed out in hexadecimal notation
 * next to their corresponding keys.
 */
bool verbose_output = false;


// FUNCTION DEFINITIONS.

// OUTPUT AND UTILITIES

/* Copyright and license notice to user at startup. 
 */
void license_notice( void ) {
    printf("bpt version %s -- Copyright (C) 2010  Amittai Aviram "
            "http://www.amittai.com\n", Version);
    printf("This program comes with ABSOLUTELY NO WARRANTY; for details "
            "type `show w'.\n"
            "This is free software, and you are welcome to redistribute it\n"
            "under certain conditions; type `show c' for details.\n\n");
}


/* Routine to print portion of GPL license to stdout.
 */
void print_license( int license_part ) {
    int start, end, line;
    FILE * fp;
    char buffer[0x100];

    switch(license_part) {
    case LICENSE_WARRANTEE:
        start = LICENSE_WARRANTEE_START;
        end = LICENSE_WARRANTEE_END;
        break;
    case LICENSE_CONDITIONS:
        start = LICENSE_CONDITIONS_START;
        end = LICENSE_CONDITIONS_END;
        break;
    default:
        return;
    }

    fp = fopen(LICENSE_FILE, "r");
    if (fp == NULL) {
        perror("print_license: fopen");
        exit(EXIT_FAILURE);
    }
    for (line = 0; line < start; line++)
        fgets(buffer, sizeof(buffer), fp);
    for ( ; line < end; line++) {
        fgets(buffer, sizeof(buffer), fp);
        printf("%s", buffer);
    }
    fclose(fp);
}


/* First message to the user.
 */
void usage_1( void ) {
    printf("B+ Tree of Order %d.\n", order);
    printf("Following Silberschatz, Korth, Sidarshan, Database Concepts, "
           "5th ed.\n\n"
           "To build a B+ tree of a different order, start again and enter "
           "the order\n"
           "as an integer argument:  bpt <order>  ");
    printf("(%d <= order <= %d).\n", MIN_ORDER, MAX_ORDER);
    printf("To start with input from a file of newline-delimited integers, \n"
           "start again and enter the order followed by the filename:\n"
           "bpt <order> <inputfile> .\n");
}


/* Second message to the user.
 */
void usage_2( void ) {
    printf("Enter any of the following commands after the prompt > :\n"
    "\ti <k>  -- Insert <k> (an integer) as both key and value).\n"
    "\tf <k>  -- Find the value under key <k>.\n"
    "\tp <k> -- Print the path from the root to key k and its associated "
           "value.\n"
    "\tr <k1> <k2> -- Print the keys and values found in the range "
            "[<k1>, <k2>\n"
    "\td <k>  -- Delete key <k> and its associated value.\n"
    "\tx -- Destroy the whole tree.  Start again with an empty tree of the "
           "same order.\n"
    "\tt -- Print the B+ tree.\n"
    "\tl -- Print the keys of the leaves (bottom row of the tree).\n"
    "\tv -- Toggle output of pointer addresses (\"verbose\") in tree and "
           "leaves.\n"
    "\tq -- Quit. (Or use Ctl-D.)\n"
    "\t? -- Print this help message.\n");
}


/* Brief usage note.
 */
void usage_3( void ) {
    printf("Usage: ./bpt [<order>]\n");
    printf("\twhere %d <= order <= %d .\n", MIN_ORDER, MAX_ORDER);
}


/* Helper function for printing the
 * tree out.  See print_tree.
 */
void enqueue( node * new_node ) {
    node * c;
    if (queue == NULL) {
        queue = new_node;
        queue->next = NULL;
    }
    else {
        c = queue;
        while(c->next != NULL) {
            c = c->next;
        }
        c->next = new_node;
        new_node->next = NULL;
    }
}


/* Helper function for printing the
 * tree out.  See print_tree.
 */
node * dequeue( void ) {
    node * n = queue;
    queue = queue->next;
    n->next = NULL;
    return n;
}


/* Prints the bottom row of keys
 * of the tree (with their respective
 * pointers, if the verbose_output flag is set.
 */
void print_leaves( node * root ) {
    int i;
    node * c = root;
    if (root == NULL) {
        printf("Empty tree.\n");
        return;
    }
    while (!c->is_leaf)
        c = c->pointers[0];
    while (true) {
        for (i = 0; i < c->num_keys; i++) {
            if (verbose_output)
                printf("%lx ", (unsigned long)c->pointers[i]);
            printf("%d ", c->keys[i]);
        }
        if (verbose_output)
            printf("%lx ", (unsigned long)c->pointers[order - 1]);
        if (c->pointers[order - 1] != NULL) {
            printf(" | ");
            c = c->pointers[order - 1];
        }
        else
            break;
    }
    printf("\n");
}


/* Utility function to give the height
 * of the tree, which length in number of edges
 * of the path from the root to any leaf.
 */
int height( node * root ) {
    int h = 0;
    node * c = root;
    while (!c->is_leaf) {
        c = c->pointers[0];
        h++;
    }
    return h;
}


/* Utility function to give the length in edges
 * of the path from any node to the root.
 */
int path_to_root( node * root, node * child ) {
    int length = 0;
    node * c = child;
    while (c != root) {
        c = c->parent;
        length++;
    }
    return length;
}


/* Prints the B+ tree in the command
 * line in level (rank) order, with the 
 * keys in each node and the '|' symbol
 * to separate nodes.
 * With the verbose_output flag set.
 * the values of the pointers corresponding
 * to the keys also appear next to their respective
 * keys, in hexadecimal notation.
 */


void print_tree( node * root ) {

    node * n = NULL;
    int i = 0;
    int rank = 0;
    int new_rank = 0;

    if (root == NULL) {
        printf("Empty tree.\n");
        return;
    }
    queue = NULL;
    enqueue(root);
    while( queue != NULL ) {
        n = dequeue();
        if (n->parent != NULL && n == n->parent->pointers[0]) {
            new_rank = path_to_root( root, n );
            if (new_rank != rank) {
                rank = new_rank;
                printf("\n");
            }
        }
        if (verbose_output) 
            printf("(%lx)", (unsigned long)n);
        for (i = 0; i < n->num_keys; i++) {
            if (verbose_output)
                printf("%lx ", (unsigned long)n->pointers[i]);
            printf("%d ", n->keys[i]);
        }
        if (!n->is_leaf)
            for (i = 0; i <= n->num_keys; i++)
                enqueue(n->pointers[i]);
        if (verbose_output) {
            if (n->is_leaf) 
                printf("%lx ", (unsigned long)n->pointers[order - 1]);
            else
                printf("%lx ", (unsigned long)n->pointers[n->num_keys]);
        }
        printf("| ");
    }
    printf("\n");
}




/* Finds the record under a given key and prints an
 * appropriate message to stdout.
 */
/*
void find_and_print(node * root, int key, bool verbose) {
    record * r = find(root, key, verbose);
    if (r == NULL)
        printf("Record not found under key %d.\n", key);
    else 
        printf("Record at %lx -- key %d, value %d.\n",
                (unsigned long)r, key, r->value);
}
*/

/* Finds and prints the keys, pointers, and values within a range
 * of keys between key_start and key_end, including both bounds.
 */
/*
void find_and_print_range( node * root, int key_start, int key_end,
        bool verbose ) {
    int i;
    int array_size = key_end - key_start + 1;
    int returned_keys[array_size];
    void * returned_pointers[array_size];
    int num_found = find_range( root, key_start, key_end, verbose,
            returned_keys, returned_pointers );
    if (!num_found)
        printf("None found.\n");
    else {
        for (i = 0; i < num_found; i++)
            printf("Key: %d   Location: %lx  Value: %d\n",
                    returned_keys[i],
                    (unsigned long)returned_pointers[i],
                    ((record *)
                     returned_pointers[i])->value);
    }
}
*/

/* Finds keys and their pointers, if present, in the range specified
 * by key_start and key_end, inclusive.  Places these in the arrays
 * returned_keys and returned_pointers, and returns the number of
 * entries found.
 */
/*
int find_range( node * root, int key_start, int key_end, bool verbose,
        int returned_keys[], void * returned_pointers[]) {
    int i, num_found;
    num_found = 0;
    node * n = find_leaf( root, key_start, verbose );
    if (n == NULL) return 0;
    for (i = 0; i < n->num_keys && n->keys[i] < key_start; i++) ;
    if (i == n->num_keys) return 0;
    while (n != NULL) {
        for ( ; i < n->num_keys && n->keys[i] <= key_end; i++) {
            returned_keys[num_found] = n->keys[i];
            returned_pointers[num_found] = n->pointers[i];
            num_found++;
        }
        n = n->pointers[order - 1];
        i = 0;
    }
    return num_found;
}
*/

/* Traces the path from the root to a leaf, searching
 * by key.  Displays information about the path
 * if the verbose flag is set.
 * Returns the leaf containing the given key.
 */
/*
node * find_leaf( node * root, int key, bool verbose ) {
    int i = 0;
    node * c = root;
    if (c == NULL) {
        if (verbose) 
            printf("Empty tree.\n");
        return c;
    }
    while (!c->is_leaf) {
        if (verbose) {
            printf("[");
            for (i = 0; i < c->num_keys - 1; i++)
                printf("%d ", c->keys[i]);
            printf("%d] ", c->keys[i]);
        }
        i = 0;
        while (i < c->num_keys) {
            if (key >= c->keys[i]) i++;
            else break;
        }
        if (verbose)
            printf("%d ->\n", i);
        c = (node *)c->pointers[i];
    }
    if (verbose) {
        printf("Leaf [");
        for (i = 0; i < c->num_keys - 1; i++)
            printf("%d ", c->keys[i]);
        printf("%d] ->\n", c->keys[i]);
    }
    return c;
}
*/

/* Finds and returns the record to which
 * a key refers.
 */

/*
record * find( node * root, int key, bool verbose ) {
    int i = 0;
    node * c = find_leaf( root, key, verbose );
    if (c == NULL) return NULL;
    for (i = 0; i < c->num_keys; i++)
        if (c->keys[i] == key) break;
    if (i == c->num_keys) 
        return NULL;
    else
        return (record *)c->pointers[i];
}
*/
/* Finds the appropriate place to
 * split a node that is too big into two.
 */
int cut( int length ) {
    if (length % 2 == 0)
        return length/2;
    else
        return length/2+1;
}


// INSERTION

/* Creates a new record to hold the value
 * to which a key refers.
 */
record * make_record(int value) {
    record * new_record = (record *)malloc(sizeof(record));
    if (new_record == NULL) {
        perror("Record creation.");
        exit(EXIT_FAILURE);
    }
    else {
        new_record->value = value;
    }
    return new_record;
}


/* Creates a new general node, which can be adapted
 * to serve as either a leaf or an internal node.
 */
/*
node * make_node( void ) {
    node * new_node;
    new_node = malloc(sizeof(node));
    if (new_node == NULL) {
        perror("Node creation.");
        exit(EXIT_FAILURE);
    }
    new_node->keys = malloc( (order - 1) * sizeof(int) );
    if (new_node->keys == NULL) {
        perror("New node keys array.");
        exit(EXIT_FAILURE);
    }
    new_node->pointers = malloc( order * sizeof(void *) );
    if (new_node->pointers == NULL) {
        perror("New node pointers array.");
        exit(EXIT_FAILURE);
    }
    new_node->is_leaf = false;
    new_node->num_keys = 0;
    new_node->parent = NULL;
    new_node->next = NULL;
    return new_node;
}
*/
/* Creates a new leaf by creating a node
 * and then adapting it appropriately.
 */

/*
node * make_leaf( void ) {
    node * leaf = make_node();
    leaf->is_leaf = true;
    return leaf;
}
*/

/* Helper function used in insert_into_parent
 * to find the index of the parent's pointer to 
 * the node to the left of the key to be inserted.
 */
int get_left_index(node * parent, node * left) {

    int left_index = 0;
    while (left_index <= parent->num_keys && 
            parent->pointers[left_index] != left)
        left_index++;
    return left_index;
}

/* Inserts a new pointer to a record and its corresponding
 * key into a leaf.
 * Returns the altered leaf.
 */

/*
node * insert_into_leaf( node * leaf, int key, record * pointer ) {

    int i, insertion_point;

    insertion_point = 0;
    while (insertion_point < leaf->num_keys && leaf->keys[insertion_point] < key)
        insertion_point++;

    for (i = leaf->num_keys; i > insertion_point; i--) {
        leaf->keys[i] = leaf->keys[i - 1];
        leaf->pointers[i] = leaf->pointers[i - 1];
    }
    leaf->keys[insertion_point] = key;
    leaf->pointers[insertion_point] = pointer;
    leaf->num_keys++;
    return leaf;
}
*/

/* Inserts a new key and pointer
 * to a new record into a leaf so as to exceed
 * the tree's order, causing the leaf to be split
 * in half.
 */
/*
node * insert_into_leaf_after_splitting(node * root, node * leaf, int key, record * pointer) {

    node * new_leaf;
    int * temp_keys;
    void ** temp_pointers;
    int insertion_index, split, new_key, i, j;

    new_leaf = make_leaf();

    temp_keys = malloc( order * sizeof(int) );
    if (temp_keys == NULL) {
        perror("Temporary keys array.");
        exit(EXIT_FAILURE);
    }

    temp_pointers = malloc( order * sizeof(void *) );
    if (temp_pointers == NULL) {
        perror("Temporary pointers array.");
        exit(EXIT_FAILURE);
    }

    insertion_index = 0;
    while (insertion_index < order - 1 && leaf->keys[insertion_index] < key)
        insertion_index++;

    for (i = 0, j = 0; i < leaf->num_keys; i++, j++) {
        if (j == insertion_index) j++;
        temp_keys[j] = leaf->keys[i];
        temp_pointers[j] = leaf->pointers[i];
    }

    temp_keys[insertion_index] = key;
    temp_pointers[insertion_index] = pointer;

    leaf->num_keys = 0;

    split = cut(order - 1);

    for (i = 0; i < split; i++) {
        leaf->pointers[i] = temp_pointers[i];
        leaf->keys[i] = temp_keys[i];
        leaf->num_keys++;
    }

    for (i = split, j = 0; i < order; i++, j++) {
        new_leaf->pointers[j] = temp_pointers[i];
        new_leaf->keys[j] = temp_keys[i];
        new_leaf->num_keys++;
    }

    free(temp_pointers);
    free(temp_keys);

    new_leaf->pointers[order - 1] = leaf->pointers[order - 1];
    leaf->pointers[order - 1] = new_leaf;

    for (i = leaf->num_keys; i < order - 1; i++)
        leaf->pointers[i] = NULL;
    for (i = new_leaf->num_keys; i < order - 1; i++)
        new_leaf->pointers[i] = NULL;

    new_leaf->parent = leaf->parent;
    new_key = new_leaf->keys[0];

    return insert_into_parent(root, leaf, new_key, new_leaf);
}
*/

/* Inserts a new key and pointer to a node
 * into a node into which these can fit
 * without violating the B+ tree properties.
 */
/*
node * insert_into_node(node * root, node * n, 
        int left_index, int key, node * right) {
    int i;

    for (i = n->num_keys; i > left_index; i--) {
        n->pointers[i + 1] = n->pointers[i];
        n->keys[i] = n->keys[i - 1];
    }
    n->pointers[left_index + 1] = right;
    n->keys[left_index] = key;
    n->num_keys++;
    return root;
}
*/

/* Inserts a new key and pointer to a node
 * into a node, causing the node's size to exceed
 * the order, and causing the node to split into two.
 */
/*
node * insert_into_node_after_splitting(node * root, node * old_node, int left_index, 
        int key, node * right) {

    int i, j, split, k_prime;
    node * new_node, * child;
    int * temp_keys;
    node ** temp_pointers;
*/
    /* First create a temporary set of keys and pointers
     * to hold everything in order, including
     * the new key and pointer, inserted in their
     * correct places. 
     * Then create a new node and copy half of the 
     * keys and pointers to the old node and
     * the other half to the new.
     */
/*
    temp_pointers = malloc( (order + 1) * sizeof(node *) );
    if (temp_pointers == NULL) {
        perror("Temporary pointers array for splitting nodes.");
        exit(EXIT_FAILURE);
    }
    temp_keys = malloc( order * sizeof(int) );
    if (temp_keys == NULL) {
        perror("Temporary keys array for splitting nodes.");
        exit(EXIT_FAILURE);
    }

    for (i = 0, j = 0; i < old_node->num_keys + 1; i++, j++) {
        if (j == left_index + 1) j++;
        temp_pointers[j] = old_node->pointers[i];
    }

    for (i = 0, j = 0; i < old_node->num_keys; i++, j++) {
        if (j == left_index) j++;
        temp_keys[j] = old_node->keys[i];
    }

    temp_pointers[left_index + 1] = right;
    temp_keys[left_index] = key;
*/
    /* Create the new node and copy
     * half the keys and pointers to the
     * old and half to the new.
     */  
/*
    split = cut(order);
    new_node = make_node();
    old_node->num_keys = 0;
    for (i = 0; i < split - 1; i++) {
        old_node->pointers[i] = temp_pointers[i];
        old_node->keys[i] = temp_keys[i];
        old_node->num_keys++;
    }
    old_node->pointers[i] = temp_pointers[i];
    k_prime = temp_keys[split - 1];
    for (++i, j = 0; i < order; i++, j++) {
        new_node->pointers[j] = temp_pointers[i];
        new_node->keys[j] = temp_keys[i];
        new_node->num_keys++;
    }
    new_node->pointers[j] = temp_pointers[i];
    free(temp_pointers);
    free(temp_keys);
    new_node->parent = old_node->parent;
    for (i = 0; i <= new_node->num_keys; i++) {
        child = new_node->pointers[i];
        child->parent = new_node;
    }
*/
    /* Insert a new key into the parent of the two
     * nodes resulting from the split, with
     * the old node to the left and the new to the right.
     */
/*
    return insert_into_parent(root, old_node, k_prime, new_node);
}
*/


/* Inserts a new node (leaf or internal node) into the B+ tree.
 * Returns the root of the tree after insertion.
 */
/*
node * insert_into_parent(node * root, node * left, int key, node * right) {

    int left_index;
    node * parent;

    parent = left->parent;
*/
    /* Case: new root. */
/*
    if (parent == NULL)
        return insert_into_new_root(left, key, right);
*/
    /* Case: leaf or node. (Remainder of
     * function body.)  
     */

    /* Find the parent's pointer to the left 
     * node.
     */
/*
    left_index = get_left_index(parent, left);
*/

    /* Simple case: the new key fits into the node. 
     */
/*
    if (parent->num_keys < order - 1)
        return insert_into_node(root, parent, left_index, key, right);
*/
    /* Harder case:  split a node in order 
     * to preserve the B+ tree properties.
     */
/*
    return insert_into_node_after_splitting(root, parent, left_index, key, right);
}
*/

/* Creates a new root for two subtrees
 * and inserts the appropriate key into
 * the new root.
 */
/*
node * insert_into_new_root(node * left, int key, node * right) {

    node * root = make_node();
    root->keys[0] = key;
    root->pointers[0] = left;
    root->pointers[1] = right;
    root->num_keys++;
    root->parent = NULL;
    left->parent = root;
    right->parent = root;
    return root;
}
*/


/* First insertion:
 * start a new tree.
 */
/*
node * start_new_tree(int key, record * pointer) {

    node * root = make_leaf();
    root->keys[0] = key;
    root->pointers[0] = pointer;
    root->pointers[order - 1] = NULL;
    root->parent = NULL;
    root->num_keys++;
    return root;
}

*/

/* Master insertion function.
 * Inserts a key and an associated value into
 * the B+ tree, causing the tree to be adjusted
 * however necessary to maintain the B+ tree
 * properties.
 */

/*
node * insert( node * root, int key, int value ) {

    record * pointer;
    node * leaf;

     The current implementation ignores
      duplicates.
     

    if (find(root, key, false) != NULL)
        return root;

     Create a new record for the
     * value.
     
    pointer = make_record(value);


     Case: the tree does not exist yet.
     * Start a new tree.
     

    if (root == NULL) 
        return start_new_tree(key, pointer);


     Case: the tree already exists.
     * (Rest of function body.)
     */
/*
    leaf = find_leaf(root, key, false);

     Case: leaf has room for key and pointer.
     

    if (leaf->num_keys < order - 1) {
        leaf = insert_into_leaf(leaf, key, pointer);
        return root;
    }

*/
    /* Case:  leaf must be split.
     */
/*
    return insert_into_leaf_after_splitting(root, leaf, key, pointer);
}

*/


// DELETION.

/* Utility function for deletion.  Retrieves
 * the index of a node's nearest neighbor (sibling)
 * to the left if one exists.  If not (the node
 * is the leftmost child), returns -1 to signify
 * this special case.
 */
/*
int get_neighbor_index( node * n ) {

    int i;

     Return the index of the key to the left
     * of the pointer in the parent pointing
     * to n.  
     * If n is the leftmost child, this means
     * return -1.
     
    for (i = 0; i <= n->parent->num_keys; i++)
        if (n->parent->pointers[i] == n)
            return i - 1;

    // Error state.
    printf("Search for nonexistent pointer to node in parent.\n");
    printf("Node:  %#lx\n", (unsigned long)n);
    exit(EXIT_FAILURE);
}
*/
/*
node * remove_entry_from_node(node * n, int key, node * pointer) {

    int i, num_pointers;

    // Remove the key and shift other keys accordingly.
    i = 0;
    while (n->keys[i] != key)
        i++;
    for (++i; i < n->num_keys; i++)
        n->keys[i - 1] = n->keys[i];

    // Remove the pointer and shift other pointers accordingly.
    // First determine number of pointers.
    num_pointers = n->is_leaf ? n->num_keys : n->num_keys + 1;
    i = 0;
    while (n->pointers[i] != pointer)
        i++;
    for (++i; i < num_pointers; i++)
        n->pointers[i - 1] = n->pointers[i];


    // One key fewer.
    n->num_keys--;

    // Set the other pointers to NULL for tidiness.
    // A leaf uses the last pointer to point to the next leaf.
    if (n->is_leaf)
        for (i = n->num_keys; i < order - 1; i++)
            n->pointers[i] = NULL;
    else
        for (i = n->num_keys + 1; i < order; i++)
            n->pointers[i] = NULL;

    return n;
}


node * adjust_root(node * root) {

    node * new_root;
*/
    /* Case: nonempty root.
     * Key and pointer have already been deleted,
     * so nothing to be done.
     */
/*
    if (root->num_keys > 0)
        return root;
*/
    /* Case: empty root. 
     */

    // If it has a child, promote 
    // the first (only) child
    // as the new root.
/*
    if (!root->is_leaf) {
        new_root = root->pointers[0];
        new_root->parent = NULL;
    }

    // If it is a leaf (has no children),
    // then the whole tree is empty.

    else
        new_root = NULL;

    free(root->keys);
    free(root->pointers);
    free(root);

    return new_root;
}
*/
/* Coalesces a node that has become
 * too small after deletion
 * with a neighboring node that
 * can accept the additional entries
 * without exceeding the maximum.
 */
/*
node * coalesce_nodes(node * root, node * n, node * neighbor, int neighbor_index, int k_prime) {

    int i, j, neighbor_insertion_index, n_end;
    node * tmp;
*/
    /* Swap neighbor with node if node is on the
     * extreme left and neighbor is to its right.
     */
/*
    if (neighbor_index == -1) {
        tmp = n;
        n = neighbor;
        neighbor = tmp;
    }
*/
    /* Starting point in the neighbor for copying
     * keys and pointers from n.
     * Recall that n and neighbor have swapped places
     * in the special case of n being a leftmost child.
     */
/*
    neighbor_insertion_index = neighbor->num_keys;
*/
    /* Case:  nonleaf node.
     * Append k_prime and the following pointer.
     * Append all pointers and keys from the neighbor.
     */
/*
    if (!n->is_leaf) {
*/
        /* Append k_prime.
         */
/*
        neighbor->keys[neighbor_insertion_index] = k_prime;
        neighbor->num_keys++;


        n_end = n->num_keys;

        for (i = neighbor_insertion_index + 1, j = 0; j < n_end; i++, j++) {
            neighbor->keys[i] = n->keys[j];
            neighbor->pointers[i] = n->pointers[j];
            neighbor->num_keys++;
            n->num_keys--;
        }
*/
        /* The number of pointers is always
         * one more than the number of keys.
         */
/*
        neighbor->pointers[i] = n->pointers[j];
*/
        /* All children must now point up to the same parent.
         */
/*
        for (i = 0; i < neighbor->num_keys + 1; i++) {
            tmp = (node *)neighbor->pointers[i];
            tmp->parent = neighbor;
        }
    }
*/
    /* In a leaf, append the keys and pointers of
     * n to the neighbor.
     * Set the neighbor's last pointer to point to
     * what had been n's right neighbor.
     */
/*
    else {
        for (i = neighbor_insertion_index, j = 0; j < n->num_keys; i++, j++) {
            neighbor->keys[i] = n->keys[j];
            neighbor->pointers[i] = n->pointers[j];
            neighbor->num_keys++;
        }
        neighbor->pointers[order - 1] = n->pointers[order - 1];
    }

    root = delete_entry(root, n->parent, k_prime, n);
    free(n->keys);
    free(n->pointers);
    free(n); 
    return root;
}

*/
/* Redistributes entries between two nodes when
 * one has become too small after deletion
 * but its neighbor is too big to append the
 * small node's entries without exceeding the
 * maximum
 */
/*
node * redistribute_nodes(node * root, node * n, node * neighbor, int neighbor_index, 
        int k_prime_index, int k_prime) {  

    int i;
    node * tmp;
*/
    /* Case: n has a neighbor to the left. 
     * Pull the neighbor's last key-pointer pair over
     * from the neighbor's right end to n's left end.
     */
/*
    if (neighbor_index != -1) {
        if (!n->is_leaf)
            n->pointers[n->num_keys + 1] = n->pointers[n->num_keys];
        for (i = n->num_keys; i > 0; i--) {
            n->keys[i] = n->keys[i - 1];
            n->pointers[i] = n->pointers[i - 1];
        }
        if (!n->is_leaf) {
            n->pointers[0] = neighbor->pointers[neighbor->num_keys];
            tmp = (node *)n->pointers[0];
            tmp->parent = n;
            neighbor->pointers[neighbor->num_keys] = NULL;
            n->keys[0] = k_prime;
            n->parent->keys[k_prime_index] = neighbor->keys[neighbor->num_keys - 1];
        }
        else {
            n->pointers[0] = neighbor->pointers[neighbor->num_keys - 1];
            neighbor->pointers[neighbor->num_keys - 1] = NULL;
            n->keys[0] = neighbor->keys[neighbor->num_keys - 1];
            n->parent->keys[k_prime_index] = n->keys[0];
        }
    }
*/
    /* Case: n is the leftmost child.
     * Take a key-pointer pair from the neighbor to the right.
     * Move the neighbor's leftmost key-pointer pair
     * to n's rightmost position.
     */
/*
    else {  
        if (n->is_leaf) {
            n->keys[n->num_keys] = neighbor->keys[0];
            n->pointers[n->num_keys] = neighbor->pointers[0];
            n->parent->keys[k_prime_index] = neighbor->keys[1];
        }
        else {
            n->keys[n->num_keys] = k_prime;
            n->pointers[n->num_keys + 1] = neighbor->pointers[0];
            tmp = (node *)n->pointers[n->num_keys + 1];
            tmp->parent = n;
            n->parent->keys[k_prime_index] = neighbor->keys[0];
        }
        for (i = 0; i < neighbor->num_keys - 1; i++) {
            neighbor->keys[i] = neighbor->keys[i + 1];
            neighbor->pointers[i] = neighbor->pointers[i + 1];
        }
        if (!n->is_leaf)
            neighbor->pointers[i] = neighbor->pointers[i + 1];
    }
*/
    /* n now has one more key and one more pointer;
     * the neighbor has one fewer of each.
     */
/*
    n->num_keys++;
    neighbor->num_keys--;

    return root;
}
*/

/* Deletes an entry from the B+ tree.
 * Removes the record and its key and pointer
 * from the leaf, and then makes all appropriate
 * changes to preserve the B+ tree properties.
 */
/*
node * delete_entry( node * root, node * n, int key, void * pointer ) {

    int min_keys;
    node * neighbor;
    int neighbor_index;
    int k_prime_index, k_prime;
    int capacity;

    // Remove key and pointer from node.

    n = remove_entry_from_node(n, key, pointer);
*/
    /* Case:  deletion from the root. 
     */
/*
    if (n == root) 
        return adjust_root(root);
*/

    /* Case:  deletion from a node below the root.
     * (Rest of function body.)
     */

    /* Determine minimum allowable size of node,
     * to be preserved after deletion.
     */

  //  min_keys = n->is_leaf ? cut(order - 1) : cut(order) - 1;

    /* Case:  node stays at or above minimum.
     * (The simple case.)
     */
/*
    if (n->num_keys >= min_keys)
        return root;
*/
    /* Case:  node falls below minimum.
     * Either coalescence or redistribution
     * is needed.
     */

    /* Find the appropriate neighbor node with which
     * to coalesce.
     * Also find the key (k_prime) in the parent
     * between the pointer to node n and the pointer
     * to the neighbor.
     */
/*
    neighbor_index = get_neighbor_index( n );
    k_prime_index = neighbor_index == -1 ? 0 : neighbor_index;
    k_prime = n->parent->keys[k_prime_index];
    neighbor = neighbor_index == -1 ? n->parent->pointers[1] : 
        n->parent->pointers[neighbor_index];
  capacity = n->is_leaf ? order : order - 1;
*/
    /* Coalescence. */
/*
    if (neighbor->num_keys + n->num_keys < capacity)
        return coalesce_nodes(root, n, neighbor, neighbor_index, k_prime);
*/
    /* Redistribution. */
/*
    else
        return redistribute_nodes(root, n, neighbor, neighbor_index, k_prime_index, k_prime);
}
*/


/* Master deletion function.
 */
/*
node * delete(node * root, int key) {

    node * key_leaf;
    record * key_record;

    key_record = find(root, key, false);
    key_leaf = find_leaf(root, key, false);
    if (key_record != NULL && key_leaf != NULL) {
        root = delete_entry(root, key_leaf, key, key_record);
        free(key_record);
    }
    return root;
}


void destroy_tree_nodes(node * root) {
    int i;
    if (root->is_leaf)
        for (i = 0; i < root->num_keys; i++)
            free(root->pointers[i]);
    else
        for (i = 0; i < root->num_keys + 1; i++)
            destroy_tree_nodes(root->pointers[i]);
    free(root->pointers);
    free(root->keys);
    free(root);
}


node * destroy_tree(node * root) {
    destroy_tree_nodes(root);
	return NULL;
}
*/
/*
char * find(int64_t key){
		
	int i = 0, num_keys;
	int64_t page_offset, keys;
	char arr[120], *val;
	
	page_offset = find_leaf(key);
	if(page_offset == -1) return NULL; 

	lseek(fd,page_offset+12,SEEK_SET);
	read(fd,&num_keys,4);
	
	if(num_keys == 0) return NULL;

	lseek(fd,page_offset+128,SEEK_SET);
	
	for(i=0; i < num_keys; i++){
		read(fd,&keys,8);
		if(key == keys) break;
		lseek(fd,120,SEEK_CUR);
	}
	if ( i == num_keys) return NULL;
	else{
		//read(fd,arr,120);
		//val = arr;
		return "abc";
	}
}
*/
/*
char * find(int64_t key, char * value){
		
	int i = 0, num_keys;
	int64_t page_offset, keys;
	char take[120], *re;
	
	page_offset = find_leaf(key);
	if(page_offset == -1) return NULL; 

	lseek(fd,page_offset+12,SEEK_SET);
	read(fd,&num_keys,4);
	
	if(num_keys == 0) return NULL;

	lseek(fd,page_offset+128,SEEK_SET);
	printf("num_keys: %d",num_keys);
	for(i=0; i < num_keys; i++){
		read(fd,&keys,8);
		if( key == keys) break;
		lseek(fd,120,SEEK_CUR);
	}
	if ( i == num_keys) return NULL;
	else{
		read(fd,take,120);
		re = take;
		return re;
	}
}
*/


//LDH
//extern int freepage_num, leaf_oder, internal_order;

void makefreepage(){ //여기서 헤더페이지의 Number of page 관리해야될 것 같음, 나중에 구현
	int i;
	lseek(fd,0,SEEK_SET);
	int64_t F_O,val,N_F_O;
	read(fd,&F_O,8); // 마지막 남은 프리페이지의 오프셋을 읽는다

	for(i=0; i<11; i++){
		lseek(fd,F_O+(i*4096),SEEK_SET);
		val = F_O + ((i+1)*4096); // next free page offset
		write(fd,&val,8);
	}
	lseek(fd,F_O+40960,SEEK_SET);
	val = -1; // 마지막 프리페이지의 next free page offset은 -1, 즉 존재하지 않음
	write(fd,&val,8);
	freepage_num += 10; // 프리페이지 열개 추가
}

int64_t takefreepage(){ // 프리페이지의 오프셋 반환
			   		// 프리페이지가 1개밖에 없는 경우 프리페이지 10개 생성
	lseek(fd,0,SEEK_SET);
	int64_t F_O,NF_O; //Free Page Offset
	read(fd,&F_O,8);
	if(freepage_num == 1){
		makefreepage();
	}
	freepage_num--; //프리페이지 갯수 차감
	lseek(fd,0,SEEK_SET);
	NF_O = F_O + 4096;
	
	write(fd,&NF_O,8); // 헤더페이지의 프리페이지 오프셋 변경
	printf("take free page %ld\n", F_O);
	return F_O;
}

int open_db(char * pathname){
	int i;
	int64_t buf, val;
	if ( (fd = open(pathname, O_RDWR|O_SYNC , 0777)) > 0){
		lseek(fd,0,SEEK_SET);
		return 0;// 존재하는 파일
	}
	else if( (fd = open(pathname, O_RDWR | O_CREAT | O_SYNC, 0777)) > 0){
		val = 4096; //Free Page Offset 초기화
		write(fd,&val,8); 
		val = -1; // Root Page Offset -1, 존재하지 않음
		write(fd,&val,8); // Root page offset
		val = 10; // Number of pages, 처음에 프리페이지 10개 만듬
		write(fd,&val,8);
		
		for(i=0; i<10; i++){
			lseek(fd,(i+1)*4096,SEEK_SET);
			val = (i+2)*4096;
			write(fd,&val,8);
		} //프리페이지 열개 생성, 첫번째 프리페이지 주소 = 4096
		//열번째 프리페이지 주소 = 40960
		val = -1;
		lseek(fd,40960,SEEK_SET); //마지막 프리페이지로 이동
		write(fd,&val,8); //마지막 프리페이지의 next 프리페이지 = -1, 즉 존재하지 않는다.
		freepage_num = 10;
		return 0;// 새로운 파일 생성	
	}// succuess
	else
		return -1; // fail
}

char * find(int64_t key){
		
	int i = 0, num_keys;
	int64_t page_offset, keys;
	char *re;
	
	re = (char*)malloc(sizeof(char)*120);

	page_offset = find_leaf(key);
	if(page_offset == -1) return NULL; 

	lseek(fd,page_offset+12,SEEK_SET);
	read(fd,&num_keys,4);
	
	if(num_keys == 0) return NULL;

	lseek(fd,page_offset+128,SEEK_SET);
	for(i=0; i < num_keys; i++){
		read(fd,&keys,8);
		if(key == keys) break;
		lseek(fd,120,SEEK_CUR);
	}
	if ( i == num_keys) return NULL;
	else{
		read(fd,re,120);
		return re;
	}
}

int64_t find_leaf(int64_t key){
	int i, num_keys; 
	int64_t R_O, isLeaf, keys, page_offset, buf;
	lseek(fd,8,SEEK_SET);
	read(fd,&R_O,8); //root page offset 읽기
	if (R_O == -1) return -1; // 실패, 아무 키도 존재하지 않음
	
	lseek(fd,R_O+8,SEEK_SET); // 루트페이지로 이동 후 8바이트이동, 즉  Is_Leaf로 이동
	read(fd,&isLeaf,4); 
	page_offset = R_O; // root page offset

	while(!isLeaf){
		i = 0;
		lseek(fd,page_offset+12,SEEK_SET);
		read(fd,&num_keys,4); //root page키의 개수
		lseek(fd,page_offset+128,SEEK_SET); // root page의 첫번째 키 값으로 이동
		while (i < num_keys){
			read(fd,&keys,8); // key 값 읽음
			lseek(fd,8,SEEK_CUR); //페이지 오프셋은 건너뛰어
			if (key >= keys) i++;
			else{
				lseek(fd,-16,SEEK_CUR); // 작은 경우 back
				break;
			}
		} //계산상 현재 오프셋보다 8 뒤로 이동해야함
		lseek(fd,-8,SEEK_CUR); 
		read(fd,&page_offset,8); // 이동해야 할 페이지 오프셋을 받는다.
		lseek(fd,page_offset+8,SEEK_SET);
		read(fd,&isLeaf,4); // isLeaf 확인
	}
	return page_offset; // Leaf의 page offset
}
//디버깅 완료

int64_t make_node(){

	int is_Leaf;
	int64_t parent_offset, offset;
	parent_offset = -1; // Parent = -1, 아직 설정하지 않았음
	offset = takefreepage();

	lseek(fd,offset,SEEK_SET);
	write(fd,&parent_offset,8);
	is_Leaf = 0;
	write(fd,&is_Leaf,4); // is_Leaf = 0
	write(fd,&is_Leaf,4); // num_keys = 0

	return offset;
}
//디버깅 완료

int64_t make_leaf(){

	int64_t L_O;
	int is_Leaf = 1;
	L_O = make_node();
	lseek(fd,L_O+8,SEEK_SET);
	write(fd,&is_Leaf,4); // is_Leaf = 1
	
	return L_O;
}

//디버깅 완료
int64_t start_new_tree(int64_t key, char * value){
	int64_t F_O, val, L_O, R_O, R_S_O;
	int isLeaf,num_keys;
	L_O = make_leaf();// 리프 만듬 
	lseek(fd,L_O+128,SEEK_SET); //Leaf node의 첫번째 키 값으로 이동
	write(fd,&key,8);
	write(fd,value,120);
	num_keys = 1;
	lseek(fd,L_O+12,SEEK_SET);
	write(fd,&num_keys,4);
	lseek(fd,8,SEEK_SET);
	write(fd,&L_O,8); // Root page offset 설정
	R_S_O = 0;
	lseek(fd,L_O+120,SEEK_SET);
	write(fd,&R_S_O,8);
	return 0;
}

//디버깅 완료
int insert_into_leaf(int64_t L_O, int64_t key, char* value){
	int insertion_point,num_keys, copy;
	int64_t leaf_key;
	char leaf_value[120];

	lseek(fd,L_O+12,SEEK_SET); // L_O number of keys 로이동
	read(fd,&num_keys,4);

	lseek(fd,L_O +128*num_keys,SEEK_SET); //L_O 마지막 키 값으로 이동
	read(fd,&leaf_key,8);
	read(fd,leaf_value,120);

	insertion_point = num_keys;

	while(insertion_point > 0 && leaf_key > key){
		lseek(fd,L_O+128*(insertion_point+1),SEEK_SET);
		write(fd,&leaf_key,8);
		write(fd,leaf_value,120);

		insertion_point--;

		lseek(fd,L_O + insertion_point * 128,SEEK_SET);
		read(fd,&leaf_key,8);
		read(fd,leaf_value,120);
	}
	
	lseek(fd,L_O + (insertion_point+1)*128,SEEK_SET);
	write(fd,&key,8);
	write(fd,value,120);

	lseek(fd,L_O+12,SEEK_SET);
	num_keys++;
	write(fd,&num_keys,4);

	return 0;
}

//디버깅완료
int insert_into_node(int64_t P_O, int64_t N_key, int64_t N_L_O){

	int num_keys, insertion_point;
	int64_t node_key, page_offset, A = N_key, B = N_L_O;

	lseek(fd,P_O+12,SEEK_SET);
	read(fd,&num_keys,4);
	
	insertion_point = num_keys;
	lseek(fd,P_O+128+(num_keys-1)*16,SEEK_SET);
	
	read(fd,&node_key,8);
	read(fd,&page_offset,8);
	
	while(insertion_point > 0 && node_key > N_key){
		lseek(fd,P_O+128+((insertion_point)*16),SEEK_SET);
		write(fd,&node_key,8);
		write(fd,&page_offset,8);

		insertion_point--;
		lseek(fd,P_O+128+((insertion_point-1)*16),SEEK_SET);
		read(fd,&node_key,8);
		read(fd,&page_offset,8);
	}

	lseek(fd,P_O+128+((insertion_point)*16),SEEK_SET);
	write(fd,&A,8);
	write(fd,&B,8);
	
	lseek(fd,P_O+12,SEEK_SET);
	num_keys++;
	write(fd,&num_keys,4);
	return 0;
}


int insert_into_node_after_splitting(int64_t P_O, int64_t N_key, int64_t N_L_O){ 
	
	int i,j,insertion_point,num_keys;
	int64_t N_P_O, node_key, node_offset,copy_key,copy_offset,N_offset, mid_key,mid_offset;
	
	N_P_O = make_node();
	/*lseek(fd,L_O,SEEK_SET);
	write(fd,&N_P_O,8);
	*/
	
	lseek(fd,P_O+12,SEEK_SET); // 추가할 P_O의 number of keys 로 이동
	read(fd,&num_keys,4);

	lseek(fd,P_O+128+((num_keys-1)*16),SEEK_SET); // 추가할 P_O의 마지막 키 값으로 이동
	read(fd,&node_key,8);
	read(fd,&node_offset,8);

	insertion_point = num_keys;

	while(insertion_point > 0 && node_key > N_key){
		lseek(fd,P_O+128+((insertion_point)*16),SEEK_SET);
		write(fd,&node_key,8);
		write(fd,&node_offset,8);
		
		insertion_point--;
		lseek(fd,P_O + 128 + ((insertion_point-1)*16),SEEK_SET);
		read(fd,&node_key,8);
		read(fd,&node_offset,8);
	}

	lseek(fd,P_O+128+(insertion_point*16),SEEK_SET);
	write(fd,&N_key,8);
	write(fd,&N_L_O,8);
	
	//아 123빼고 옮겨야 하구나! 해결했다 씨바 123오프셋값을 맨왼쪽에 써주고 124 올리면 된다
	//그리고 자식의 부모 오프셋값을 N_P_O해주면 되겠다.
	
	int split = cut(internal_order); //4 -> 2, 5-> 3, 6-> 3
//여기 밑에 좀 이상해
	lseek(fd,P_O+128+(16*(split-1)),SEEK_SET);
	read(fd,&mid_key,8);
	read(fd,&mid_offset,8);
	
	lseek(fd,mid_offset,SEEK_SET);
	write(fd,&N_P_O,8);

	lseek(fd,N_P_O+120,SEEK_SET);
	write(fd,&mid_offset,8);

	for(i=split, j=0; i < internal_order; i++,j++){
		lseek(fd,P_O+128+(i*16),SEEK_SET);
		read(fd,&copy_key,8);
		read(fd,&copy_offset,8);

		lseek(fd,copy_offset,SEEK_SET);
		write(fd,&N_P_O,8);
		
		lseek(fd,N_P_O+128+(j*16),SEEK_SET);
		write(fd,&copy_key,8);
		write(fd,&copy_offset,8);
	}
	
	/* 이 부분은 디버깅 해봐야 해, 뭐를 올리는지 아직 정확하게는 몰라! */

	lseek(fd,P_O+12,SEEK_SET);
	num_keys = split - 1;
	write(fd,&num_keys,4);

	lseek(fd,N_P_O+12,SEEK_SET);
	num_keys = internal_order - split;
	write(fd,&num_keys,4);
	printf("insert into parent after splitting\n");
	//이제 부모 설정은 insert into parent한테 맡긴다.
	return insert_into_parent(P_O,N_P_O,mid_key);
}

//여기맨 밑 전까지 디버깅
int insert_into_parent(int64_t L_O, int64_t N_L_O, int64_t N_key){
	//맨처음 리프노드의 parent를 내가 -1로 했다는 가정 하에 작성해봄
	//즉 루트가 리프인 경우를  생각해야 하니까
	int num_keys;
	int64_t P_O, R_O; //Parent offset, New key, Root offset

	lseek(fd,L_O,SEEK_SET); // 왼쪽 노드의 부모 오프셋
	read(fd,&P_O,8);

	if(P_O == -1){ //부모가 존재하지 않는다, 새로운 루트 생성해야 함
		R_O = make_node();
		//printf("12288다음은 여기겠고");

		num_keys = 1;
		lseek(fd,R_O+12,SEEK_SET);
		write(fd,&num_keys,4); // 키가 1개 생성되니까
		
		lseek(fd,R_O+120,SEEK_SET);
		write(fd,&L_O,8);
		write(fd,&N_key,8);
		write(fd,&N_L_O,8); // 키값+오프셋들 넣어줬어

		lseek(fd,8,SEEK_SET);
		write(fd,&R_O,8); //헤더페이지에서 이어줌
		//이제 자식들의 부모를 이어주자
		
		lseek(fd,L_O,SEEK_SET);
		write(fd,&R_O,8);

		lseek(fd,N_L_O,SEEK_SET);
		write(fd,&R_O,8);
		printf("make new root----\n");
		return 0;
	}else{ 
		//여기서 아직 자식들의 부모를 이어주지 않았음, 그냥 여기서 이어주면 될 것 같아.
		lseek(fd,P_O+12,SEEK_SET);
		read(fd,&num_keys,4);
		
		if (num_keys < internal_order - 1){//247 여기서 이어주고 부모 자식 이어주자 어차피 스플릿 안생겨
			lseek(fd,N_L_O,SEEK_SET);
			write(fd,&P_O,8);
			return insert_into_node(P_O, N_key, N_L_O); 
			// 넣어야 할 parent page offset과 넣어야 할 key 값을 전달한다.
		}
		return insert_into_node_after_splitting(P_O, N_key, N_L_O);
	}
}

//이거 사실 여기서 넣을 필요도 없는데, 잘못했네
//디버깅 완료
//넣을 리프페이지 , 키값, value값 받았음, 디버깅 해봤는데 오류 없는 것 같아.
int insert_into_leaf_after_splitting(int64_t L_O, int64_t key, char * value){
	int i,j,insertion_point,num_keys,leaf_key;
	int64_t N_L_O, R_S_O, copy_key, re_key,N_key; //new leaf offset, right sibling offset, return key
	char copy_val[120],leaf_value[120];


//마지막 수정.. 필요 없는 거였어 생각해보니
/*	
	lseek(fd,L_O+12,SEEK_SET); // L_O로 number of keys 로이동
	read(fd,&num_keys,4); //사실 31이니까 굳이 읽을필요는 없지..
*/

	num_keys = leaf_order - 1;

	lseek(fd,L_O + 128*num_keys,SEEK_SET); //L_O 마지막 키 값으로 이동
	read(fd,&leaf_key,8);
	read(fd,leaf_value,120);

	insertion_point = num_keys;

	while(insertion_point > 0 && leaf_key > key){
		lseek(fd,L_O+128*(insertion_point+1),SEEK_SET);
		write(fd,&leaf_key,8);
		write(fd,leaf_value,120);

		insertion_point--;
		lseek(fd,L_O + insertion_point * 128,SEEK_SET);
		read(fd,&leaf_key,8);
		read(fd,leaf_value,120);
	}
	
	lseek(fd,L_O + (insertion_point+1)*128,SEEK_SET);
	write(fd,&key,8);
	write(fd,value,120);

	/*이제 31개 되었으니.. key[15] 가 중간값이겠네.
	  그러면 15부터 30까지 새로운 노드고, 이전노드에 right sibiling offset으로 이어준다
	  루트에 15를 추가해야 겠지?*/
	//이전 노드의  right sibling을 새로운 애의 right sibling 으로 설정하자

	N_L_O = make_leaf();
	lseek(fd,L_O+120,SEEK_SET); 
	read(fd,&R_S_O,8); //원래 right sibling offset

	lseek(fd,L_O+120,SEEK_SET); 
	write(fd,&N_L_O,8);
	
	lseek(fd,N_L_O+120,SEEK_SET);
	write(fd,&R_S_O,8);
	
	//이어주는 것 까지 설정
	//원래 노드 keys[0]~keys[15] 
	//new_leaf_node에 keys[15]~keys[30] insertion
	
	//현재 꽉찬 노드
	
	for(i=leaf_order / 2 ,j=0; i < leaf_order; i++,j++){ //15 , 31에서 수정.
		lseek(fd,L_O+((i+1)*128),SEEK_SET);
		read(fd,&copy_key,8);
		read(fd,copy_val,120);
		lseek(fd,N_L_O+((j+1)*128),SEEK_SET);
		write(fd,&copy_key,8);
		write(fd,copy_val,120);
	}

	lseek(fd,N_L_O+128,SEEK_SET);
	read(fd,&N_key,8);

	lseek(fd,L_O+12,SEEK_SET);
	num_keys = leaf_order / 2; //15 
	write(fd,&num_keys,8);

	lseek(fd,N_L_O+12,SEEK_SET);
	num_keys = leaf_order - num_keys; //16
	write(fd,&num_keys,8);

	//number of keys 값들 수정
	printf("-------split leaf!\n");
	return insert_into_parent(L_O, N_L_O, N_key); //부모 공유는 여기서!
}


int insert(int64_t key, char * value){

	int64_t R_O,F_O,L_O,val; // root page offset, free page offset, leaf page offset
	int num_keys;
	char * f;

	if ( (f =find(key)) != NULL) return -1; // 존재하므로 실패
	free(f);

	lseek(fd,8,SEEK_SET);
	read(fd,&R_O,8);// root page offset
	
	if(R_O == -1)
		return start_new_tree(key,value);

	L_O = find_leaf(key); //넣어야 할 리프 페이지의 오프셋 받음
	//printf("L_O : %ld\n",L_O);

	lseek(fd,L_O+12,SEEK_SET); // 리프 페이지로 이동
	read(fd,&num_keys,4); // key 개수 받음 
	
	if (num_keys < leaf_order - 1) // if leaf order 31 -> num_keys 30이면 밑으로 가야함
		return insert_into_leaf(L_O,key,value);
	else
		return insert_into_leaf_after_splitting(L_O,key,value);
}

/* 
   delete
		  */

//리프 노드로 가서, 지워주고, shift, num_keys 감소

int64_t remove_entry_from_node(int64_t key, int64_t N_offset){
	printf("-----remove entry from node-----\n");
	int i, num_keys, is_Leaf;
	int64_t N_keys,offset;
	char leaf_values[120];

	lseek(fd,N_offset+8,SEEK_SET);
	read(fd,&is_Leaf,4);
	read(fd,&num_keys,4);

	i = 0;

	lseek(fd,N_offset+128,SEEK_SET);
	read(fd,&N_keys,8);
	
	if(is_Leaf){
		while(key != N_keys){
			i++;
			lseek(fd,120,SEEK_CUR);
			read(fd,&N_keys,8);
		}
		//i가 0인경우..
		for(++i; i < num_keys ; i++){
			lseek(fd,N_offset+((i+1)*128),SEEK_SET);
			read(fd,&N_keys,8);
			read(fd,leaf_values,120);
			lseek(fd,-256,SEEK_CUR);
			write(fd,&N_keys,8);
			write(fd,leaf_values,120);
		}
	}else{
		//internal
		while(key != N_keys){
			i++; // index
			lseek(fd,8,SEEK_CUR);
			read(fd,&N_keys,8);
		}
		for(++i; i < num_keys+1; i++){
			lseek(fd,N_offset+128+(16*i),SEEK_SET);
			read(fd,&N_keys,8);
			read(fd,&offset,8);
			
			lseek(fd,N_offset+128+(16*(i-1)),SEEK_SET);
			write(fd,&N_keys,8);
			write(fd,&offset,8);
		}
	}
	num_keys--;
	lseek(fd,N_offset+12,SEEK_SET);
	write(fd,&num_keys,4);
	return N_offset;
}

int adjust_root(int64_t leaf_offset){
	printf("-----adjust root-----\n");
	int num_keys,is_Leaf;
	int64_t R_O,new_R_O, tmp;

	lseek(fd,leaf_offset+8,SEEK_SET);
	read(fd,&is_Leaf,4);
	read(fd,&num_keys,4);
	
	if (num_keys > 0)
		return 0;
	/* Case: empty root */
	// 아직 is_Leaf가 0인 케이스는 이해를 못했음.
	if(!is_Leaf){
		lseek(fd,leaf_offset+120,SEEK_SET);
		read(fd,&new_R_O,8);
		lseek(fd,new_R_O,SEEK_SET);
		tmp = -1;
		write(fd,&tmp,8);
		lseek(fd,8,SEEK_SET);
		write(fd,&new_R_O,8);
	}
	else{
		lseek(fd,8,SEEK_SET);
		new_R_O = -1;
		write(fd,&new_R_O,8);
	}
	//return_freepage(R_O);
	return 0;
}

int get_neighbor_index(int64_t leaf_offset){
	int num_keys, i;
	int64_t parent_offset, keys, read_offset;

	lseek(fd,leaf_offset,SEEK_SET);
	read(fd,&parent_offset,8);

	lseek(fd,parent_offset+12,SEEK_SET);
	read(fd,&num_keys,8);

	for(i=0; i <= num_keys; i++){
		lseek(fd,parent_offset+120+(i*16),SEEK_SET);
		read(fd,&read_offset,8);
		if(read_offset == leaf_offset) break;
	}
	return i-1;
}

int64_t get_neighbor_offset(int key, int64_t leaf_offset){
	int num_keys,i;
	int64_t keys, neighbor_offset, parent_offset,read_offset;
	
	lseek(fd,leaf_offset,SEEK_SET);
	read(fd,&parent_offset,8);

	lseek(fd,parent_offset+12,SEEK_SET);
	read(fd,&num_keys,8);
	
	for(i=0; i <num_keys; i++){
		lseek(fd,parent_offset+120+(i*16),SEEK_SET);
		read(fd,&read_offset,8);
		if(read_offset == leaf_offset) break;
	}
	
	if(i==0){
		lseek(fd,parent_offset+136,SEEK_SET);
		read(fd,&read_offset,8);
		return read_offset;
	}else{
		lseek(fd,parent_offset+120+((i-1)*16),SEEK_SET);
		read(fd,&read_offset,8);
		return read_offset;
	}
}

//병합할 때, neighbor offset으로 병합
int coalesce_nodes(int64_t neighbor_offset, int64_t N_offset, int neighbor_index, int64_t k_prime)
{
	printf("-----coalesce_nodes-----\n");
	int i, j, neighbor_num_keys, neighbor_insertion_index, is_Leaf, num_keys, n_end;
	char value[120];
	int64_t tmp, keys, parent_offset, copy_offset,R_S_O;


	if(neighbor_index == -1){
		tmp = neighbor_offset;
		neighbor_offset = N_offset;
		N_offset = tmp;
	}
	
	lseek(fd,N_offset+12,SEEK_SET);
	read(fd,&num_keys,4);
	
	lseek(fd,neighbor_offset+12,SEEK_SET);
	read(fd,&neighbor_num_keys,4);

	neighbor_insertion_index = neighbor_num_keys;
	
	lseek(fd,N_offset+8,SEEK_SET);
	read(fd,&is_Leaf,4);
	
	if(!is_Leaf){
		// internal

		lseek(fd,neighbor_offset+128+(16*neighbor_insertion_index),SEEK_SET);
		write(fd,&k_prime,8);
		
		neighbor_num_keys++; //copy offset도 올려줘야해
		lseek(fd,neighbor_offset+12,SEEK_SET);
		write(fd,&neighbor_num_keys,4);

		n_end = num_keys;
		
		for(i = neighbor_insertion_index + 1, j = 0; j < n_end; i++, j++) {
			
			lseek(fd,N_offset+120+(16*j),SEEK_SET);
			read(fd,&copy_offset,8);
			read(fd,&keys,8);
			
			lseek(fd,neighbor_offset+120+(16*i),SEEK_SET);
			write(fd,&copy_offset,8);
			write(fd,&keys,8);

			num_keys --;
			neighbor_num_keys++;
		}
		
		lseek(fd,N_offset+120+(16*j),SEEK_SET);
		read(fd,&copy_offset,8);
		
		lseek(fd,neighbor_offset+120+(16*i),SEEK_SET);
		write(fd,&copy_offset,8);

		for (i=0; i<neighbor_num_keys+1;i++){
			lseek(fd,neighbor_offset+120+(16*i),SEEK_SET);
			read(fd,&copy_offset,8);
			lseek(fd,copy_offset,SEEK_SET);
			write(fd,&neighbor_offset,8);
		}
	}
	else{
		for (i = neighbor_insertion_index, j=0; j < num_keys; i++, j++){
			
			lseek(fd,N_offset+(128*(j+1)),SEEK_SET);
			read(fd,&keys,8);
			read(fd,value,120);
			
			lseek(fd,neighbor_offset+(128*(i+1)),SEEK_SET);
			write(fd,&keys,8);
			write(fd,value,120);
			
			neighbor_num_keys++;
		}
		lseek(fd,N_offset+120,SEEK_SET);
		read(fd,&R_S_O,8);
		lseek(fd,neighbor_offset+120,SEEK_SET);
		write(fd,&R_S_O,8);
	}

	lseek(fd,neighbor_offset+12,SEEK_SET);
	write(fd,&neighbor_num_keys,4);
	lseek(fd,N_offset+12,SEEK_SET);
	num_keys = 0;	
	write(fd,&num_keys,4);

	lseek(fd,neighbor_offset,SEEK_SET);
	read(fd,&parent_offset,8);
	
	return delete_entry(k_prime, parent_offset);
}

void return_freepage(int64_t N_offset){
	int64_t N_F_O;

	lseek(fd,0,SEEK_SET);
	read(fd,&N_F_O,8);

	lseek(fd,N_offset,SEEK_SET);
	write(fd,&N_F_O,8);

	lseek(fd,0,SEEK_SET);
	write(fd,&N_offset,8);

	freepage_num++;
	
	return;
}


int redistribute_node(int64_t N_offset, int64_t neighbor_offset, int neighbor_index,
		int k_prime_index, int64_t k_prime){
	
	printf("-----redistribute node-----\n");
	int i,is_Leaf,num_keys,neighbor_num_keys;
	char value[120];
	int64_t tmp, keys, parent_offset, offset;
	
	lseek(fd,N_offset,SEEK_SET);
	read(fd,&parent_offset,8);
	read(fd,&is_Leaf,4);
	read(fd,&num_keys,4);

	lseek(fd,N_offset+12,SEEK_SET);
	read(fd,&neighbor_num_keys,4);

	/* Case: n has a neighbor to the left */

	if(neighbor_index != -1) {
		if(!is_Leaf) {
			// case internal, 여기 조금 수정하자
			lseek(fd,N_offset+120+(16*num_keys),SEEK_SET);
			read(fd,&offset,8);

			lseek(fd,N_offset+120+(16*(num_keys+1)),SEEK_SET);
			write(fd,&offset,8);

			for (i = num_keys; i > 0; i--){
				lseek(fd,N_offset+120+(16*(i-1)),SEEK_SET);
				read(fd,&offset,8);
				read(fd,&keys,8);
				
				lseek(fd,N_offset+120+(16*i),SEEK_SET);
				write(fd,&offset,8);
				write(fd,&keys,8);
			}

			lseek(fd,neighbor_offset+120+(16*neighbor_num_keys),SEEK_SET);
			read(fd,&offset,8);
			
			lseek(fd,offset,SEEK_SET); //parent
			write(fd,&N_offset,8);

			lseek(fd,N_offset+120,SEEK_SET);
			write(fd,&offset,8);
			write(fd,&k_prime,8);
			
			lseek(fd,neighbor_offset+128+(16*(neighbor_num_keys-1)),SEEK_SET);
			read(fd,&keys,8);

			lseek(fd,parent_offset+128+(16*k_prime_index),SEEK_SET);
			write(fd,&keys,8);
		}
		else {
			// case leaf
			for (i = num_keys; i > 0; i--){
				lseek(fd,N_offset+128+(128*(i-1)),SEEK_SET);
				read(fd,&keys,8);
				read(fd,value,120);
				
				lseek(fd,N_offset+128+(128*i),SEEK_SET);
				write(fd,&keys,8);
				write(fd,value,120);
			}

			lseek(fd,neighbor_offset+(128*neighbor_num_keys),SEEK_SET);
			read(fd,&keys,8);
			read(fd,value,120);

			lseek(fd,N_offset+128,SEEK_SET);
			write(fd,&keys,8);
			write(fd,value,120);
			
			lseek(fd,parent_offset+128+(16*k_prime_index),SEEK_SET);
			write(fd,&keys,8);
		}
	}
	else { // n has a neighbor to the right
		if(is_Leaf) {
			lseek(fd,neighbor_offset+128,SEEK_SET);
			read(fd,&keys,8);
			read(fd,value,120);

			lseek(fd,N_offset+128+(128*num_keys),SEEK_SET);
			write(fd,&keys,8);
			write(fd,value,120);

			lseek(fd,neighbor_offset+256,SEEK_SET);
			read(fd,&keys,8);

			lseek(fd,parent_offset+128+(16*k_prime_index),SEEK_SET);
			write(fd,&keys,8);

			for(i = 0; i < neighbor_num_keys - 1; i++){
				lseek(fd,neighbor_offset+128+(128*(i+1)),SEEK_SET);
				read(fd,&keys,8);
				read(fd,value,120);
				
				lseek(fd,neighbor_offset+128+(128*i),SEEK_SET);
				write(fd,&keys,8);
				write(fd,value,120);
			}
		}
		else{
			//Case internl

			lseek(fd,N_offset+128+(16*num_keys),SEEK_SET);
			write(fd,&k_prime,8);
			lseek(fd,neighbor_offset+120,SEEK_SET);
			read(fd,&offset,8);
			lseek(fd,N_offset+120+(16*(num_keys+1)),SEEK_SET);
			write(fd,&offset,8);
			lseek(fd,offset,SEEK_SET);
			write(fd,&N_offset,8);
			
			lseek(fd,neighbor_offset+128,SEEK_SET);
			read(fd,&keys,8);

			lseek(fd,parent_offset+128+(16*k_prime_index),SEEK_SET);
			write(fd,&keys,8);

			for(i=0; i < neighbor_num_keys - 1 ; i++){
				lseek(fd,neighbor_offset+120+(16*(i+1)),SEEK_SET);
				read(fd,&offset,8);
				read(fd,&keys,8);
				
				lseek(fd,neighbor_offset+(120+(16*i)),SEEK_SET);
				write(fd,&offset,8);
				write(fd,&keys,8);
			}

			lseek(fd,neighbor_offset+120+(16*(i+1)),SEEK_SET);
			read(fd,&offset,8);

			lseek(fd,neighbor_offset+(120+(16*i)),SEEK_SET);
			write(fd,&offset,8);
		}
	}
	neighbor_num_keys --;
	num_keys ++;
	
	lseek(fd,neighbor_offset+12,SEEK_SET);
	write(fd,&neighbor_num_keys,4);
	lseek(fd,N_offset+12,SEEK_SET);
	write(fd,&num_keys,4);
	return 0;
}

// key를 가지고있는 오프셋이 N_offset인 페이지에서, key를 지운다.
int delete_entry(int64_t key, int64_t N_offset){

	printf("-----delete entry-----\n");
	int min_keys, is_Leaf, neighbor_num_keys, num_keys; 
	int capacity,  neighbor_index, k_prime_index;
	int64_t root_offset, neighbor_offset, parent_offset, N_O,k_prime;

	N_O = remove_entry_from_node(key,N_offset);
	
	lseek(fd,8,SEEK_SET);
	read(fd,&root_offset,8);
	
	if ( N_O == root_offset )
		return adjust_root(N_offset);

	lseek(fd,N_O+8,SEEK_SET);
	read(fd,&is_Leaf,4);
	read(fd,&num_keys,4);
	
	if(is_Leaf == 1)
		min_keys = cut(leaf_order - 1);
	else
		min_keys = cut(internal_order) - 1;
	
	//printf("num keys : %d\n", num_keys);
	//종료 조건 1
	if(num_keys >= min_keys)
		return 0;
	
	//leaf offset은, 지워야 할 키를 가지고 있는 page offset이다.
	
	neighbor_index = get_neighbor_index(N_offset);
	k_prime_index = neighbor_index == -1 ? 0 : neighbor_index;
	
	lseek(fd,N_offset,SEEK_SET);
	read(fd,&parent_offset,8);

	lseek(fd,parent_offset+128+(16*k_prime_index),SEEK_SET);
	read(fd,&k_prime,8);

	neighbor_offset = get_neighbor_offset(key, N_offset);

	lseek(fd,neighbor_offset+12,SEEK_SET);
	read(fd,&neighbor_num_keys,4);

	//printf("neighbor_num_keys : %d, num_keys %d\n",neighbor_num_keys,num_keys);
	if(is_Leaf){
		capacity = leaf_order;
	}else
		capacity = internal_order - 1;
	
	printf("k_prime : %d\n",k_prime);
	printf("neighbor_num_keys : %d num_keys : %d\n",neighbor_num_keys, num_keys);
	
	if ( neighbor_num_keys + num_keys < capacity )
		return coalesce_nodes(neighbor_offset, N_offset, neighbor_index, k_prime);
	else
		return redistribute_node(N_offset, neighbor_offset, neighbor_index, k_prime_index,k_prime);
}


int delete(int64_t key){
	
	int64_t leaf_offset;
	char * f;

	if ( (f=find(key)) == NULL)  return -1; // 존재하지 않으므로 실패
	free(f);

	leaf_offset = find_leaf(key);

	return delete_entry(key,leaf_offset);
}
