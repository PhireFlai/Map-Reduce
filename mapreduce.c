#include "mapreduce.h"
#include "threadpool.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
// function pointer typedefs
typedef void (*Mapper)(char *file_name);
typedef void (*Reducer)(char *key, unsigned int partition_idx);

//  linked list of keyValues
typedef struct KeyValue {
  char *key;
  char *value;
  struct KeyValue *next;
} KeyValue;

// for passing reducer arguments to thread
typedef struct {
  unsigned int partition_idx;
  Reducer reducer_func;
} ReducerArgs;

// global pointers for dynamic arrays
pthread_mutex_t *partition_locks = NULL;
KeyValue **partitions = NULL;

size_t *partition_sizes;
size_t numParts = 0;

// library functions that must be implemented

/**
 * Run the MapReduce framework
 * Parameters:
 *     file_count   - Number of files (i.e. input splits)
 *     file_names   - Array of filenames
 *     mapper       - Function pointer to the map function
 *     reducer      - Function pointer to the reduce function
 *     num_workers  - Number of threads in the thread pool
 *     num_parts    - Number of partitions to be created
 */
void MR_Run(unsigned int file_count, char *file_names[], Mapper mapper,
            Reducer reducer, unsigned int num_workers, unsigned int num_parts) {
  // Initialize thread pool
  ThreadPool_t *tp = ThreadPool_create(num_workers);

  // saving a global refernce to number of partitons
  numParts = num_parts;

  // initialing global arrays
  partition_locks = malloc(numParts * sizeof(pthread_mutex_t));
  partitions = malloc(numParts * sizeof(KeyValue *));
  partition_sizes = malloc(numParts * sizeof(size_t));

  // Initialize mutexes for each partition
  for (unsigned int i = 0; i < numParts; i++) {
    pthread_mutex_init(&partition_locks[i], NULL);
    partitions[i] = NULL;
    partition_sizes[i] = 0;
  }

  // printf("file count %d\n", file_count);

  // Add mapper tasks to the thread pool for each file
  for (unsigned int i = 0; i < file_count; i++) {
    struct stat file_stat;

    // get the file sizes
    if (stat(file_names[i], &file_stat) == 0) {
      size_t file_size = (size_t)file_stat.st_size;
      ThreadPool_add_job(tp, (thread_func_t)mapper, file_names[i], file_size);
    }
  }

  // wait for all map tasks to complete
  ThreadPool_check(tp);

  // reduce each partition
  for (unsigned int i = 0; i < numParts; i++) {
    // printf("running reduce\n");

    // passing arguments
    ReducerArgs *args = malloc(sizeof(ReducerArgs));
    args->partition_idx = i;
    args->reducer_func = reducer;
    ThreadPool_add_job(tp, MR_Reduce, args, partition_sizes[i]);
  }

  // Wait for all reduce tasks to complete
  ThreadPool_check(tp);

  // clean up all resources
  ThreadPool_destroy(tp);

  for (unsigned int i = 0; i < numParts; i++) {
    pthread_mutex_destroy(&partition_locks[i]);
    KeyValue *current = partitions[i];
    while (current != NULL) {
      KeyValue *temp = current;
      current = current->next;
      free(temp->key); // Free the key
      free(temp);      // Free the KeyValue structure
    }
  }

  free(partition_sizes);
  free(partition_locks);
  free(partitions);
}

/**
 * Write a specifc map output, a <key, value> pair, to a partition
 * Parameters:
 *     key           - Key of the output
 *     value         - Value of the output
 */
void MR_Emit(char *key, char *value) {
  // Determine the partition index based on the key
  // printf("running emit\n");

  int partition = MR_Partitioner(key, numParts);

  // Lock the partition to prevent concurrent access
  pthread_mutex_lock(&partition_locks[partition]);

  // find out size of key value pair
  size_t key_size = strlen(key) + 1; 
  size_t value_size = strlen(value) + 1;
  size_t kv_size = sizeof(KeyValue) + key_size + value_size;

  // create a copy of the key value pair
  KeyValue *new_pair = malloc(sizeof(KeyValue));
  new_pair->key = strdup(key);
  new_pair->value = strdup(value);
  new_pair->next = NULL;

  // insert key vlaue pair based on order
  KeyValue **current = &partitions[partition];
  while (*current != NULL && strcmp((*current)->key, key) < 0) {
    current = &(*current)->next;
  }

  new_pair->next = *current;
  *current = new_pair;

  // add size of key value to partition size
  partition_sizes[partition] += kv_size;

  pthread_mutex_unlock(&partition_locks[partition]);
}

/**
 * Hash a mapper's output to determine the partition that will hold it
 * Parameters:
 *     key           - Key of a specifc map output
 *     num_partitions- Total number of partitions
 * Return:
 *     unsigned int  - Index of the partition
 */
unsigned int MR_Partitioner(char *key, unsigned int num_partitions) {
  unsigned int hash = 5381;
  int c;
  while ((c = *key++) != '\0')
    hash = hash * 33 + c;
  return hash % num_partitions;
}
/**
 * Run the reducer callback function for each <key, (list of values)>
 * retrieved from a partition
 * Parameters:
 *     threadarg     - Pointer to a hidden args object
 */
void MR_Reduce(void *threadarg) {
  // Extract the partition index and reducer function from the argument
  ReducerArgs *args = (ReducerArgs *)threadarg;
  unsigned int partition_idx = args->partition_idx;
  Reducer reducer_func = args->reducer_func;

  // Ensure the partition index is valid
  if (partition_idx >= numParts) {
    free(args); // Free the ReducerArgs if the index is invalid
    return;
  }

  // make a copy of the pointer to start of each partition
  KeyValue *start = partitions[partition_idx];

  // tracks to see if key is different from the last
  KeyValue *current = partitions[partition_idx];
  char *last_key = NULL;

  // Process each unique key in the partition
  while (current != NULL) {

    if (last_key == NULL || strcmp(last_key, current->key) != 0) {
      // printf("Reducing key: %s in partition %d\n", current->key,
      // partition_idx);
      reducer_func(current->key, partition_idx);

      // Update last_key to point to current key
      last_key = current->key;
    }
    current = current->next;
  }

  // restore the pointer to the start so that values can be freed properly later
  partitions[partition_idx] = start;

  free(args);
}

/**
 * Run the reducer callback function for each <key, (list of values)>
 * retrieved from a partition
 * Parameters:
 *     threadarg     - Pointer to a hidden args object
 */

char *MR_GetNext(char *key, unsigned int partition_idx) {
  // Check if the partition index is valid
  if (partition_idx >= numParts) {
    return NULL;
  }

  pthread_mutex_lock(&partition_locks[partition_idx]);

  // Start at current position in current partition
  KeyValue *current = partitions[partition_idx];
  while (current != NULL) {
    if (strcmp(current->key, key) == 0) {
      char *value = current->value;
      KeyValue *next = current->next;


      // update index
      partitions[partition_idx] = next;

      pthread_mutex_unlock(&partition_locks[partition_idx]);
      return value;
    }
    current = current->next;
  }

  pthread_mutex_unlock(&partition_locks[partition_idx]);
  return NULL;
}


