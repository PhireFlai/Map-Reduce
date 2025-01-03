#include "threadpool.h"
#include <bits/pthreadtypes.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <unistd.h> // for sleep

typedef void (*thread_func_t)(void *arg);

/**
 * C style constructor for creating a new ThreadPool object
 * Parameters:
 *     num - Number of threads to create
 * Return:
 *     ThreadPool_t* - Pointer to the newly created ThreadPool object
 */

void sample_job(void *arg) {
  int *duration = (int *)arg;
  printf("Processing job of length %d seconds\n", *duration);
  sleep(*duration); // Simulate work by sleeping for the specified duration
  printf("Finished job of length %d seconds\n", *duration);
  free(duration); // Free the allocated argument memory
}

ThreadPool_t *ThreadPool_create(unsigned int num) {

  // initalize threadpool
  ThreadPool_t *pool = (ThreadPool_t *)malloc(sizeof(*pool));

  // make mutex and conditional variables
  pthread_mutex_init(&(pool->lock), NULL);
  pthread_cond_init(&(pool->workAvailable), NULL);
  pthread_cond_init(&(pool->done), NULL);

  // create members
  pool->numThreads = num;
  pool->numWorking = 0;
  pool->stop = 0;
  pool->threads = (pthread_t *)malloc(num * sizeof(pthread_t));

  pool->jobs.size = 0;
  pool->jobs.head = NULL;

  // create each thread
  for (size_t i = 0; i < num; ++i) {
    // printf("thread %d\n", i);
    pthread_create(&pool->threads[i], NULL, Thread_run, pool);
  }
  return pool;
}

/*
 * C style destructor to destroy a ThreadPool object
 * Parameters:
 *     tp - Pointer to the ThreadPool object to be destroyed
 */
void ThreadPool_destroy(ThreadPool_t *tp) {
  // acquire lock for shutdown
  pthread_mutex_lock(&tp->lock);

  // tell every thread to stop
  tp->stop = 1;
  pthread_cond_broadcast(&tp->workAvailable);

  pthread_mutex_unlock(&tp->lock);

  //  joining threads back
  for (size_t i = 0; i < tp->numThreads; ++i) {
    pthread_join(tp->threads[i], NULL);
  }

  // destory and free
  pthread_mutex_destroy(&tp->lock);
  pthread_cond_destroy(&tp->workAvailable);
  pthread_cond_destroy(&tp->done);
  free(tp->threads);

  free(tp);
}

/**
 * Add a job to the ThreadPool's job queue
 * Parameters:
 *     tp   - Pointer to the ThreadPool object
 *     func - Pointer to the function that will be called by the serving
 * thread arg  - Arguments for that function Return: true  - On success false
 * - Otherwise
 */
bool ThreadPool_add_job(ThreadPool_t *tp, thread_func_t func, void *arg,
                        size_t size) {

  // make a job
  ThreadPool_job_t *job = (ThreadPool_job_t *)malloc(sizeof(ThreadPool_job_t));
  if (!job || func == NULL)
    return false;

  job->func = func;
  job->arg = arg;
  job->next = NULL;
  job->size = size;

  // get lock for inserting job
  pthread_mutex_lock(&tp->lock);

  // insert the job based on its size 
  if (tp->jobs.head == NULL || tp->jobs.head->size > size) {
    // insert based on being smaller
    job->next = tp->jobs.head;
    tp->jobs.head = job;
  } else {
    // traverse and find where to insert job
    ThreadPool_job_t *current = tp->jobs.head;
    while (current->next != NULL && current->next->size <= size) {
      current = current->next;
    }
    // Insert the new job
    job->next = current->next;
    current->next = job;
  }

  // printf("job queue:\n");
  // ThreadPool_job_t *temp = tp->jobs.head;
  // while (temp != NULL) {
  //   printf(" - Job of size: %zu\n", temp->size);
  //   temp = temp->next;
  // }
  

  // signal that there is work
  pthread_cond_broadcast(&tp->workAvailable);
  pthread_mutex_unlock(&tp->lock);

  return true;
}

/**
 * Get a job from the job queue of the ThreadPool object
 * Parameters:
 *     tp - Pointer to the ThreadPool object
 * Return:
 *     ThreadPool_job_t* - Next job to run
 */
ThreadPool_job_t *ThreadPool_get_job(ThreadPool_t *tp) {
  ThreadPool_job_t *job;

  // check if threadpool exists
  if (tp == NULL) {
    return NULL;
  }

  // check if job is not null
  job = tp->jobs.head;
  if (job == NULL) {
    return NULL;
  }

  // update job list after getting job out
  if (tp->jobs.head->next == NULL) {
    tp->jobs.head = NULL;
  } else {
    tp->jobs.head = tp->jobs.head->next;
  }
  return job;
};

/**
 * Start routine of each thread in the ThreadPool Object
 * In a loop, check the job queue, get a job (if any) and run it
 * Parameters:
 *     tp - Pointer to the ThreadPool object containing this thread
 */
void *Thread_run(void *arg) {

  ThreadPool_t *tp = arg;
  while (1) {
    // acquire lock
    pthread_mutex_lock(&(tp->lock));

    // wait for job or stop
    while (tp->jobs.head == NULL && !tp->stop) {
      // printf("thread waiting");
      pthread_cond_wait(&(tp->workAvailable), &(tp->lock));
    }

    // if stop unlock and break
    if (tp->stop) {
      pthread_mutex_unlock(&tp->lock);
      break;
    }

    // get job in lock
    ThreadPool_job_t *job = ThreadPool_get_job(tp);
    tp->numWorking++;

    pthread_mutex_unlock(&(tp->lock));

    // execute the job outside the lock
    if (job != NULL) {
      // printf("running job\n");
      job->func(job->arg);
      free(job);
    }

    // complete job and update with lock
    pthread_mutex_lock(&tp->lock);
    tp->numWorking--;

    // signal completeion if no work left
    if (tp->jobs.head == NULL && tp->numWorking == 0) {
      pthread_cond_signal(&tp->done);
    }
    pthread_mutex_unlock(&tp->lock);
  }

  return NULL;
}

/**
 * Ensure that all threads are idle and the job queue is empty before
 * returning Parameters: tp - Pointer to the ThreadPool object that will be
 * destroyed
 */
void ThreadPool_check(ThreadPool_t *tp) {
  // acquire lock
  pthread_mutex_lock(&tp->lock);
  // printf("running waiting\n");
  // wait until the job queue is empty and all threads are idle
  while (tp->jobs.head != NULL || tp->numWorking > 0) {
    pthread_cond_wait(&tp->done, &tp->lock);
  }
  // printf("done waiting\n");

  pthread_mutex_unlock(&tp->lock);
}

