CC = gcc
CFLAGS = -Wall -Wextra -Werror -pthread -g 
OBJS = threadpool.o mapreduce.o distwc.o


# link all object files and create the wordcount executable
wordcount: $(OBJS)
	$(CC) $(CFLAGS) $(OBJS) -o wordcount


# compile threadpool.c
threadpool.o: threadpool.c
	$(CC) $(CFLAGS) -c threadpool.c -o threadpool.o

# compile mapreduce.c
mapreduce.o: mapreduce.c
	$(CC) $(CFLAGS) -c mapreduce.c -o mapreduce.o

# compile distwc.c
distwc.o: distwc.c
	$(CC) $(CFLAGS) -c distwc.c -o distwc.o



# Target to clean up object files and executable
clean:
	rm -f $(OBJS) wordcount
