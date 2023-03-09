#include <stdio.h>
#include <unistd.h>
#include <sys/wait.h>
#include <stdint.h>
#include <assert.h>
#include <fcntl.h> // For O_* constants.
#include <semaphore.h>
#include <stdbool.h>
#include <stdlib.h>
//#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h> // For mode constants.
#include <sys/types.h>
#include <time.h>
#include <string.h>
#include <pthread.h>
#include "err.h"
#include "utils.h"

#define MAX_N_TASKS 4096
#define MAX_LINE_LENGTH 1022
#define MAX_COMMAND_LENGTH 511

struct Task {
    pid_t task_pid;
    char std_out[MAX_LINE_LENGTH + 1];
    char std_err[MAX_LINE_LENGTH + 1];
    int fds_out[2];
    int fds_err[2];
    sem_t mutex_pid;
    sem_t mutex_out;
    sem_t mutex_err;
    sem_t can_read_out;
    sem_t can_read_err;
};

struct Task tasks[MAX_N_TASKS];
int how_many_tasks = 0;

struct Queue_element {
    int task_number;
    int status_returned;
};

struct Queue_element queue[MAX_N_TASKS];
sem_t mutex_queue;
int beginning_in_queue = 0;
int ending_in_queue = 0;

bool during_command_handling = false;
sem_t mutex_command_handling;

pthread_t assistants[MAX_N_TASKS];
pthread_t readers_out[MAX_N_TASKS];
pthread_t readers_err[MAX_N_TASKS];

pthread_attr_t attr;

int tasks_numbers[MAX_N_TASKS];

struct Data_for_assistant {
    int task_no;
    char **arguments;
};

struct Data_for_assistant data_for_assistant[MAX_N_TASKS];

void* assistant_main(void* data) {
    struct Data_for_assistant * my_data = data;
    int task_number = my_data->task_no;

    pid_t pid = fork();
    ASSERT_SYS_OK(pid);
    if (!pid) {
        // Close the read descriptor.
        ASSERT_SYS_OK(close(tasks[task_number].fds_out[0]));
        ASSERT_SYS_OK(close(tasks[task_number].fds_err[0]));

        printf("Task %d started: pid %d.\n", task_number, getpid());

        // Replace the standard output with the pipe's writing end.
        ASSERT_SYS_OK(dup2(tasks[task_number].fds_out[1], STDOUT_FILENO));
        ASSERT_SYS_OK(close(tasks[task_number].fds_out[1]));  // Close the original copy.

        ASSERT_SYS_OK(dup2(tasks[task_number].fds_err[1], STDERR_FILENO));
        ASSERT_SYS_OK(close(tasks[task_number].fds_err[1]));  // Close the original copy.

        ASSERT_SYS_OK(execvp(my_data->arguments[1], &(my_data->arguments[1])));
    } else {
        // Close the write descriptor.
        ASSERT_SYS_OK(close(tasks[task_number].fds_out[1]));
        ASSERT_SYS_OK(close(tasks[task_number].fds_err[1]));

        sem_wait(&tasks[task_number].mutex_pid);
        tasks[task_number].task_pid = pid;
        sem_post(&tasks[task_number].mutex_pid);
        sem_post(&tasks[task_number].can_read_out);
        sem_post(&tasks[task_number].can_read_err);

        int status_info;
        // Wait for child
        ASSERT_SYS_OK(waitpid(pid, &status_info, 0));
        sem_wait(&mutex_command_handling);
        sem_wait(&tasks[task_number].mutex_pid);
        tasks[task_number].task_pid = 0;
        sem_post(&tasks[task_number].mutex_pid);

        if (during_command_handling) {
            sem_wait(&mutex_queue);
            queue[ending_in_queue].task_number = task_number;
            queue[ending_in_queue].status_returned = status_info;
            ending_in_queue++;
            sem_post(&mutex_queue);
        }
        else {
            pthread_join(readers_out[task_number], NULL);
            readers_out[task_number] = 0;
            pthread_join(readers_err[task_number], NULL);
            readers_err[task_number] = 0;
            if (WIFEXITED(status_info)) {
                printf("Task %d ended: status %d.\n", task_number, WEXITSTATUS(status_info));
            }
            else {
                printf("Task %d ended: signalled.\n", task_number);
            }
        }
        sem_post(&mutex_command_handling);
    }

    return NULL;
}

void* readers_out_main(void* data) {
    int * my_data = data;
    int task_number = *my_data;

    sem_wait(&tasks[task_number].can_read_out);

    // For reading
    char *buffer_out = malloc((MAX_LINE_LENGTH + 1) * sizeof(char));
    size_t buffer_out_size = MAX_LINE_LENGTH + 1;
    ssize_t buffer_out_length = 0;

    // For storing data that is not rewritten into tasks
    char *last_line = malloc((MAX_LINE_LENGTH + 1) * sizeof(char));
    last_line[0] = '\0';


    while (true) {
        buffer_out_length = read(tasks[task_number].fds_out[0], buffer_out, buffer_out_size);

        if (buffer_out_length == 0) { // The pipe is closed and all is read
            int last_line_length = (int)strlen(last_line);
            if (last_line_length > 0) { // There is something to rewrite as "last line not ended with '\n'"
                sem_wait(&(tasks[task_number].mutex_out));
                for (int i = 0; i < last_line_length; ++i) {
                    tasks[task_number].std_out[i] = last_line[i];
                }
                tasks[task_number].std_out[last_line_length] = '\0';
                last_line[0] = '\0';
                sem_post(&(tasks[task_number].mutex_out));
            }
            break;
        }
        else { // Look for positions of last two '\n' signs.
            int last_end_line = -1;
            int previous_end_line = -1;

            for (int i = 0; i < (int)buffer_out_length; ++i) {
                if (buffer_out[i] == '\n') {
                    previous_end_line = last_end_line;
                    last_end_line = i;
                }
            }
            // If there are no '\n' signs, then concatenate last_line + buffer
            if (last_end_line == -1) {
                int last_line_length = (int)strlen(last_line);
                for (int i = 0; i < buffer_out_length; ++i) {
                    last_line[last_line_length + i] = buffer_out[i];
                }
                last_line[last_line_length + buffer_out_length] = '\0';
            }
            // If there is one '\n' sign, then concatenate first part with last_line
            // and rewrite into tasks. Update last_line
            if ((last_end_line != -1) && (previous_end_line == -1)) {
                int last_line_length = (int)strlen(last_line);
                for (int i = 0; i < last_end_line; ++i) { // We don't want to rewrite '\n'
                    last_line[last_line_length + i] = buffer_out[i];
                }
                last_line[last_line_length + last_end_line] = '\0';

                last_line_length = (int)strlen(last_line);
                sem_wait(&(tasks[task_number].mutex_out));
                for (int i = 0; i < last_line_length; ++i) {
                    tasks[task_number].std_out[i] = last_line[i];
                }
                tasks[task_number].std_out[last_line_length] = '\0';
                last_line[0] = '\0';
                sem_post(&(tasks[task_number].mutex_out));

                for (int i = last_end_line + 1; i < buffer_out_length; ++i) {
                    last_line[i - last_end_line - 1] = buffer_out[i];
                }
                last_line[buffer_out_length - last_end_line - 1] = '\0';
            }
            // If there are at least two '\n' signs, then rewrite into tasks what's between them
            // and update last_line
            if ((last_end_line != -1) && (previous_end_line != -1)) {
                last_line[0] = '\0';

                sem_wait(&(tasks[task_number].mutex_out));
                for (int i = previous_end_line + 1; i < last_end_line; ++i) {
                    tasks[task_number].std_out[i - previous_end_line - 1] = buffer_out[i];
                }
                tasks[task_number].std_out[last_end_line - previous_end_line - 1] = '\0';
                sem_post(&(tasks[task_number].mutex_out));

                for (int i = last_end_line + 1; i < buffer_out_length; ++i) {
                    last_line[i - last_end_line - 1] = buffer_out[i];
                }
                last_line[buffer_out_length - last_end_line - 1] = '\0';
            }
        }
    }

    free(buffer_out);
    free(last_line);
    ASSERT_SYS_OK(close(tasks[task_number].fds_out[0]));
    return NULL;
}

void* readers_err_main(void* data) {
    int * my_data = data;
    int task_number = *my_data;

    sem_wait(&tasks[task_number].can_read_err);

    // For reading
    char *buffer_err = malloc((MAX_LINE_LENGTH + 1) * sizeof(char));
    size_t buffer_err_size = MAX_LINE_LENGTH + 1;
    ssize_t buffer_err_length = 0;

    // For storing data that is not rewritten into tasks
    char *last_line = malloc((MAX_LINE_LENGTH + 1) * sizeof(char));
    last_line[0] = '\0';

    while (true) {
        buffer_err_length = read(tasks[task_number].fds_err[0], buffer_err, buffer_err_size);

        if (buffer_err_length == 0) { // The pipe is closed
            int last_line_length = (int)strlen(last_line);
            if (last_line_length > 0) { // There is something to rewrite as "last line not ended with '\n'"
                sem_wait(&(tasks[task_number].mutex_err));
                for (int i = 0; i < last_line_length; ++i) {
                    tasks[task_number].std_err[i] = last_line[i];
                }
                tasks[task_number].std_err[last_line_length] = '\0';
                last_line[0] = '\0';
                sem_post(&(tasks[task_number].mutex_err));
            }
            break;
        }
        else { // Look for positions of last two '\n' signs.
            int last_end_line = -1;
            int previous_end_line = -1;

            for (int i = 0; i < (int)buffer_err_length; ++i) {
                if (buffer_err[i] == '\n') {
                    previous_end_line = last_end_line;
                    last_end_line = i;
                }
            }
            // If there are no '\n' signs, then concatenate last_line + buffer
            if (last_end_line == -1) {
                int last_line_length = (int)strlen(last_line);
                for (int i = 0; i < buffer_err_length; ++i) {
                    last_line[last_line_length + i] = buffer_err[i];
                }
                last_line[last_line_length + buffer_err_length] = '\0';
            }
            // If there is one '\n' sign, then concatenate first part with last_line
            // and rewrite into tasks. Update last_line
            if ((last_end_line != -1) && (previous_end_line == -1)) {
                int last_line_length = (int)strlen(last_line);
                for (int i = 0; i < last_end_line; ++i) { // We don't want to rewrite '\n'
                    last_line[last_line_length + i] = buffer_err[i];
                }
                last_line[last_line_length + last_end_line] = '\0';

                last_line_length = (int)strlen(last_line);
                sem_wait(&(tasks[task_number].mutex_err));
                for (int i = 0; i < last_line_length; ++i) {
                    tasks[task_number].std_err[i] = last_line[i];
                }
                tasks[task_number].std_err[last_line_length] = '\0';
                last_line[0] = '\0';
                sem_post(&(tasks[task_number].mutex_err));

                for (int i = last_end_line + 1; i < buffer_err_length; ++i) {
                    last_line[i - last_end_line - 1] = buffer_err[i];
                }
                last_line[buffer_err_length - last_end_line - 1] = '\0';
            }
            // If there are at least two '\n' signs, then rewrite into tasks what's between them
            // and update last_line
            if ((last_end_line != -1) && (previous_end_line != -1)) {
                last_line[0] = '\0';

                sem_wait(&(tasks[task_number].mutex_err));
                for (int i = previous_end_line + 1; i < last_end_line; ++i) {
                    tasks[task_number].std_err[i - previous_end_line - 1] = buffer_err[i];
                }
                tasks[task_number].std_err[last_end_line - previous_end_line - 1] = '\0';
                sem_post(&(tasks[task_number].mutex_err));

                for (int i = last_end_line + 1; i < buffer_err_length; ++i) {
                    last_line[i - last_end_line - 1] = buffer_err[i];
                }
                last_line[buffer_err_length - last_end_line - 1] = '\0';
            }
        }
    }

    free(buffer_err);
    free(last_line);
    ASSERT_SYS_OK(close(tasks[task_number].fds_err[0]));
    return NULL;
}

void run(int id, char ***args) {
    data_for_assistant[id].task_no = id;
    int counter = 0;
    char **tmp = *args;
    data_for_assistant[id].arguments = calloc(MAX_COMMAND_LENGTH + 1, sizeof(char*));
    while (*tmp != NULL) {
        data_for_assistant[id].arguments[counter] = calloc((int)strlen(*tmp) + 1, sizeof(char));
        memcpy(data_for_assistant[id].arguments[counter], *tmp, strlen(*tmp) + 1);
        ++tmp;
        ++counter;
    }
    data_for_assistant[id].arguments[counter] = NULL;

    ASSERT_SYS_OK(pipe(tasks[id].fds_out));
    ASSERT_SYS_OK(pipe(tasks[id].fds_err));

    // Creating threads: assistant, reader_out, reader_err
    ASSERT_ZERO(pthread_create(&(assistants[id]), &attr, assistant_main, (void*)&data_for_assistant[id]));
    ASSERT_ZERO(pthread_create(&(readers_out[id]), &attr, readers_out_main, (void*)&(tasks_numbers[id])));
    ASSERT_ZERO(pthread_create(&(readers_err[id]), &attr, readers_err_main, (void*)&(tasks_numbers[id])));

}

void out(int T) {
    sem_wait(&tasks[T].mutex_out);
    printf("Task %d stdout: '%s'.\n", T, tasks[T].std_out);
    sem_post(&tasks[T].mutex_out);
}

void err(int T) {
    sem_wait(&tasks[T].mutex_err);
    printf("Task %d stderr: '%s'.\n", T, tasks[T].std_err);
    sem_post(&tasks[T].mutex_err);
}

void my_kill(int T) {
    sem_wait(&tasks[T].mutex_pid);
    if (tasks[T].task_pid != 0) {
        kill(tasks[T].task_pid, SIGINT);
    }
    sem_post(&tasks[T].mutex_pid);
}

void my_sleep(int N) { // N in milliseconds, w usleep in microseconds
    ASSERT_SYS_OK(usleep(1000 * N));
}

void destroy_semaphores() {
    for (int i = 0; i < MAX_N_TASKS; ++i) {
        sem_destroy(&(tasks[i].mutex_pid));
        sem_destroy(&(tasks[i].mutex_out));
        sem_destroy(&(tasks[i].mutex_err));
        sem_destroy(&(tasks[i].can_read_out));
        sem_destroy(&(tasks[i].can_read_err));
    }
    sem_destroy(&mutex_queue);
    sem_destroy(&mutex_command_handling);
}

void clean_memory() {

    for (int i = 0; i < MAX_N_TASKS; ++i) {
        if (data_for_assistant[i].arguments != NULL) {
            for (int j = 0; j < MAX_COMMAND_LENGTH + 1; ++j) {
                if (data_for_assistant[i].arguments[j] == NULL) {
                    break;
                }
                free(data_for_assistant[i].arguments[j]);
            }
            free(data_for_assistant[i].arguments);
        }
    }
}

void quit() {
    for (int i = 0; i < MAX_N_TASKS; ++i) {
        sem_wait(&tasks[i].mutex_pid);
        pid_t pid = tasks[i].task_pid;
        sem_post(&tasks[i].mutex_pid);
        if (pid != 0) {
            kill(pid, SIGKILL);
        }
        if (assistants[i] != 0) {
            pthread_join(assistants[i], NULL);
        }
        if (readers_out[i] != 0) {
            pthread_join(readers_out[i], NULL);
        }
        if (readers_err[i] != 0) {
            pthread_join(readers_err[i], NULL);
        }
    }
    clean_memory();
    destroy_semaphores();
    exit(0);
}

int main() {

    setbuf(stdin, 0);
    setbuf(stdout, 0);

    ASSERT_SYS_OK(pthread_attr_init(&attr));
    ASSERT_SYS_OK(pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE));

    // Some strings to compare with
    char run_command[] = "run\0";
    char out_command[] = "out\0";
    char err_command[] = "err\0";
    char kill_command[] = "kill\0";
    char sleep_command[] = "sleep\0";
    char quit_command[] = "quit\0";

    // Semaphores initialization and variable declaration
    sem_init(&mutex_queue, 0, 1);
    sem_init(&mutex_command_handling, 0, 1);
    for (int i = 0; i < MAX_N_TASKS; ++i) {
        tasks[i].task_pid = 0;
        tasks[i].std_out[0] = '\0';
        tasks[i].std_err[0] = '\0';
        sem_init(&tasks[i].mutex_pid, 0, 1);
        sem_init(&tasks[i].mutex_out, 0, 1);
        sem_init(&tasks[i].mutex_err, 0, 1);
        sem_init(&tasks[i].can_read_out, 0, 0);
        sem_init(&tasks[i].can_read_err, 0, 0);
    }

    for (int i = 0; i < MAX_N_TASKS; ++i) {
        tasks_numbers[i] = i;
    }

    // Process of reading commands from file
    bool read_next_line = true;
    char buffer_for_command[MAX_COMMAND_LENGTH + 1];
    size_t size_of_buffer_for_command = MAX_COMMAND_LENGTH + 1;
    while (read_next_line) {
        read_next_line = read_line(buffer_for_command, size_of_buffer_for_command, stdin);

        // Executing a command
        sem_wait(&mutex_command_handling);
        during_command_handling = true;
        sem_post(&mutex_command_handling);

        if (read_next_line == false) {
            quit();
        }
        if (buffer_for_command[0] == '\n') {
            continue;
        }

        char **result_of_spliting = split_string(buffer_for_command);

        if (strcmp(result_of_spliting[0], run_command) == 0) {
            run(how_many_tasks++, &result_of_spliting);
        }
        else if (strcmp(result_of_spliting[0], out_command) == 0) {
            out(atoi(result_of_spliting[1]));
        }
        else if (strcmp(result_of_spliting[0], err_command) == 0) {
            err(atoi(result_of_spliting[1]));
        }
        else if (strcmp(result_of_spliting[0], kill_command) == 0) {
            my_kill(atoi(result_of_spliting[1]));
        }
        else if (strcmp(result_of_spliting[0], sleep_command) == 0) {
            my_sleep(atoi(result_of_spliting[1]));
        }
        else if (strcmp(result_of_spliting[0], quit_command) == 0) {
            free_split_string(result_of_spliting); // Caution! There is exit() in quit, so here perform memory deallocation
            quit();
        }
        free_split_string(result_of_spliting);

        sem_wait(&mutex_command_handling);
        during_command_handling = false;
        sem_post(&mutex_command_handling);

        // Info about tasks that ended while executing a command
        sem_wait(&mutex_queue);
        while (beginning_in_queue < ending_in_queue) {
            pthread_join(readers_out[queue[beginning_in_queue].task_number], NULL);
            readers_out[queue[beginning_in_queue].task_number] = 0;
            pthread_join(readers_err[queue[beginning_in_queue].task_number], NULL);
            readers_err[queue[beginning_in_queue].task_number] = 0;
            if (WIFEXITED(queue[beginning_in_queue].status_returned)) {
                printf("Task %d ended: status %d.\n", queue[beginning_in_queue].task_number, WEXITSTATUS(queue[beginning_in_queue].status_returned));
            }
            else {
                printf("Task %d ended: signalled.\n", queue[beginning_in_queue].task_number);
            }
            beginning_in_queue++;
        }
        sem_post(&mutex_queue);
    }

    sem_wait(&mutex_command_handling);
    during_command_handling = true;
    sem_post(&mutex_command_handling);

    quit();

    return 0;
}