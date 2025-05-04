#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <pthread.h>
#include <sys/stat.h>
#include <dirent.h>
#include <fcntl.h>
#include "stream.h"
#include <ctype.h>
#include <semaphore.h>
#define MAX_PATHS 100
#define MAX_CLIENTS 20
#define BUFFER_SIZE 8192
#define MAX_READS 10
#define PATH_MAX_LENGTH 4096
int nm_socket;

pthread_mutex_t mutex_client = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t mutex_port = PTHREAD_MUTEX_INITIALIZER;
int present_client_port;

typedef struct fileLock
{
    sem_t write_sem;
    sem_t read_sem;
} fileLock;

fileLock FileLock[MAX_PATHS];

int get_hash(const char *s, int table_size)
{
    unsigned long long hash = 0;
    unsigned long long p = 31;
    unsigned long long m = 1e9 + 9;
    unsigned long long p_pow = 1;

    for (int i = 0; s[i] != '\0'; i++)
    {
        char c = tolower(s[i]);
        hash = (hash + (c - ' ' + 1) * p_pow) % m;
        p_pow = (p_pow * p) % m;
    }

    return hash % table_size;
}

void init_semaphores()
{
    for (int i = 0; i < MAX_PATHS; i++)
    {
        sem_init(&FileLock[i].write_sem, 0, 1);
        sem_init(&FileLock[i].read_sem, 0, MAX_READS);
    }
}

void check_sem(sem_t *x, int client_sock)
{
    if (sem_trywait(x) == -1)
    {
        printf("\nConcurrent limit reached! Wait for some time :(\n");
        char message[MAX_PATHS];
        snprintf(message, MAX_PATHS, "Wait for some time :(");
        send(client_sock, message, strlen(message), 0);
        sem_wait(x);
    }
    return;
}

// int nm_socket;
typedef struct Client
{
    int client_sock;
    char client_ip[INET_ADDRSTRLEN];
    struct Client *next;
} Client;
Client *client_list = NULL;
typedef struct Hashtable
{
    char *paths[MAX_PATHS];
    int count_paths;
} Hashtable;
Hashtable paths_accessible;
void initialisation_ht(Hashtable *ht)
{
    for (int i = 0; i < MAX_PATHS; i++)
    {
        ht->paths[i] = NULL;
    }
    ht->count_paths = 0;
}
void addpath_ht(Hashtable *ht, char *newpath)
{
    if (ht->count_paths >= MAX_PATHS)
    {
        printf("Exceeded limit of paths.\n");
    }
    else
    {
        ht->paths[ht->count_paths] = strdup(newpath);
        ht->count_paths++;
    }
}
int valid_path(char *path)
{
    struct stat validpaths;
    return (stat(path, &validpaths) == 0);
}
void input_paths()
{
    char path[1024];
    printf("Enter the accessible paths:\n");
    while (1)
    {
        printf("Path: ");
        fgets(path, sizeof(path), stdin);
        path[strcspn(path, "\n")] = '\0';
        if (strcmp(path, "finish") == 0)
        {
            break;
        }
        if (!valid_path(path))
        {
            printf("Invalid path. Enter correct path.\n");
        }
        else
        {
            addpath_ht(&paths_accessible, path);
        }
    }
}
// int nm_socket;
// void registration_with_nm(char *ip_nm, int port_nm, char *ip_ss, int port_ss, int port_client)
// {
//     char buffer[BUFFER_SIZE];
//     int nm_socket;
//     struct sockaddr_in ss;

//     nm_socket = socket(AF_INET, SOCK_STREAM, 0);
//     if (nm_socket < 0)
//     {
//         perror("Error in creating the socket");
//         exit(0);
//     }

//     int opt = 1;
//     if (setsockopt(nm_socket, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0)
//     {
//         perror("Error in reusing the port");
//         close(nm_socket);
//         exit(0);
//     }

//     ss.sin_port = htons(port_nm);
//     ss.sin_family = AF_INET;

//     if (inet_pton(AF_INET, ip_nm, &ss.sin_addr) <= 0)
//     {
//         printf("Invalid address.Address is not valid\n");
//         exit(0);
//     }

//     if (connect(nm_socket, (struct sockaddr *)&ss, sizeof(ss)) < 0)
//     {
//         perror("Error in connecting to the naming server");
//         exit(0);
//     }

//     snprintf(buffer, sizeof(buffer), "%s %d %d", ip_ss, port_ss, port_client);
//     send(nm_socket, buffer, strlen(buffer), 0);

//     for (int i = 0; i < paths_accessible.count_paths; i++)
//     {
//         usleep(45000);
//         if (paths_accessible.paths[i])
//         {
//             snprintf(buffer, sizeof(buffer), "%s\n", paths_accessible.paths[i]);
//             send(nm_socket, buffer, strlen(buffer), 0);
//         }
//     }
//     send(nm_socket, "END PATHS\n", 10, 0);

//     close(nm_socket);
// }
int path_exists_ht(Hashtable *ht, const char *path)
{
    for (int i = 0; i < ht->count_paths; i++)
    {
        if (ht->paths[i] != NULL && strcmp(ht->paths[i], path) == 0)
        {
            return 1; // Path exists
        }
    }
    return 0; // Path does not exist
}

void send_data_to_nm(char *path)
{
    char buffer[PATH_MAX_LENGTH + 5];
    char ack[PATH_MAX_LENGTH];
    snprintf(buffer, sizeof(buffer), "%s\n", path);
    send(nm_socket, buffer, strlen(buffer), 0);

    // Wait for acknowledgment
    int ack_received = recv(nm_socket, ack, sizeof(ack) - 1, 0);
    if (ack_received > 0)
    {
        ack[ack_received] = '\0';
        if (strncmp(ack, "Path Already exists in one of the servers.", 12) == 0)
        {
            printf("It is there\n");
        }
        else
        {
            addpath_ht(&paths_accessible, path);
        }
        printf("Acknowledgment: %s\n", ack);
    }
}

void list_directory_recursive(const char *base_path, const char *current_path, int level)
{
    struct dirent *entry;
    DIR *dir;
    char full_path[PATH_MAX_LENGTH]; // Increased buffer size
    char entry_path[PATH_MAX_LENGTH];

    // Combine base path and current path
    if (strcmp(current_path, ".") == 0)
    {
        // For the root level, just use the base_path
        snprintf(full_path, sizeof(full_path), "%s", base_path);
    }
    else
    {
        if (snprintf(full_path, sizeof(full_path), "%s/%s", base_path, current_path) >= sizeof(full_path))
        {
            fprintf(stderr, "Path too long: %s/%s\n", base_path, current_path);
            return;
        }
    }

    dir = opendir(full_path);

    if (dir == NULL)
    {
        perror("opendir");
        return;
    }

    while ((entry = readdir(dir)) != NULL)
    {
        // Skip "." and ".."
        if (entry->d_name[0] == '.' &&
            (entry->d_name[1] == '\0' ||
             (entry->d_name[1] == '.' && entry->d_name[2] == '\0')))
        {
            continue;
        }

        // Construct the full path for the current entry
        if (snprintf(entry_path, sizeof(entry_path), "%s/%s", full_path, entry->d_name) >= sizeof(entry_path))
        {
            fprintf(stderr, "Path too long: %s/%s\n", full_path, entry->d_name);
            continue;
        }

        // Print the full path
        for (int i = 0; i < level; i++)
            printf("  "); // Indentation
        printf("%s\n", entry_path);
        char buffer[PATH_MAX_LENGTH + 5];
        char ack[PATH_MAX_LENGTH];
        if (path_exists_ht(&paths_accessible, entry_path) == 0)
        {
            // send_data_to_nm(entry_path);
            snprintf(buffer, sizeof(buffer), "%s\n", entry_path);
            send(nm_socket, buffer, strlen(buffer), 0);

            int ack_received = recv(nm_socket, ack, sizeof(ack) - 1, 0);
            if (ack_received > 0)
            {
                ack[ack_received] = '\0';
                if (strncmp(ack, "Path Already exists in one of the servers.", 12) == 0)
                {
                    printf("It is there\n");
                }
                else
                {
                    addpath_ht(&paths_accessible, entry_path);
                }
                printf("Acknowledgment: %s\n", ack);
            }

            // addpath_ht(&paths_accessible, entry_path);
        }
        else
        {
            printf("Path already exists %s\n\n", entry_path);
        }

        // Check if the entry is a directory
        struct stat path_stat;
        if (stat(entry_path, &path_stat) == 0 && S_ISDIR(path_stat.st_mode))
        {
            // Recurse into the directory
            list_directory_recursive(base_path, entry_path + strlen(base_path) + 1, level + 1);
        }
    }

    closedir(dir);
}

int check_path_type(char *path)
{
    struct stat path_stat;
    if (stat(path, &path_stat) != 0)
    {
        perror("stat");
        return 0; // Error occurred
    }

    if (S_ISREG(path_stat.st_mode))
    {
        printf("%s is a file.\n", path);
        return 1; // It's a file
    }
    else if (S_ISDIR(path_stat.st_mode))
    {
        printf("%s is a directory.\n", path);
        char buffer[PATH_MAX_LENGTH];
        char ack[PATH_MAX_LENGTH];
        if (path_exists_ht(&paths_accessible, path) == 0)
        {
            // send_data_to_nm(path);
            snprintf(buffer, sizeof(buffer), "%s\n", path);
            send(nm_socket, buffer, strlen(buffer), 0);

            // Wait for acknowledgment
            int ack_received = recv(nm_socket, ack, sizeof(ack) - 1, 0);
            if (ack_received > 0)
            {
                ack[ack_received] = '\0';
                if (strncmp(ack, "Path Already exists in one of the servers.", 12) == 0)
                {
                    printf("It is there\n");
                    printf("Acknowledgment: %s\n", ack);
                }
                else
                {
                    addpath_ht(&paths_accessible, path);
                    printf("Acknowledgment: %s\n", ack);
                    list_directory_recursive(path, ".", 0);
                }
            }
        }
        else
        {
            printf("Path already exists %s in ss\n\n", path);
        }

        return 2; // It's a directory
    }
    else
    {
        printf("%s is neither a regular file nor a directory.\n", path);
        return 0; // Neither
    }
    return 0;
}

void registration_with_nm(char *ip_nm, int port_nm, char *ip_ss, int port_ss, int port_client)
{
    char buffer[BUFFER_SIZE];
    // int nm_socket;
    struct sockaddr_in ss;

    nm_socket = socket(AF_INET, SOCK_STREAM, 0);
    if (nm_socket < 0)
    {
        perror("Error in creating the socket");
        exit(0);
    }

    int opt = 1;
    if (setsockopt(nm_socket, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0)
    {
        perror("Error in reusing the port");
        close(nm_socket);
        exit(0);
    }

    ss.sin_port = htons(port_nm);
    ss.sin_family = AF_INET;

    if (inet_pton(AF_INET, ip_nm, &ss.sin_addr) <= 0)
    {
        printf("Invalid address. Address is not valid\n");
        exit(0);
    }

    if (connect(nm_socket, (struct sockaddr *)&ss, sizeof(ss)) < 0)
    {
        perror("Error in connecting to the naming server");
        exit(0);
    }

    snprintf(buffer, sizeof(buffer), "%s %d %d", ip_ss, port_ss, port_client);
    send(nm_socket, buffer, strlen(buffer), 0);

    char ack[BUFFER_SIZE];
    // for (int i = 0; i < paths_accessible.count_paths; i++)
    // {
    char path[1024];
    while (1)
    {
        printf("Path: ");
        fgets(path, sizeof(path), stdin);
        path[strcspn(path, "\n")] = '\0';
        int path_add_dir = paths_accessible.count_paths;
        int check_path = check_path_type(path);
        if (strcmp(path, "finish") == 0)
        {
            send(nm_socket, "END_PATHS\n", 10, 0);

            // Final acknowledgment for end of paths
            int final_ack_received = recv(nm_socket, ack, sizeof(ack) - 1, 0);
            if (final_ack_received > 0)
            {
                ack[final_ack_received] = '\0';
                printf("Final acknowledgment: %s\n\n", ack);
            }
            break;
        }
        if (check_path == 0)
        {
            printf("Invalid path. Enter correct path.\n");
            continue;
        }
        else if (path_exists_ht(&paths_accessible, path) == 0)
        {
            if (check_path == 1)
            { // file
                usleep(45000);
                send_data_to_nm(path);
            }
            else if (check_path == 2)
            { // directory
              // EAT FIVE STAR DO NOTHING!!
            }
        }
        else
        {
            printf("Path already exists %s\n\n", path);
            continue;
        }
    }
    for (int i = 0; i < paths_accessible.count_paths; i++)
    {
        printf("%s\n", paths_accessible.paths[i]);
    }

    close(nm_socket);
}
// **** Added functionalities of client interactions in 1.2 by rishika ****
#define ASYNC_WRITE_THRESHOLD 50
struct async_write_data
{
    char *file_path;
    char *data;
};
void *async_write(void *args)
{
    struct async_write_data *write_data = args;

    FILE *file = fopen(write_data->file_path, "a");
    if (file == NULL)
    {
        perror("Error opening file in async write thread");
        free(write_data->data);
        free(write_data->file_path);
        free(write_data);
        return NULL;
    }

    fwrite(write_data->data, 1, strlen(write_data->data), file);
    fclose(file);
    char buffer[BUFFER_SIZE];

    /// snprintf(buffer, sizeof(buffer), "%s %d %d", ip_ss, port_ss, port_client);

    // snprintf(buffer,sizeof(buffer),"Asynchronous Write completed.\n");
    // send(nm_socket,buffer,strlen(buffer),0);
    printf("Asynchronous write completed.\n");
    // snprintf()

    free(write_data->data);
    free(write_data->file_path);
    free(write_data);
    return NULL;
}
void copy_file(const char *src_path, const char *dest_path)
{
    FILE *src_file = fopen(src_path, "rb");
    if (src_file == NULL)
    {
        perror("Error opening source file");
        return;
    }

    FILE *dest_file = fopen(dest_path, "wb");
    if (dest_file == NULL)
    {
        perror("Error opening destination file");
        fclose(src_file);
        return;
    }

    char buffer[BUFFER_SIZE];
    size_t bytes;
    while ((bytes = fread(buffer, 1, BUFFER_SIZE, src_file)) > 0)
    {
        fwrite(buffer, 1, bytes, dest_file);
    }

    fclose(src_file);
    fclose(dest_file);
}

void copy_directory(const char *src_dir, const char *dest_dir)
{
    DIR *dir = opendir(src_dir);
    if (dir == NULL)
    {
        perror("Error opening source directory");
        return;
    }

    // Create destination directory if it doesn't exist
    mkdir(dest_dir, 0755);

    struct dirent *entry;
    while ((entry = readdir(dir)) != NULL)
    {
        char src_path[BUFFER_SIZE];
        char dest_path[BUFFER_SIZE];

        if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0)
        {
            continue;
        }

        snprintf(src_path, sizeof(src_path), "%s/%s", src_dir, entry->d_name);
        snprintf(dest_path, sizeof(dest_path), "%s/%s", dest_dir, entry->d_name);

        if (entry->d_type == DT_DIR)
        {
            // Recursively copy subdirectory
            copy_directory(src_path, dest_path);
        }
        else
        {
            // Copy file
            copy_file(src_path, dest_path);
        }
    }

    closedir(dir);
}
void receive_and_write_file(int client_sock, int file_fd)
{
    char receive_buffer[1024];
    ssize_t bytes_received;

    while ((bytes_received = recv(client_sock, receive_buffer, sizeof(receive_buffer), 0)) > 0)
    {
        receive_buffer[bytes_received] = '\0';
        char *end_marker_pos = strstr(receive_buffer, "FILE_TRANSFER_END");

        if (end_marker_pos != NULL)
        {
            size_t data_len = end_marker_pos - receive_buffer;

            if (write(file_fd, receive_buffer, data_len) < 0)
            {
                perror("Error writing final data to file");
                break;
            }

            printf("End marker found. File transfer complete.\n");
            break;
        }
        if (write(file_fd, receive_buffer, bytes_received) < 0)
        {
            perror("Error writing to file");
            break;
        }
    }
    if (bytes_received < 0)
    {
        perror("Receive error");
    }
    else
    {
        printf("File data received and written successfully.\n");
        const char *success_message = "File data received and written successfully.";
        send(client_sock, success_message, strlen(success_message), 0);
    }
    close(file_fd);
}
void send_file(int client_sock, const char *file_path)
{
    char buffer[BUFFER_SIZE];
    int file_fd = open(file_path, O_RDONLY);
    if (file_fd < 0)
    {
        perror("Error opening file");
        return;
    }

    // Notify the client of the file being sent
    snprintf(buffer, sizeof(buffer), "FILE_START:%s\n", file_path);
    send(client_sock, buffer, strlen(buffer), 0);

    // Read and send file data
    ssize_t bytes_read;
    while ((bytes_read = read(file_fd, buffer, sizeof(buffer))) > 0)
    {
        send(client_sock, buffer, bytes_read, 0);
    }

    // Notify the client that the file transfer is complete
    const char *end_message = "FILE_TRANSFER_END\n";
    send(client_sock, end_message, strlen(end_message), 0);

    close(file_fd);
}

void send_directory(int client_sock, const char *dir_path)
{
    DIR *dir = opendir(dir_path);
    if (!dir)
    {
        perror("Error opening directory");
        return;
    }

    struct dirent *entry;
    struct stat entry_stat;
    char full_path[BUFFER_SIZE];

    // Notify the client of the directory being sent
    char buffer[BUFFER_SIZE];
    snprintf(buffer, sizeof(buffer), "DIR_START:%s\n", dir_path);
    send(client_sock, buffer, strlen(buffer), 0);

    while ((entry = readdir(dir)) != NULL)
    {
        // Skip "." and ".."
        if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0)
            continue;

        snprintf(full_path, sizeof(full_path), "%s/%s", dir_path, entry->d_name);

        // Get file or directory stats
        if (stat(full_path, &entry_stat) < 0)
        {
            perror("Error reading file/directory stats");
            continue;
        }

        if (S_ISREG(entry_stat.st_mode))
        {
            // If it's a file, send it
            send_file(client_sock, full_path);
        }
        else if (S_ISDIR(entry_stat.st_mode))
        {
            // If it's a directory, recursively traverse it
            send_directory(client_sock, full_path);
        }
    }

    // Notify the client that the directory transfer is complete
    const char *end_message = "DIR_TRANSFER_END\n";
    send(client_sock, end_message, strlen(end_message), 0);

    closedir(dir);
}
// void receive_file_system(int sock, const char *dest_path) {
//     char buffer[BUFFER_SIZE];
//     char current_path[BUFFER_SIZE];
//     FILE *current_file = NULL;

//     while (1) {
//         ssize_t bytes_received = recv(sock, buffer, sizeof(buffer) - 1, 0);
//         if (bytes_received <= 0) {
//             printf("1\n");
//             // Connection closed or error
//             break;
//         }

//         buffer[bytes_received] = '\0';

//         // Handle directory creation
//         if (strncmp(buffer, "DIR:", 4) == 0) {
//             char dir_path[BUFFER_SIZE];
//             sscanf(buffer + 4, "%s", dir_path);

//             // Create the directory
//             snprintf(current_path, sizeof(current_path), "%s", dir_path);
//             if (mkdir(current_path, 0755) == 0) {
//                 printf("Directory created: %s\n", current_path);
//             }
//             else
//             {
//                 printf(":(((\n");
//             }

//         } else if (strncmp(buffer, "FILE:", 5) == 0) {
//             // Close the previous file if open
//             printf("%s\n",buffer);
//             if (current_file) {
//                 fclose(current_file);
//                 current_file = NULL;
//             }

//             // Handle file creation
//             char file_path[BUFFER_SIZE];
//             sscanf(buffer + 5, "%s", file_path);

//             // Open the file for writing
//             snprintf(current_path, sizeof(current_path), "%s", file_path);
//             current_file = fopen(current_path, "wb");
//             if (!current_file) {
//                 printf("%s\n",current_path);
//                 perror("Error creating file");
//                 continue;
//             }

//         }
//         else if (strncmp(buffer, "FILE_END",8) == 0) {
//             // File transfer complete
//             if (current_file) {
//                 fclose(current_file);
//                 current_file = NULL;
//             }

//         }

//         else if (strncmp(buffer, "TRANSFER_COMPLETE",17) == 0) {
//             // Entire file system transfer complete
//             printf("File system transfer complete.\n");
//             break;

//         } else {
//             // Write file data
//             if (current_file) {
//                 fwrite(buffer, 1, bytes_received, current_file);
//             }
//         }
//     }

//     // Close the last file if open
//     if (current_file) {
//         fclose(current_file);
//     }
// }
void receive_file_system(int sock, const char *dest_path)
{
    char buffer[BUFFER_SIZE];
    ssize_t bytes_received;

    while (1)
    {
        // Receive the directory or file information
        printf("1\n");
        bytes_received = recv(sock, buffer, sizeof(buffer), 0);
        printf("ggg%ld\n", bytes_received);
        printf("%s\n", buffer);
        if (bytes_received <= 0)
        {
            perror("Failed to receive data");
            break;
        }

        buffer[bytes_received] = '\0'; // Null-terminate the buffer

        // Handle the message type (DIR, FILE, or END)
        if (strncmp(buffer, "DIR:", 4) == 0)
        {
            // Create the directory (if it doesn't already exist)
            char dir_path[4096];
            snprintf(dir_path, sizeof(dir_path), "%s", buffer + 4);
            dir_path[sizeof(dir_path) - 1] = '\0';
            printf("%s\n", dir_path);
            struct stat st;
            if (stat(dir_path, &st) != 0)
            {
                if (mkdir(dir_path, 0755) != 0)
                {
                    perror("Failed to create directory");
                }
            }
            snprintf(buffer, sizeof(buffer), "File Created");
            send(sock, buffer, strlen(buffer), 0);
            printf("Created directory: %s\n", dir_path);
        }
        else if (strncmp(buffer, "FILE:", 5) == 0)
        {
            // File transfer begins
            char file_path[4096];
            snprintf(file_path, sizeof(file_path), "%s", buffer + 5);
            printf("%s\n", file_path);
            // Open the file for writing
            int fd = open(file_path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
            if (fd == -1)
            {
                perror("Failed to open file");
                continue;
            }
            snprintf(buffer, sizeof(buffer), "File Created");
            send(sock, buffer, strlen(buffer), 0);
            // Receive the file data in chunks
            while (1)
            {
                bytes_received = recv(sock, buffer, sizeof(buffer), 0);
                if (bytes_received <= 0)
                {
                    perror("Failed to receive file data");
                    break;
                }

                if (strncmp(buffer, "FILE_END", 8) == 0)
                {
                    break; // End of file data
                }

                write(fd, buffer, bytes_received);
            }

            close(fd);
            printf("Received file: %s\n", file_path);
            char new_buffer[BUFFER_SIZE];
            snprintf(new_buffer, sizeof(new_buffer), "File Recieved");
            send(sock, new_buffer, strlen(new_buffer), 0);
        }
        else if (strncmp(buffer, "TRANSFER_COMPLETE", 17) == 0)
        {
            // Transfer is complete, break the loop
            printf("File transfer complete.\n");
            break;
        }
    }
}

void *client_handling(void *client_socket)
{
    int client_sock = *((int *)client_socket);
    free(client_socket);
    char buffer[BUFFER_SIZE];
    char data_port[100];

    int data_read;
    data_read = recv(client_sock, buffer, sizeof(buffer) - 1, 0);
    if (data_read <= 0)
    {
        perror("Error in receving the file request from the client");
        close(client_sock);
        return NULL;
    }

    buffer[data_read] = '\0';
    char store[BUFFER_SIZE];
    strcpy(store, buffer);
    printf("%s\n", buffer);
    char *command = strtok(buffer, " ");
    printf("%s\n", command);
    char actual_file_path[BUFFER_SIZE];
    strcpy(actual_file_path, store + strlen(command) + 1);
    if (command == NULL || actual_file_path == NULL)
    {
        snprintf(buffer, sizeof(buffer), "Error in file path\n");
        send(client_sock, buffer, strlen(buffer), 0);
        close(client_sock);
        return NULL;
    }
    if (strcmp(command, "READ") == 0)
    {
        printf("Client requested filepath for reading: %s\n", actual_file_path);
        int index = get_hash(actual_file_path, MAX_PATHS);
        check_sem(&FileLock[index].read_sem, client_sock);
        FILE *file = fopen(actual_file_path, "r");
        if (file == NULL)
        {
            perror("File not found or file cannot be opened");
            snprintf(buffer, sizeof(buffer), "Error: File not found or file cannot be opened.\n");
            send(client_sock, buffer, strlen(buffer), 0);
            close(client_sock);
            return NULL;
        }
        send(client_sock, "File found! Sending data in the file\n", strlen("File found! Sending data in the file\n"), 0);
        while ((data_read = fread(buffer, 1, sizeof(buffer), file)) > 0)
        {
            if (send(client_sock, buffer, data_read, 0) < 0)
            {
                perror("Error in sending the file.");
                break;
            }
        }

        fclose(file);
        printf("File sent sucessfully :)\n");
        sem_post(&FileLock[index].read_sem);
    }
    else if (strcmp(command, "WRITE") == 0)
    {
        int index = get_hash(actual_file_path, MAX_PATHS);
        printf("%d\n", index);
        int value;
        sem_getvalue(&FileLock[index].write_sem, &value);
        printf("%d\n", value);
        // sem_wait(&FileLock[index].write_sem);
        check_sem(&FileLock[index].write_sem, client_sock);
        FILE *file = fopen(actual_file_path, "w");
        if (file == NULL)
        {
            perror("Error in opening the file for writing");
            snprintf(buffer, sizeof(buffer), "Unable to open the file for writing the data requested by client\n");
            send(client_sock, buffer, strlen(buffer), 0);
            close(client_sock);
            return NULL;
        }

        snprintf(buffer, sizeof(buffer), "Ready to receive the data.\n");
        send(client_sock, buffer, strlen(buffer), 0);

        size_t total_data_size = 0;
        char *data_buffer = NULL;

        while ((data_read = recv(client_sock, buffer, sizeof(buffer) - 1, 0)) > 0)
        {
            buffer[data_read] = '\0';
            if (strcmp(buffer, "STOP\n") == 0)
            {
                break;
            }

            total_data_size += data_read;
            data_buffer = realloc(data_buffer, total_data_size + 1);
            if (data_buffer == NULL)
            {
                perror("Error allocating memory for data buffer");
                fclose(file);
                close(client_sock);
                return NULL;
            }
            strncat(data_buffer, buffer, data_read);
        }
        if (total_data_size > ASYNC_WRITE_THRESHOLD)
        {
            snprintf(buffer, sizeof(buffer), "Write request accepted. Data will be written asynchronously.\n");
            send(client_sock, buffer, strlen(buffer), 0);

            struct async_write_data *write_data = malloc(sizeof(struct async_write_data));
            if (write_data == NULL)
            {
                perror("Error allocating memory for async_write_data");
                free(data_buffer);
                fclose(file);
                close(client_sock);
                return NULL;
            }
            write_data->file_path = strdup(actual_file_path);
            write_data->data = data_buffer;
            pthread_t write_thread;
            pthread_create(&write_thread, NULL, async_write, write_data);
            pthread_detach(write_thread);
        }
        else
        {
            fwrite(data_buffer, 1, total_data_size, file);
            fclose(file);
            printf("Data requested by client is written to the file successfully :)\n");
            snprintf(buffer, sizeof(buffer), "Data written successfully :)\n");
            send(client_sock, buffer, strlen(buffer), 0);
            free(data_buffer);
        }
        sem_post(&FileLock[index].write_sem);
    }
    else if (strcmp(command, "INFO") == 0)
    {
        struct stat file;
        if (stat(actual_file_path, &file) != 0)
        {
            perror("Error in retriving the information of the file");
            snprintf(buffer, sizeof(buffer), "Error: File not found or cannot be accessed\n");
            send(client_sock, buffer, strlen(buffer), 0);
            close(client_sock);
            return NULL;
        }
        send(client_sock, "Sending information of req file\n", strlen("Sending information of req file\n"), 0);
        // Sending data in the file
        snprintf(buffer, sizeof(buffer), "Size: %lld bytes\nPermissions: %o\n", (long long)file.st_size, file.st_mode & 0777);
        send(client_sock, buffer, strlen(buffer), 0);

        printf("File Information has sent to client successfully :)\n");
    }
    else if (strcmp(command, "STREAM") == 0)
    {
        if (send_audio(actual_file_path, client_sock))
            printf("Audio file sent to client successfully:)\n");
        else
            printf("Audio file couldn't be sent :(\n");
    }
    else if (strcmp(command, "CREATE") == 0)
    {
        char *fd_str = strtok(actual_file_path, " ");
        char *path = strtok(NULL, " ");
        char *filename = strtok(NULL, " ");
        int fd = atoi(fd_str);
        if (path && filename)
        {
            char full_path[BUFFER_SIZE];
            snprintf(full_path, sizeof(full_path), "%s/%s", path, filename);
            if (fd == 1)
            {
                int file_fd = open(full_path, O_CREAT | O_WRONLY, 0644);
                if (file_fd == -1)
                {
                    perror("Error creating file");
                }
                else
                {
                    printf("File created successfully at: %s\n", full_path);
                    close(file_fd);
                    addpath_ht(&paths_accessible, full_path);
                    snprintf(buffer, sizeof(buffer), "File Created Successfully.\n");
                    send(client_sock, buffer, strlen(buffer), 0);
                }
            }
            else
            {
                if (mkdir(full_path, 0755) == -1)
                {
                    perror("Error creating directory");
                }
                {
                    printf("Folder created successfully at: %s\n", full_path);
                    addpath_ht(&paths_accessible, full_path);
                    snprintf(buffer, sizeof(buffer), "Folder Created Successfully.\n");
                    send(client_sock, buffer, strlen(buffer), 0);
                }
            }
        }
    }
    else if (strcmp(command, "DELETEFILE") == 0)
    {
        int index = get_hash(actual_file_path);
        check_sem(&FileLock[index].write_sem, client_sock);
        printf("NM requested filepath for deleting: %s\n", actual_file_path);
        if (remove(actual_file_path) == 0)
        {
            printf("File deleted successfully\n");
            snprintf(buffer, sizeof(buffer), "File deleted successfully.\n");
            send(client_sock, buffer, strlen(buffer), 0);
        }
        else
        {
            perror("Error deleting file");
        }
        sem_post(&FileLock[index].write_sem);
    }
    else if (strcmp(command, "DELETEFOLDER") == 0)
    {
        int index = get_hash(actual_file_path);
        check_sem(&FileLock[index].write_sem, client_sock);
        printf("NM requested filepath for deleting: %s\n", actual_file_path);
        if (rmdir(actual_file_path) == 0)
        {
            printf("Folder deleted successfully\n");
            snprintf(buffer, sizeof(buffer), "Folder deleted successfully.\n");
            send(client_sock, buffer, strlen(buffer), 0);
        }
        else
        {
            perror("Error deleting folder");
        }
        sem_post(&FileLock[index].write_sem);
    }
    else if (strcmp(command, "COPY1") == 0)
    {
        char *bhh = strtok(actual_file_path, " ");
        char *src_path = strtok(NULL, " ");
        char *dest_path = strtok(NULL, " ");
        printf("src_path:%s\n", src_path);
        printf("dest_path:%s\n", dest_path);

        struct stat src_stat, dest_stat;
        if (stat(src_path, &src_stat) != 0)
        {
            perror("Error accessing source path");
        }

        if (stat(dest_path, &dest_stat) == 0 && S_ISDIR(dest_stat.st_mode))
        {
            if (S_ISREG(src_stat.st_mode))
            {
                char dest_file_path[BUFFER_SIZE];
                snprintf(dest_file_path, sizeof(dest_file_path), "%s/%s", dest_path, strrchr(src_path, '/') + 1);
                copy_file(src_path, dest_file_path);
                printf("File copied successfully.\n");
            }
            else if (S_ISDIR(src_stat.st_mode))
            {
                char dest_dir_path[BUFFER_SIZE];
                snprintf(dest_dir_path, sizeof(dest_dir_path), "%s/%s", dest_path, strrchr(src_path, '/') + 1);
                copy_directory(src_path, dest_dir_path);
                printf("Directory copied successfully.\n");
            }
        }
        printf("Data Copied Successfully\n");
        snprintf(buffer, sizeof(buffer), "Data Copied successfully.\n");
        send(client_sock, buffer, strlen(buffer), 0);
    }
    else if (strcmp(command, "COPY2") == 0)
    {
        char *bhh = strtok(actual_file_path, " ");
        char *src_path = strtok(NULL, " ");
        char *dest_path = strtok(NULL, " ");
        printf("src_path:%s\n", src_path);
        printf("dest_path:%s\n", dest_path);
        char buffer[BUFFER_SIZE];
        snprintf(buffer, sizeof(buffer), "Data from Src File\n");
        send(client_sock, buffer, strlen(buffer), 0);
        int file_fd = open(src_path, O_RDONLY);
        if (file_fd < 0)
        {
            perror("File open error");
        }
        char new_buffer[1024];
        ssize_t bytes_read;
        while ((bytes_read = read(file_fd, new_buffer, sizeof(new_buffer))) > 0)
        {
            printf("1\n");
            printf("%s\n", new_buffer);
            send(client_sock, new_buffer, bytes_read, 0);
        }
        close(file_fd);
        char *end_message = "FILE_TRANSFER_END";
        send(client_sock, end_message, strlen(end_message), 0);
    }
    else if (strcmp(command, "DEST") == 0)
    {
        printf("HUhhhh:)\n");
        printf("%s\n", actual_file_path);
        char *bhh = strtok(actual_file_path, " ");
        char *hfhh = strtok(NULL, " ");
        char *src_path = strtok(NULL, " ");
        char *dest_path = strtok(NULL, " ");
        printf("src_path:%s\n", src_path);
        printf("dest_path:%s\n", dest_path);
        char *src_name = strrchr(src_path, '/');
        if (src_name)
            src_name++;
        else
            src_name = src_path;
        char new_path[BUFFER_SIZE];
        snprintf(new_path, sizeof(new_path), "%s/%s", dest_path, src_name);
        int file_fd = open(new_path, O_WRONLY | O_CREAT | O_TRUNC, 0666);
        if (file_fd < 0)
        {
            perror("Error creating file");
            exit(1);
        }
        printf("File created successfully at: %s\n", new_path);
        snprintf(buffer, sizeof(buffer), "Ready to Receive data");
        send(client_sock, buffer, strlen(buffer), 0);
        receive_and_write_file(client_sock, file_fd);
    }
    else if (strcmp(command, "COPY3") == 0)
    {
        char *bhh = strtok(actual_file_path, " ");
        char *src_path = strtok(NULL, " ");
        char *dest_path = strtok(NULL, " ");
        printf("src_path:%s\n", src_path);
        printf("dest_path:%s\n", dest_path);
        char buffer[BUFFER_SIZE];
        snprintf(buffer, sizeof(buffer), "Data from Src Folder\n");
        send(client_sock, buffer, strlen(buffer), 0);
        struct stat path_stat;
        if (stat(src_path, &path_stat) < 0)
        {
            perror("Error getting path info");
            exit(1);
            // return 1;
        }
        send_directory(client_sock, src_path);
    }
    else if (strcmp(command, "DEST1") == 0)
    {
        char *bhh = strtok(actual_file_path, " ");
        char *hfhh = strtok(NULL, " ");
        char *src_path = strtok(NULL, " ");
        char *dest_path = strtok(NULL, " ");
        printf("src_path:%s\n", src_path);
        printf("dest_path:%s\n", dest_path);
        // char *src_name = strrchr(src_path, '/');
        // if (src_name) src_name++;
        // else src_name = src_path;
        // char new_path[BUFFER_SIZE];
        // snprintf(new_path, sizeof(new_path), "%s/%s", dest_path, src_name);
        // if (mkdir(new_path, 0755) == 0)
        // {
        //     printf("Directory created successfully: %s\n", new_path);
        // }
        snprintf(buffer, sizeof(buffer), "Ready to Receive data");
        send(client_sock, buffer, strlen(buffer), 0);
        receive_file_system(client_sock, dest_path);
    }
    close(client_sock);
    return NULL;
}
void startClient_connection(char *ip_ss, int port_client_init)
{
    int client_socket;
    struct sockaddr_in client_addr;

    present_client_port = port_client_init;

    client_socket = socket(AF_INET, SOCK_STREAM, 0);
    if (client_socket < 0)
    {
        perror("Error in creating the socket for client.");
        exit(0);
    }

    client_addr.sin_family = AF_INET;
    client_addr.sin_port = htons(port_client_init);
    client_addr.sin_addr.s_addr = inet_addr(ip_ss);
    int opt = 1;
    if (setsockopt(client_socket, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0)
    {
        perror("Error in reusing the port");
        close(client_socket);
        exit(0);
    }
    if (bind(client_socket, (struct sockaddr *)&client_addr, sizeof(client_addr)) < 0)
    {
        perror("Error in binding in socket");
        close(client_socket);
        exit(0);
    }
    if (listen(client_socket, 5) < 0)
    {
        perror("Error in listening the client.");
        close(client_socket);
        exit(0);
    }
    printf("Storage server is ready for clients on port %d\n", port_client_init);
    // int *connection_client;
    socklen_t client_addr_len = sizeof(client_addr);
    int current_port = port_client_init;
    while (1)
    {
        int *connection_client = malloc(sizeof(int)); // To store the file fd of accepted client connection
        if (connection_client == NULL)
        {
            perror("Error in allocating the memory");
            continue;
        }
        *connection_client = accept(client_socket, (struct sockaddr *)&client_addr, &client_addr_len);
        if (*connection_client < 0)
        {
            perror("Error in connecting to the client");
            free(connection_client);
            continue;
        }

        char buffer[1024];
        // snprintf(buffer,sizeof(buffer),"Connected to storage server client port.\n");
        // send(connection_client,buffer,strlen(buffer),0);
        printf("Accepted client connection using PORT: %d\n", current_port);
        // client_handling(current_port,client_socket);
        pthread_t client_thread;
        if (pthread_create(&client_thread, NULL, client_handling, connection_client) != 0)
        {
            perror("Error in creating the thread");
            close(*connection_client);
            free(connection_client);
        }
        else
        {
            pthread_detach(client_thread);
        }
    }
    close(client_socket);
}

int main(int argc, char *argv[])
{
    if (argc <= 5)
    {
        printf("Error in command line input.\n");
        exit(0);
    }

    char *ip_nm = argv[1];
    int port_nm = atoi(argv[2]);
    char *ip_ss = argv[3];
    int port_ss = atoi(argv[4]);
    int port_client = atoi(argv[5]);
    init_semaphores();
    initialisation_ht(&paths_accessible);
    // input_paths();
    registration_with_nm(ip_nm, port_nm, ip_ss, port_ss, port_client);
    startClient_connection(ip_ss, port_client);

    return 0;
}