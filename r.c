#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>

#define PORT 8080

int main()
{
    int server_fd, new_socket;
    struct sockaddr_in address;
    char buffer[1024] = {0};
    int addr_len = sizeof(address);

    // Create server socket
    if ((server_fd = socket(AF_INET, SOCK_STREAM, 0)) == 0)
    {
        perror("Socket creation failed");
        exit(EXIT_FAILURE);
    }

    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY; // Accept connections on any local IP
    address.sin_port = htons(PORT);

    // Bind the socket to the IP and port
    if (bind(server_fd, (struct sockaddr *)&address, sizeof(address)) < 0)
    {
        perror("Bind failed");
        exit(EXIT_FAILURE);
    }

    // Listen for incoming connections
    if (listen(server_fd, 3) < 0)
    {
        perror("Listen failed");
        exit(EXIT_FAILURE);
    }

    printf("Server is listening on port %d...\n", PORT);

    // Accept incoming connections
    if ((new_socket = accept(server_fd, (struct sockaddr *)&address, &addr_len)) < 0)
    {
        perror("Accept failed");
        exit(EXIT_FAILURE);
    }

    // Read the message from the client
    read(new_socket, buffer, 1024);
    printf("Message received: %s\n", buffer);

    // Send a response to the client
    char response[] = "Hello from server!";
    send(new_socket, response, strlen(response), 0);
    printf("Response sent to client: %s\n", response);

    // Close the socket
    close(new_socket);
    close(server_fd);

    return 0;
}