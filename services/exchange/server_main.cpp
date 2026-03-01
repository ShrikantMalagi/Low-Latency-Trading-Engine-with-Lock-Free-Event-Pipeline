#include "exchange.hpp"
#include "wire.hpp"

#include <iostream>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
#include <arpa/inet.h>

#include <cerrno>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <vector>

namespace{
    int make_listen_socket(int port);

    int accept_client(int listen_fd);

    bool read_exact(int fd, void* buf, size_t n);

    bool write_all(int fd, const void* buf, size_t n);
}

int main()
{   

    int port = 9000;

    int serverSocket = socket(AF_INET, SOCK_STREAM, 0);

    sockaddr_in serverAddress;
    serverAddress.sin_family = AF_INET;
    serverAddress.sin_port = htons(9000);
    serverAddress.sin_addr.s_addr = INADDR_ANY;

    return 0;
}