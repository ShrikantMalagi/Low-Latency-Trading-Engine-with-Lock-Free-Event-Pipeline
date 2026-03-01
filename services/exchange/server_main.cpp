#include "exchange.hpp"
#include "wire.hpp"
#include "error.hpp"

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
    std::expected<int,Error> make_listen_socket(int port){
        int serverSocket = socket(AF_INET, SOCK_STREAM, 0);
        if(serverSocket < 0){
            std::cerr << "socket failed: " << std::strerror(errno) << "\n";
            return -1;
        }
        int yes = 1;

        if(::setsockopt(serverSocket, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes))){
            ::close(serverSocket);
            return std::unexpected(make_sys_error(ErrorCode::SetSockOpt, "setsockopt"));
        }

        sockaddr_in serverAddress{};
        serverAddress.sin_family = AF_INET;
        serverAddress.sin_port = htons(port);
        serverAddress.sin_addr.s_addr = htonl(INADDR_ANY);

        if(::bind(serverSocket, (sockaddr*)&serverAddress, sizeof(serverAddress)) < 0){
            ::close(serverSocket);
            return std::unexpected(make_sys_error(ErrorCode::Bind, "bind"));
        }
        
        if(::listen(serverSocket,16)<0){
            ::close(serverSocket);
            return std::unexpected(make_sys_error(ErrorCode::Listen, "listen"));
        }

        return serverSocket;
    }

    std::expected<int,Error> accept_client(int listen_fd);{
        return accept(serverSocket, nullptr, nullptr);
    }

    std::expected<void,Error> read_exact(int fd, void* buf, size_t n){

    }

    std::expected<void,Error> write_all(int fd, const void* buf, size_t n){
        
    }
}

int main()
{   

    int port = 9000;

    

    listen(serverSocket,16);

    int client = accept_client();

    ssize_t r = read(client, buf, n);
    ssize_t w = write(client, buf, n);

    close(client);
    close(serverSocket);

    return 0;
}