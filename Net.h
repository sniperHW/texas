#pragma once
#include <mutex>
#include <thread>
#include <winsock2.h>
#include <WinBase.h>
#include <Winerror.h>
#include <stdio.h>
#include <WS2tcpip.h>

namespace net {

	class NetClient : public std::enable_shared_from_this<NetClient> {
	public:
		typedef std::shared_ptr<NetClient> Ptr;

		static NetClient::Ptr New(const std::string &ip,uint32_t port) {
			struct sockaddr_in addr;
			addr.sin_family = AF_INET;
			addr.sin_port = htons((u_short)port);
			addr.sin_addr.s_addr = inet_addr(ip.c_str());
			auto sock = NetClient::connect(&addr);
			if (sock == INVALID_SOCKET) {
				return nullptr;
			}
			else {
				auto ptr = Ptr(new NetClient(addr, sock));
				ptr->start();
				return ptr;
			}
		}







	private:

		NetClient(struct sockaddr_in addr, SOCKET socket):serverAddr(addr),socket(socket){}

		void start() {

		}

		void loop() {

		}


		static SOCKET connect(struct sockaddr_in *addr) {
			SOCKET sock = ::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
			int ret = ::connect(sock, (struct sockaddr *)addr, sizeof(*addr));
			if (ret >= 0){
				return sock;
			}
			else {
				::closesocket(sock);
				return INVALID_SOCKET;
			}
		}

		struct sockaddr_in serverAddr;
		SOCKET             socket;

	};
}