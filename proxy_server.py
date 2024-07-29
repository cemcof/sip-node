import argparse
import asyncio
import socket
import ssl

class TCPProxyServer:
    def __init__(self, local_addr, remote_addr, use_ssl, skip_cert_verification):
        self.local_host, self.local_port = local_addr
        self.remote_host, self.remote_port = remote_addr
        self.use_ssl = use_ssl
        self.skip_cert_verification = skip_cert_verification
        self.ssl_context_out = self.create_ssl_context(ssl.Purpose.CLIENT_AUTH) if use_ssl else None
        self.ssl_context_in = self.create_ssl_context(ssl.Purpose.SERVER_AUTH) if use_ssl else None

    def create_ssl_context(self, purpose=ssl.Purpose.SERVER_AUTH):
        ssl_context = ssl.create_default_context(purpose)
        if self.skip_cert_verification:
            ssl_context.check_hostname = False
            ssl_context.verify_mode = ssl.CERT_NONE
        return ssl_context

    async def handle_client(self, local_reader, local_writer):
        try:
            remote_reader, remote_writer = await asyncio.open_connection(
                self.remote_host, self.remote_port, ssl=self.ssl_context_out)
            client_name = local_writer._transport.get_extra_info('peername')
            target = remote_writer._transport.get_extra_info('peername')

            print(f"New client {client_name} -> {target}, ssl_version={self.ssl_context_out.protocol}")
            async def forward(reader, writer):
                try:
                    while True:
                        data = await reader.read(4096)
                        if not data:
                            break
                        # print("WRITING DATA", data)
                        writer.write(data)
                        await writer.drain()
                except Exception as e:
                    print(f'Connection error: {e}, {client_name}')
                finally:
                    writer.close()

            await asyncio.gather(
                forward(local_reader, remote_writer),
                forward(remote_reader, local_writer)
            )

            print(f"Client finished: {client_name}")

        except Exception as e:
            print(f'Failed to connect to remote server: {e}')
        finally:
            local_writer.close()

    async def start(self):
        server = await asyncio.start_server(
            self.handle_client, self.local_host, self.local_port, ssl=self.ssl_context_in)
        print(f'Serving on {self.local_host}:{self.local_port}, ssl_version={self.ssl_context_in.protocol}')
        async with server:
            await server.serve_forever()

def parse_args():
    parser = argparse.ArgumentParser(description='Simple TCP proxy server with SSL support.')
    parser.add_argument('-l', '--local', type=str, required=True, help='Local address in the format host:port')
    parser.add_argument('-r', '--remote', type=str, required=True, help='Remote address in the format host:port')
    parser.add_argument('-s', '--ssl', action='store_true', help='Use SSL for connections to the remote host')
    parser.add_argument('-k', '--skip-cert', action='store_true', help='Skip SSL certificate verification')

    args = parser.parse_args()

    # Parse the local and remote addresses
    local_host, local_port = args.local.split(':')
    remote_host, remote_port = args.remote.split(':')

    return (local_host, int(local_port)), (remote_host, int(remote_port)), args.ssl, args.skip_cert

def main():
    local_addr, remote_addr, use_ssl, skip_cert_verification = parse_args()

    # Resolve hostnames to IP addresses
    local_host_ip = socket.gethostbyname(local_addr[0])
    remote_host_ip = socket.gethostbyname(remote_addr[0])

    proxy_server = TCPProxyServer((local_host_ip, local_addr[1]), (remote_host_ip, remote_addr[1]), use_ssl, skip_cert_verification)
    
    try:
        asyncio.run(proxy_server.start())
    except KeyboardInterrupt:
        print("Server stopped")

if __name__ == "__main__":
    main()
