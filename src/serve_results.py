import http.server
import socketserver
import os
import argparse

PORT = 8080

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dir", default="/data", help="Directory to serve")

    cli_args = parser.parse_args()

    # Ensure directory exists
    if not os.path.exists(cli_args.dir):
        print(f"Directory {cli_args.dir} does not exist. Creating it.")
        os.makedirs(cli_args.dir)

    class Handler(http.server.SimpleHTTPRequestHandler):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, directory=cli_args.dir, **kwargs)

    socketserver.TCPServer.allow_reuse_address = True

    with socketserver.TCPServer(("", PORT), Handler) as httpd:
        print(f"Serving {cli_args.dir} at port {PORT}")
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            pass
        finally:
            httpd.server_close()
