from socket import *
import yaml

hostname = 'localhost'
port = 1234
timeout = 5

def call(params):
    s = socket(AF_INET, SOCK_STREAM)
    s.settimeout(timeout)
    s.connect((hostname, port))
    s.send(yaml.dump(params))
    s.send("---\n")

    buffer = ""
    while True:
        data = s.recv(1024)
        if '---' not in data:
            buffer += data
        else:
            buffer += data[:data.index('---')]
            try:
                response = yaml.load(buffer)
                s.close()
                return response
            except yaml.parser.ParserError:
                raise


def main():
    print "HEARTBEAT EXAMPLE: "
    try:    
        response = call({"action":"heartbeat"})
        print "   The WFE is " + response['reply']
    except:
        print "   Something is terribly wrong !"

    print call({"action":"kill_job", "jobguid":"123"})


if __name__ == '__main__':
	main()
