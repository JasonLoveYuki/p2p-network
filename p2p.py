
import sys
import os
import signal
from threading import Thread, Lock
from time import sleep
from socket import *


class Peer():
    peer_id = -1
    first_successor = -1
    second_successor = -1
    first_predecessor = -1
    second_predecessor = -1
    first_successor_state = False
    second_successor_state = False
    first_successor_reply_count = 0
    second_successor_reply_count = 0
    ping_interval = -1
    state = 1
    files = []
    lock = Lock()

    def __init__(self, peer_id, first_successor, second_successor, ping_interval):
        Peer.peer_id = peer_id
        Peer.first_successor = first_successor
        Peer.second_successor = second_successor
        Peer.ping_interval = ping_interval
        Peer.first_successor_state = False
        Peer.second_successor_state = False
        Peer.first_successor_reply_count = 0
        Peer.second_successor_reply_count = 0

    @classmethod
    def update_successors(cls, first_successor, second_successor, first_successor_state, second_successor_state,
                          first_successor_reply_count, second_successor_reply_count):
        """
        update successor information
        """
        cls.lock.acquire()
        cls.first_successor, cls.second_successor,\
        cls.first_successor_state, cls.second_successor_state,\
        cls.first_successor_reply_count, cls.second_successor_reply_count =\
            first_successor, second_successor,\
            first_successor_state, second_successor_state,\
            first_successor_reply_count, second_successor_reply_count
        cls.lock.release()

    @staticmethod
    def hash(filename):
        return int(filename) % 256

    @staticmethod
    def valid_filename(filename):
        """
        boolean function checking whether the input filename is legal
        """
        return (len(filename) == 4) and filename.isdigit()

    @classmethod
    def correct_file_location(cls, hash_value):
        """
        boolean function checking whether peer should handle input hash value
        """
        if cls.first_predecessor < hash_value <= cls.peer_id \
                or hash_value > cls.first_predecessor > cls.peer_id \
                or cls.first_predecessor > cls.peer_id >= hash_value:
            return True
        return False

    @classmethod
    def correct_peer_location(cls, joining_peer):
        """
        boolean function checking whether the joining peer should be peer's new successor
        """
        if cls.peer_id < joining_peer < cls.first_successor \
                or joining_peer > cls.peer_id > cls.first_successor \
                or joining_peer < cls.first_successor < cls.peer_id:
            return True
        return False

    @classmethod
    def print_peer_info(cls):
        print(f'My new first successor is Peer {cls.first_successor}')
        print(f'My new second successor is Peer {cls.second_successor}')

    @staticmethod
    def send_file(path, address):
        """
        send files to address
        """
        with open(path, 'rb') as pdf_file:
            while True:
                data_read = pdf_file.read(800)
                # meet eof
                if not data_read:
                    break
                if len(data_read) < 800:
                    # last block
                    message = f'filE {path[:-4]} '.encode() + data_read
                    Peer.send_tcp_message(message, address)
                else:
                    # normal block
                    message = f'file {path[:-4]} '.encode() + data_read
                    Peer.send_tcp_message(message, address)


    @staticmethod
    def receive_file(received_message):
        """
        receive data packet and write to output file
        """
        received_message_split = received_message.split()
        file_number = received_message_split[1].decode()
        header_length = len(received_message_split[0]) + len(received_message_split[1]) + 2
        filename = f'received_{file_number}.pdf'
        actual_data = received_message[header_length:]
        with open(filename, "ab") as new_file:
            new_file.write(actual_data)

    @staticmethod
    def send_udp_message(message, address):
        client_socket = socket(AF_INET, SOCK_DGRAM)
        client_socket.sendto(message, address)
        client_socket.close()

    @staticmethod
    def send_tcp_message(message, address):
        client_socket = socket(AF_INET, SOCK_STREAM)
        client_socket.connect(address)
        client_socket.send(message)
        client_socket.close()

    def launch_init_peer(self):
        """
        start all threads
        """
        udp_server = self.UDPServer('UDPServer')
        udp_server.start()
        tcp_server = self.TCPServer('TCPServer')
        tcp_server.start()
        sleep(5)
        ping_client = self.PingClient('PingClient')
        ping_client.start()
        heartbeat_client = self.HeartBeat('HeartBeat')
        heartbeat_client.start()
        input_info = self.InputInfo('InputInfo')
        input_info.start()
        failure_monitor = self.FailureMonitor('FailureMonitor')
        failure_monitor.start()

    def launch_join_peer(self, known_peer):
        """
        5 steps
        1. start tcp_server
        2. send peer_joining message to known_peer
        3. wait for joining details
        4. update successors
        5. start all threads
        """
        welcome_socket = socket(AF_INET, SOCK_STREAM)
        welcome_socket.bind(('localhost', Peer.peer_id + 12000))
        welcome_socket.listen(1)
        message = f'message join_request from {Peer.peer_id}'.encode()
        Peer.send_tcp_message(message, ('localhost', known_peer + 12000))
        connection_socket, client_address = welcome_socket.accept()
        received_message = connection_socket.recv(1024).decode().split()
        # deal with received_message and update successors
        Peer.first_successor, Peer.second_successor = int(received_message[2]), int(received_message[3])
        connection_socket.close()
        welcome_socket.close()
        print('Join request has been accepted')
        print(f'My first successor is Peer {Peer.first_successor}')
        print(f'My second successor is Peer {Peer.second_successor}')
        self.launch_init_peer()


    class PingClient(Thread):
        """
        send ping requests to successors
        """
        def __init__(self, name):
            super().__init__()
            self.name = name

        def run(self):
            while Peer.state:
                print(f'Ping requests were sent to Peers {Peer.first_successor} and {Peer.second_successor}')
                message = f'message ping_request from {Peer.peer_id}'.encode()
                Peer.send_udp_message(message, ('localhost', Peer.first_successor + 12000))
                Peer.send_udp_message(message, ('localhost', Peer.second_successor + 12000))
                sleep(Peer.ping_interval)

    class HeartBeat(Thread):
        """
        send heartbeat requests to successors
        """
        def __init__(self, name):
            super().__init__()
            self.name = name

        def run(self):
            while Peer.state:
                first_successor_message = f'message heartbeat_request 1 {Peer.peer_id}'.encode()
                second_successor_message = f'message heartbeat_request 2 {Peer.peer_id}'.encode()
                Peer.send_udp_message(first_successor_message, ('localhost', Peer.first_successor + 12000))
                Peer.send_udp_message(second_successor_message, ('localhost', Peer.second_successor + 12000))
                sleep(2)

    class UDPServer(Thread):
        """
        4 parts:
        1. receive ping_request, send ping_responses back
        2. receive ping_response
        3. receive heartbeat_request, send ping_responses back and update predecessors
        4. receive heartbeat_response, increase reply-count
        """
        def __init__(self, name):
            super().__init__()
            self.name = name

        def run(self):
            server_socket = socket(AF_INET, SOCK_DGRAM)
            server_socket.bind(('localhost', Peer.peer_id +12000))
            while Peer.state:
                message, client_address = server_socket.recvfrom(2048)
                decoded_message = message.decode().split()
                if decoded_message[0] == 'message':
                    # receive ping_request, send ping_responses back
                    # message format: f'message ping_request from {Peer.peer_id}'
                    if decoded_message[1] == 'ping_request':
                        client_peer_id = int(decoded_message[3])
                        print(f'Ping request message received from Peer {client_peer_id}')
                        response_message = f'message ping_response from {Peer.peer_id}'.encode()
                        Peer.send_udp_message(response_message, ('localhost', client_peer_id + 12000))
                    # receive ping_response
                    # message format: f'message ping_response from {Peer.peer_id}'
                    elif decoded_message[1] == 'ping_response':
                        client_peer_id = int(decoded_message[3])
                        print(f'Ping response received from Peer {client_peer_id}')
                    # receive heartbeat_request, send ping_responses back and update predecessors
                    # message format: f'message heartbeat_request 1/2 {Peer.peer_id}'
                    elif decoded_message[1] == 'heartbeat_request':
                        client_peer_id = int(decoded_message[3])
                        response_message = f'message heartbeat_response from {Peer.peer_id}'.encode()
                        Peer.send_udp_message(response_message, ('localhost', client_peer_id + 12000))
                        if decoded_message[2] == '1':
                            Peer.lock.acquire()
                            Peer.first_predecessor = client_peer_id
                            Peer.lock.release()
                        elif decoded_message[2] == '2':
                            Peer.lock.acquire()
                            Peer.second_predecessor = client_peer_id
                            Peer.lock.release()
                    # receive heartbeat_response, increase reply-count
                    # message format: f'message heartbeat_response from {Peer.peer_id}'
                    elif decoded_message[1] == 'heartbeat_response':
                        client_peer_id = int(decoded_message[3])
                        if client_peer_id == Peer.first_successor:
                            Peer.lock.acquire()
                            Peer.first_successor_state = True
                            Peer.first_successor_reply_count += 1
                            Peer.lock.release()
                        elif client_peer_id == Peer.second_successor:
                            Peer.lock.acquire()
                            Peer.second_successor_state = True
                            Peer.second_successor_reply_count += 1
                            Peer.lock.release()
            server_socket.close()

    class TCPServer(Thread):
        """
        9 parts:
        1. receive join_request, and deliver request/(send update_second_successor request & send response)
        2. receive update_second_successor request, and update successors
        3. receive quit message, and update successors
        4. receive successor_query request due to failure, and send successor_query response
        5. receive successor_query response, and update successors
        6. receive store_file request, and deliver request/(send response & store files)
        7. receive request_file request, and deliver request/(send response & send file)
        8. receive request_file response
        9. receive data file
        """
        def __init__(self, name):
            super().__init__()
            self.name = name
        def run(self):
            welcome_socket = socket(AF_INET, SOCK_STREAM)
            welcome_socket.bind(('localhost', Peer.peer_id + 12000))
            welcome_socket.listen(1)
            while Peer.state:
                connection_socket, client_address = welcome_socket.accept()
                received_message = connection_socket.recv(1024)
                # receive join_request, and deliver request/send response
                if received_message[:7] == 'message'.encode():
                    decoded_message = received_message.decode().split()
                    # join request
                    # message format: f'message join_request from {Peer.peer_id}'
                    if decoded_message[1] == 'join_request':
                        joining_peer = int(decoded_message[3])
                        # case 1: the new peer should be new successor
                        if Peer.correct_peer_location(joining_peer):
                            previous_successors = Peer.first_successor, Peer.second_successor
                            Peer.update_successors(joining_peer, Peer.first_successor, False, Peer.first_successor_state, 5, Peer.first_successor_reply_count)
                            print(f'Peer {joining_peer} Join request received')
                            Peer.print_peer_info()
                            # send details about joining_peer to its first predecessor
                            message_to_predecessor = f'message update_second_successor {joining_peer}'.encode()
                            Peer.send_tcp_message(message_to_predecessor, ('localhost', Peer.first_predecessor + 12000))
                            # contact joining_peer over TCP and send network join details
                            message_to_joining_peer = f'message join_response {previous_successors[0]} {previous_successors[1]}'.encode()
                            Peer.send_tcp_message(message_to_joining_peer, ('localhost', joining_peer + 12000))
                        # case2: others
                        else:
                            Peer.send_tcp_message(received_message, ('localhost', Peer.first_successor + 12000))
                            print(f'Peer {joining_peer} Join request forwarded to my successor')
                    # update_second_successor request: due to join request
                    # message format: f'message update_second_successor {joining_peer}'
                    elif decoded_message[1] == 'update_second_successor':
                        new_second_successor = int(decoded_message[2])
                        Peer.lock.acquire()
                        Peer.second_successor = new_second_successor
                        Peer.second_successor_state = False
                        Peer.second_successor_reply_count = 5
                        Peer.lock.release()
                        print('Successor Change request received')
                        Peer.print_peer_info()
                    # quit request
                    # message format: f'message quit {Peer.peer_id} {Peer.first_successor} {Peer.second_successor}'
                    elif decoded_message[1] == 'quit':
                        quit_peer = int(decoded_message[2])
                        quit_peer_first_successor = int(decoded_message[3])
                        quit_peer_second_successor = int(decoded_message[4])
                        if Peer.first_successor == quit_peer:
                            Peer.update_successors(Peer.second_successor, quit_peer_second_successor, Peer.second_successor_state, False, Peer.second_successor_reply_count, 5)
                            print(f'Peer {quit_peer} will depart from the network')
                            Peer.print_peer_info()
                        elif Peer.second_successor == quit_peer:
                            Peer.lock.acquire()
                            Peer.second_successor = quit_peer_first_successor
                            Peer.second_successor_state = False
                            Peer.second_successor_reply_count = 5
                            Peer.lock.release()
                            print(f'Peer {quit_peer} will depart from the network')
                            Peer.print_peer_info()
                    # successor_query request: due to failure monitor
                    elif decoded_message[1] == 'successor_query':
                        # message sent by second predecessor
                        # message format: f'message successor_query 1 {Peer.peer_id}'
                        if decoded_message[2] == '1':
                            target_peer = int(decoded_message[3])
                            message = f'message query_response 1 {Peer.first_successor}'.encode()
                            Peer.send_tcp_message(message, ('localhost', target_peer + 12000))
                        # message sent by first predecessor
                        # message format: f'message successor_query 2 {Peer.peer_id} {Peer.second_successor}
                        elif decoded_message[2] == '2':
                            target_peer = int(decoded_message[3])
                            # message contains who is actually dead
                            dead_peer = int(decoded_message[4])
                            # decide if peer has already modified its successor
                            if Peer.first_successor == dead_peer:
                                message = f'message query_response 2 {Peer.second_successor}'.encode()
                            else:
                                message = f'message query_response 2 {Peer.first_successor}'.encode()
                            Peer.send_tcp_message(message, ('localhost', target_peer + 12000))
                    # query_response: to update successors
                    elif decoded_message[1] == 'query_response':
                        if decoded_message[2] == '1':
                            new_second_successor = int(decoded_message[3])
                            Peer.update_successors(Peer.second_successor, new_second_successor, Peer.second_successor_state, False, Peer.second_successor_reply_count, 5)
                            Peer.print_peer_info()
                        elif decoded_message[2] == '2':
                            new_second_successor = int(decoded_message[3])
                            Peer.lock.acquire()
                            Peer.second_successor = new_second_successor
                            Peer.second_successor_state = False
                            Peer.second_successor_reply_count = 5
                            Peer.lock.release()
                            Peer.print_peer_info()
                    # store_file request
                    # message format: f'message store_file {filename} {hash_value}'
                    elif decoded_message[1] == 'store_file':
                        filename = decoded_message[2]
                        file_path = f'{filename}.pdf'
                        hash_value = int(decoded_message[3])
                        if Peer.correct_file_location(hash_value):
                            # if file exists in directory then accept the request
                            if os.path.isfile(file_path):
                                print(f'Store {filename} request accepted')
                                Peer.files.append(filename)
                            else:
                                print(f'File does not exist in directory')
                        else:
                            print(f'Store {filename} request forwarded to my successor')
                            Peer.send_tcp_message(received_message, ('localhost', Peer.first_successor + 12000))
                    # request_file request
                    # message format: f'message request_file {filename} {hash_value} {Peer.peer_id}'
                    elif decoded_message[1] == 'request_file':
                        filename = decoded_message[2]
                        hash_value = int(decoded_message[3])
                        request_peer = int(decoded_message[4])
                        if Peer.correct_file_location(hash_value):
                            if filename in Peer.files:
                                print(f'File {filename} is stored here')
                                print(f'Sending file {filename} to Peer 2')
                                # fetch and split pdf file
                                file_path = f'{filename}.pdf'
                                # send response message
                                message = f'message file_response {Peer.peer_id} {filename}'.encode()
                                Peer.send_tcp_message(message, ('localhost', request_peer + 12000))
                                # send split files to destination
                                Peer.send_file(file_path, ('localhost', request_peer + 12000))
                                print('The file has been sent')
                            else:
                                print(f'File {filename} is not stored')
                        else:
                            print(f'Request for File {filename} has been received, but the file is not stored here')
                            Peer.send_tcp_message(received_message, ('localhost', Peer.first_successor + 12000))
                    # file_response
                    # message format f'message file_response {Peer.peer_id} {filename}'.encode()
                    elif decoded_message[1] == 'file_response':
                        sending_peer = int(decoded_message[2])
                        filename = decoded_message[3]
                        print(f'Peer {sending_peer} had File {filename}')
                        print(f'Receiving File {filename} from Peer {sending_peer}')
                # data transfer
                # receive normal packet
                elif received_message[:4] == 'file'.encode():
                    Peer.receive_file(received_message)
                # receive last packet
                elif received_message[:4] == 'filE'.encode():
                    Peer.receive_file(received_message)
                    print(f'File {filename} received')
                # close the connection socket
                connection_socket.close()
            welcome_socket.close()

    class InputInfo(Thread):
        """
        3 parts:
        1. send quit message to predeccessor
        2. store file/send store message to successor
        3. send file requests
        """
        def __init__(self, name):
            super().__init__()
            self.name = name
        def run(self):
            while Peer.state:
                user_input = input()
                split_user_input = user_input.split()
                # send quit message to successor
                if user_input == 'Quit':
                    # sends details of its successors to predecessors
                    message = f'message quit {Peer.peer_id} {Peer.first_successor} {Peer.second_successor}'.encode()
                    Peer.send_tcp_message(message, ('localhost', Peer.first_predecessor + 12000))
                    Peer.send_tcp_message(message, ('localhost', Peer.second_predecessor + 12000))
                    Peer.state = 0
                    os._exit(0)
                # store file/send store message to successor
                elif split_user_input[0] == 'Store':
                    filename = split_user_input[1]
                    if Peer.valid_filename(filename):
                        hash_value = Peer.hash(filename)
                        # peer itself is the correct location to store the file
                        if Peer.correct_file_location(hash_value):
                            print(f'Store {filename} request accepted')
                            Peer.files.append(filename)
                        # hand the request to successor
                        else:
                            print(f'Store {filename} request forwarded to my successor')
                            message = f'message store_file {filename} {hash_value}'.encode()
                            Peer.send_tcp_message(message, ('localhost', Peer.first_successor + 12000))
                    # invalid filename
                    else:
                        print('invalid filename')
                # send file requests
                elif split_user_input[0] == 'Request':
                    filename = split_user_input[1]
                    if Peer.valid_filename(filename):
                        hash_value = Peer.hash(filename)
                        if filename in Peer.files:
                            print(f'Peer {Peer.peer_id} already have File {filename}')
                        else:
                            print(f'File request for {filename} has been sent to my successor')
                            message = f'message request_file {filename} {hash_value} {Peer.peer_id}'.encode()
                            Peer.send_tcp_message(message, ('localhost', Peer.first_successor + 12000))
                    else:
                        print('invalid filename')
                else:
                    print('invalid input')

    class FailureMonitor(Thread):
        """
        loop every 10s
        1. monitor state of successors and send query if some successor is dead
        2. clear the reply_count
        """
        def __init__(self, name):
            super().__init__()
            self.name = name

        def run(self):
            while Peer.state:
                # while loop 10s
                sleep(10)
                # first successor is dead
                if Peer.first_successor_reply_count <= 3:
                    if Peer.first_successor_state:
                        print(f'Peer {Peer.first_successor} is no longer alive')
                        message = f'message successor_query 1 {Peer.peer_id}'.encode()
                        Peer.send_tcp_message(message, ('localhost', Peer.second_successor + 12000))
                # second successor is dead
                if Peer.second_successor_reply_count <= 3:
                    if Peer.second_successor_state:
                        print(f'Peer {Peer.second_successor} is no longer alive')
                        message = f'message successor_query 2 {Peer.peer_id} {Peer.second_successor}'.encode()
                        Peer.send_tcp_message(message, ('localhost', Peer.first_successor + 12000))
                Peer.lock.acquire()
                Peer.first_successor_state = False
                Peer.second_successor_state = False
                Peer.first_successor_reply_count = 0
                Peer.second_successor_reply_count = 0
                Peer.lock.release()


def init_peer(peer_id, first_successor, second_successor, ping_interval):
    peer = Peer(peer_id, first_successor, second_successor, ping_interval)
    peer.launch_init_peer()


def join_peer(peer_id, known_peer, ping_interval):
    peer = Peer(peer_id, -1, -1, ping_interval)
    peer.launch_join_peer(known_peer)


def exit_program(signal, frame):
    print("program terminated")
    os._exit(0)


def main(argv):
    signal.signal(signal.SIGINT, exit_program)
    signal.signal(signal.SIGTERM, exit_program)
    operation = argv[1]
    if operation == 'init':
        peer_id = int(argv[2])
        first_successor = int(argv[3])
        second_successor = int(argv[4])
        ping_interval = int(argv[5])
        init_peer(peer_id, first_successor, second_successor, ping_interval)
    elif operation == 'join':
        peer_id = int(argv[2])
        known_peer = int(argv[3])
        ping_interval = int(argv[4])
        join_peer(peer_id, known_peer, ping_interval)
    else:
        print('not supported')


if __name__ == "__main__":
    main(sys.argv)

