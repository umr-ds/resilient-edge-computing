#! /usr/bin/env python3

from socket import socket, AF_UNIX, SOCK_STREAM

from rec.dtn.messages import JobsQuery, MsgType


DTN_ID = "dtn://test_1/"
DTN_SOCKET = "/tmp/rec_test_1.sock"


def main() -> None:
    with socket(AF_UNIX, SOCK_STREAM) as s:
        s.connect(DTN_SOCKET)
        print("Connected")

        message = JobsQuery(Type=MsgType.JOBS_QUERY, Sender=DTN_ID, Submitter=DTN_ID)
        message_bytes = message.serialize()
        message_length = len(message_bytes)
        print(f"Message length: {message_length}")
        message_length_bytes = message_length.to_bytes(
            length=4, byteorder="big", signed=False
        )

        s.sendall(message_length_bytes)
        print("Sent message length")
        s.sendall(message_bytes)
        print("Sent message")


if __name__ == "__main__":
    main()
