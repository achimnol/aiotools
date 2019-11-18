import zmq


if __name__ == '__main__':
    ctx = zmq.Context()
    s = ctx.socket(zmq.PUSH)
    s.connect('tcp://127.0.0.1:5033')

    for _ in range(100):
        s.send(b'hello world')

    s.close()
