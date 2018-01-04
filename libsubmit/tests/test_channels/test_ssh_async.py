import libsubmit
import time
from libsubmit.channels.ssh.ssh import SshChannel as Ssh

def connect_and_list(hostname, username):
    conn = Ssh(hostname, username=username)
    ec, out, err = conn.execute_wait("echo $HOSTNAME")
    conn.close()
    return out


def connect_and_slow_exec(conn, dur=10):
    handle = conn.execute_no_wait("sleep {0}; uname -a".format(dur))
    return handle


def test_cooley_async ():
    ''' Test ssh async exec on Midway
    '''
    url = 'swift.rcc.uchicago.edu'
    uname = 'yadunand'
    conn = Ssh(url, username=uname)

    h1 = connect_and_slow_exec(conn)
    h2 = connect_and_slow_exec(conn, dur=15)
    h3 = connect_and_slow_exec(conn)

    handles = [h1, h2, h3]

    print("Launched three calls:", handles)
    while True:
        print("Timer trig")
        print("Statuses :", [conn.poll_handle(h) for h in handles])

        if all([conn.poll_handle(h) for h in handles]):
            for h in handles:
                print("Result of :", h)
                print(conn.result(h))
            break

        time.sleep(2)
    return


if __name__ == "__main__":
    libsubmit.set_stream_logger()
    test_cooley_async()
