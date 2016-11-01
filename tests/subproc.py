import subprocess, shlex
import time
import threading, queue

def subprocess_exit_check():
    cmd_line = "java -cp jlib-0.1.1.jar com.github.hackerwin7.jlib.utils.executors.ProcessBuilderId"
    args = shlex.split(cmd_line)
    process = subprocess.Popen(args=args, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
    time.sleep(1)
    if process.poll() == None:
        print("running")
        process.kill()
        print(str(process.stderr.read()))
        print(str(process.returncode)) # return code == none is normal exit without error
    else:
        print("terminated")
        if process.stderr.read() == None:
            print("read None")
        else:
            print("read no None")
        print(str(process.stderr.read()))
        print(str(process.returncode))
    print(process.pid)

def subprocess_non_blocking():
    cmd_line = "java -cp jlib-0.1.1.jar com.github.hackerwin7.jlib.utils.executors.ProcessBuilderId"
    args = shlex.split(cmd_line)
    process = subprocess.Popen(args=args, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
    q = queue.Queue()
    thread = threading.Thread(target=enqueue_out, args=(process.stderr, q)) # threading.Thread(target=enqueue_out(process.stderr, q)) is error format for threading
    thread.daemon = True
    thread.start()
    time.sleep(1)

    try:
        line = q.get_nowait()
    except queue.Empty:
        print("no contents in queue")
    else:
        print("get line : %s %s" % (line, cmd_line))

def enqueue_out(out, queue):
    for line in iter(out.readline, b''):
        queue.put(line)
    out.close()

# main process
subprocess_non_blocking()
