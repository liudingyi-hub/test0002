import os
import sys
import fcntl
import signal
import colors

ok   = colors.bold(colors.green("OK"))
fail = colors.bold(colors.red("FAILED"))

class Daemon(object):
    '''
    Create, destroy daemon process
    '''
    lock_fp = None
    def __init__(self, app_name, pid_file, stdout = "/dev/null",
                                           stderr = "/dev/null",
                                           stdin  = "/dev/null"):
        self.app_name = app_name
        self.pid_file = pid_file
        self.stdout = stdout
        self.stderr = stderr
        self.stdin = stdin

    def single_instance(self):
        '''
        Check if the program has only one instance now
        '''
        pid_file = self.pid_file

        # create the pid file when it doesn't exist
        if not os.path.isfile(pid_file):
            open(pid_file, "wb").close()

        lock_fp = open(pid_file, 'ab', 0)
        try:
            fcntl.flock(lock_fp, fcntl.LOCK_EX | fcntl.LOCK_NB)
            # clear pid_file's content so we could write new pid number into it
            # later
            lock_fp.truncate(0)
        except IOError:
            return False
        self.__class__.lock_fp = lock_fp
        return True

    def daemonize(self):
        '''
        This forks the current process into a daemon.  The stdin, stdout, and
        stderr arguments are file names that will be opened and be used to
        replace the standard file descriptors in sys.stdin, sys.stdout, and
        sys.stderr.  '''
        # Do first fork.
        try:
            pid = os.fork()
            if pid > 0:
                sys.exit(0) # Exit first parent.
        except OSError as e:
            sys.stderr.write("fork #1 failed: (%d) %s" % (e.errno, e.strerror))
            sys.exit(1)

        os.umask(0)
        os.setsid()

        # Do second fork.
        try:
            pid = os.fork()
            if pid > 0:
                sys.exit(0) # Exit second parent.
        except OSError as e:
            sys.stderr.write("fork #2 failed: (%d) %s" % (e.errno, e.strerror))
            sys.exit(1)

        si = open(self.stdin, 'rb')
        so = open(self.stdout, 'ab+')
        se = open(self.stderr, 'ab+', 0)

        os.dup2(si.fileno(), sys.stdin.fileno())
        os.dup2(so.fileno(), sys.stdout.fileno())
        os.dup2(se.fileno(), sys.stderr.fileno())

    def start(self):
        if not self.single_instance():
            sys.stdout.write("Another %s instance seems is running ... \
                                         [%s]\n" % (self.app_name, fail))
            sys.exit()

        sys.stdout.write("Start %s ... [%s]\n" % (self.app_name, ok))
        self.daemonize()

        lock_fp  = self.__class__.lock_fp
        lock_fp.write(bytes("%d" % os.getpid(), 'UTF-8'))

    def stop(self):
        try:
            pid = int(open(self.pid_file, 'rb').read())
            os.killpg(os.getpgid(pid), signal.SIGINT)
        except Exception as e:
            sys.stdout.write("Stop %s ... [%s]\n" % (self.app_name, fail))
            sys.exit(-1)
        sys.stdout.write("Stop %s ... [%s]\n" % (self.app_name, ok))

    def status(self):
        if not self.single_instance():
            status = "running"
        else:
            status = "stopped"
        sys.stdout.write( "%s program is %s\n" % (self.app_name, status))
