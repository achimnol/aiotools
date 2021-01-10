Now the primary target is Python 3.9
Add a new module `fork` to support PID file descriptors in Linux 5.4+ and a POSIX-compatible fallback to asynchornously fork the Python process without signal/PID races.
