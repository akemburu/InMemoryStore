import threading

class ReadWriteLock:

    def __init__(self):
        self._allow_read = threading.Condition(threading.Lock(  ))
        self._num_readers = 0

    def get_read_lock(self):
        self._allow_read.acquire(  )
        try:
            self._num_readers += 1
        finally:
            self._allow_read.release(  )

    def release_read_lock(self):
        self._allow_read.acquire(  )
        try:
            self._num_readers -= 1
            if self._num_readers == 0:
                self._allow_read.notifyAll(  )
        finally:
            self._allow_read.release(  )

    def get_write_lock(self):
        self._allow_read.acquire(  )
        while self._num_readers > 0:
            self._allow_read.wait(  )

    def release_write_lock(self):
        self._allow_read.release(  )