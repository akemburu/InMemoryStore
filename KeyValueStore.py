import threading
from datetime import datetime, timedelta

from InMemoryStore import InMemoryStore
from Utils import ReadWriteLock

class KeyValueStore(InMemoryStore):

	def __init__(self):
		self._store = dict()
		self.lock = ReadWriteLock()

	def get(self, key):

		try:
			self.lock.get_read_lock()
			val, expiry_time = self._store[key]

			# if the time now is equal to the expiration time then
			# remove then this value is not valid.
			if (expiry_time is not None) and (datetime.now() > expiry_time):
				# start a thread to remove the key from the store in the background
				# not using deamon because if the program quits then the thread should
				# not be running anymore.
				t = threading.Thread(target=self.delete, args=(key,))
				t.start()
				raise KeyError()

		except:
			raise KeyError("Key does not exist in the store.")
		finally:
			self.lock.release_read_lock()

		return val

	def put(self, key, value, duration=None):
		start_time = datetime.now()
		expiry_time = None
		if not isinstance(key, (str, int, float, bool, frozenset)):
			raise ValueError("The store only allows keys of the following types: strings, ints, floats, bools, or frozensets.")

		self.lock.get_write_lock()

		if duration is not None:
			# deduct the amount of time that has elapsed since the start of the call to put from the duration
			duration_time = timedelta(milliseconds=duration)
			expiry_time = datetime.now() + (duration_time - (datetime.now() - start_time))
			if expiry_time > datetime.now():
				self._store[key] = (value, expiry_time)
		else:
			self._store[key] = (value, None)

		self.lock.release_write_lock()
		return value

	def delete(self, key):
		self.lock.get_write_lock()

		if key in self._store:
			del self._store[key]
		else:
			raise KeyError("Key does not exist in the store.")

		self.lock.release_write_lock()