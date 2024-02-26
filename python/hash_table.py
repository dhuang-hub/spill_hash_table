import shutil
import hashlib
import json
import threading
from pathlib import Path
from typing import Union


class PersistentHashTable:

    def __init__(self, table_dir: Union[str, Path], log: bool = True, bucket_mag: int = 3):
        """
        Initialize the PersistentHashTable object

        Set up the table directory if the table directory does not exist, or
        will reference a pre-existing table directory as-is.

        Params:
            table_dir: Path-like to directory for the hash table
            log: Flag for loggin set actions (useful for recovery)
            bucket_mag: Magnitude of number of buckets, calculation of 16^bucket_mag.
                Increasing the number of buckets can be useful for RAM constrained envs.
                Default value of 3 yields 4096 buckets.
        """
        self.table_dir = Path(table_dir)
        self.table_dir.mkdir(mode=0o777, parents=True, exist_ok=True)
        self.bucket_mag = 3
        self.bucket_lock = {}
        self.bucket_lock_lock = threading.Lock()
        self.log_lock = threading.Lock()

        if log:
            self.log_file = self.table_dir / 'log.txt'
            if not self.log_file.exists():
                self.log_file.touch(mode=0o777)
        else:
            self.log_file = None

    @classmethod
    def from_log(cls, log_path: Union[str, Path], table_dir: Union[str, Path], bucket_mag: int = 3):
        """
        Initialize the PersistentHashTable object from a log-file

        Sets up the table directory by 'replaying' the actions recorded in the log-file.
        This is useful for reloading a corrupted file system from a valid log-file.

        Params:
            log_path: Path-like to log-file for the hash table
            table_dir: Path-like to directory for the hash table
            log: Flag for loggin set actions (useful for recovery)
            bucket_mag: Magnitude of number of buckets, calculation of 16^bucket_mag.
                Increasing the number of buckets can be useful for RAM constrained envs.
                Default value of 3 yields 4096 buckets.
        """
        log_path = Path(log_path)
        if not log_path.exists():
            raise ValueError('Log path not found!')
        log_contents = log_path.read_text()

        # wipe table dir if it exists
        table_dir = Path(table_dir)
        if table_dir.exists():
            shutil.rmtree(table_dir)
        table_dir.mkdir(mode=0o775, parents=True)

        # replay log
        ht = cls(table_dir, True, bucket_mag)
        for line in log_contents.strip().split():
            k, v = line.split(',')
            ht[int(k)] = int(v)

        return ht
                
    def _hash(self, key: int) -> str:
        """
        Deterministic hashing for bucketing

        Default of 3 hexadecimal values for 4096 (2^12) buckets
        """
        return hashlib.sha256(str(key).encode()).hexdigest()[:self.bucket_mag]

    def _get_bucket_lock(self, bucket_path: Path) -> threading.Lock:
        """
        Get the lock for any given bucket
        """
        with self.bucket_lock_lock:
            if bucket_path not in self.bucket_lock:
                self.bucket_lock[bucket_path] = threading.Lock()
            return self.bucket_lock[bucket_path]

    def _get_bucket_path(self, key: int) -> Path:
        """
        Get bucket path from hashing of key

        Makes bucket path if it doesn't exist
        """
        bucket_path = self.table_dir / f'{self._hash(key)}.json'
        if not bucket_path.exists():
            json.dump({}, bucket_path.open('w'))
        return bucket_path

    def _log(self, key: int, val: int):
        """
        Log the setting of key and value
        """
        with self.log_file.open('a') as f:
            f.write(f'{key},{val}\n')

    def __getitem__(self, key: int) -> int:
        """
        Get hash table value by key
        """
        bucket_path = self._get_bucket_path(key)
        bucket_lock = self._get_bucket_lock(bucket_path)
        with bucket_lock:
            bucket = json.load(bucket_path.open('r'))
            try:
                return bucket[f'{key}']
            except Exception as e:
                raise e

    def __setitem__(self, key: int, val: int) -> None:
        """
        Set hash table value by key
        """
        bucket_path = self._get_bucket_path(key)
        bucket_lock = self._get_bucket_lock(bucket_path)
        with bucket_lock:
            bucket = json.load(bucket_path.open('r'))
            try:
                bucket[key] = val
                json.dump(bucket, bucket_path.open('w'))
            except Exception as e:
                raise e
        with self.log_lock:
            self._log(key, val)
