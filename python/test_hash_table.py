import shutil
import threading
from random import randint
from pathlib import Path

from hash_table import PersistentHashTable


def randint32():
    return randint(-1 << 31, (1 << 31) - 1)


def test_setup():
    """
    Test setup of hash-table including log file
    """
    table_dir = Path('_test_setup')
    shutil.rmtree(table_dir, ignore_errors=True)

    # creation of new hash table
    ht = PersistentHashTable(table_dir)
    assert table_dir.exists()
    assert table_dir.is_dir()
    assert ht.log_file == table_dir / 'log.txt'
    assert ht.log_file.exists()
    del ht

    # instantiate existing hash table
    ht = PersistentHashTable(table_dir)
    assert table_dir.exists()
    assert table_dir.is_dir()
    assert (table_dir / 'log.txt').exists()
    assert ht.log_file == table_dir / 'log.txt'
    assert ht.log_file.exists()

    shutil.rmtree(table_dir)


def test_correct(n=5000):
    """
    Test correctness of hash-table set and get methods
    """
    table_dir = Path('_test_runtime')
    shutil.rmtree(table_dir, ignore_errors=True)

    ht = PersistentHashTable(table_dir)
    keys = [randint32() for _ in range(n)]
    vals = [randint32() for _ in range(n)]

    # set values
    for k, v in zip(keys, vals):
        ht[k] = v

    # assert correct storage
    for k, v in zip(keys, vals):
        assert ht[k] == v

    # assert correct logging
    assert len(ht.log_file.read_text().strip().split()) == n

    shutil.rmtree(table_dir)


def test_set_error(n=10):
    """
    Test the handling of errors during set method
    """
    table_dir = Path('_test_error')
    shutil.rmtree(table_dir, ignore_errors=True)

    ht = PersistentHashTable(table_dir)
    keys = [randint32() for _ in range(n)]
    vals = [randint32() for _ in range(n)]

    # forced error in setting
    try:
        ht[set()] = 'error'
    except TypeError:
        pass

    for k, v in zip(keys, vals):
        ht[k] = v

    # forced error in setting
    try:
        ht[set()] = 'error'
    except TypeError:
        pass

    # assert correct storage
    for k, v in zip(keys, vals):
        assert ht[k] == v

    # assert correct logging
    assert len(ht.log_file.read_text().strip().split()) == n

    shutil.rmtree(table_dir)


def test_reload(n=1000):
    """
    Test the reload of the hash table from a log file
    """
    orig_dir = Path('_test_reload_a')
    new_dir = Path('_test_reload_b')
    shutil.rmtree(orig_dir, ignore_errors=True)
    shutil.rmtree(new_dir, ignore_errors=True)

    orig_ht = PersistentHashTable(orig_dir)
    keys = [randint32() for _ in range(n)]
    vals = [randint32() for _ in range(n)]
    for k, v in zip(keys, vals):
        orig_ht[k] = v

    # assert correct hash table key-values
    new_ht = PersistentHashTable.from_log(orig_ht.log_file, new_dir)
    for k, v in zip(keys, vals):
        assert new_ht[k] == v

    # assert correct logging
    assert len(new_ht.log_file.read_text().strip().split()) == n

    shutil.rmtree(orig_dir)
    shutil.rmtree(new_dir)


def test_thread(n=300):
    """
    Test the thread safety of the hash table
    """
    table_dir = Path('_test_thread')
    shutil.rmtree(table_dir, ignore_errors=True)

    ht = PersistentHashTable(table_dir, bucket_mag=1)
    keys = [randint32() for _ in range(n)]
    vals = [randint32() for _ in range(n)]

    def set_key_val(ht, key, val):
        ht[key] = val
        assert ht[key] == val

    threads = []
    for i in range(n):
        t = threading.Thread(target=set_key_val, args=(ht, i, i))
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

    # assert final result correctness
    for i in range(i):
        assert ht[i] == i

    # assert correct logging
    assert len(ht.log_file.read_text().strip().split()) == n

    shutil.rmtree(table_dir, ignore_errors=True)


if __name__ == '__main__':
    test_setup()
    test_correct()
    test_set_error()
    test_reload()
    test_thread()
