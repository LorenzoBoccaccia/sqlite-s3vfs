import os
import tempfile
import uuid
from contextlib import closing, contextmanager


import apsw
import boto3
import sqlite3
import pytest

from concurrent import futures
from sqlite_ddbvfs import DDBVFS


PAGE_SIZES = [512, 1024, 2048, 4096, 8192, 16384, 32768, 65536]
BLOCK_SIZES = [4095, 4096, 4097]
JOURNAL_MODES = ['DELETE', 'TRUNCATE', 'PERSIST', 'MEMORY', 'OFF']


@pytest.fixture
def table():
    session = boto3.Session(
        aws_access_key_id='AKIAIDIDIDIDIDIDIDID',
        aws_secret_access_key='aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
        region_name='us-east-1',
    )
    dynamo = session.resource('dynamodb' , endpoint_url='http://localhost:8000')
    #dynamo = boto3.resource('dynamodb' )


    table = dynamo.create_table(
        TableName=f'ddbvfs-table-{str(uuid.uuid4())}',
        KeySchema=[
            {'AttributeName': 'key', 'KeyType': 'HASH'},
            {'AttributeName': 'range', 'KeyType': 'RANGE'},
        ]
        ,
        AttributeDefinitions=[
            {'AttributeName': 'key', 'AttributeType': 'S'},
            {'AttributeName': 'range', 'AttributeType': 'S'},
        ],
        BillingMode='PAY_PER_REQUEST',
    )
    table.wait_until_exists()
    yield table
    #drop the dynamodb table
    table.delete()

@pytest.fixture
def bucket():
    session = boto3.Session(
        aws_access_key_id='AKIAIDIDIDIDIDIDIDID',
        aws_secret_access_key='aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
        region_name='us-east-1',
    )
    s3 = session.resource('s3',
        endpoint_url='http://localhost:9000/'
    )

    bucket = s3.create_bucket(Bucket=f's3vfs-bucket-{str(uuid.uuid4())}')
    yield bucket
    bucket.objects.all().delete()
    bucket.delete()

@contextmanager
def transaction(cursor):
    cursor.execute('BEGIN;')
    try:
        yield cursor
    except:
        cursor.execute('ROLLBACK;')
        raise
    else:
        cursor.execute('COMMIT;')


def set_pragmas(cursor, page_size, journal_mode):
    sqls = [
        f'PRAGMA page_size = {page_size};',
        f'PRAGMA journal_mode = {journal_mode};',
    ]
    for sql in sqls:
        cursor.execute(sql)


def create_db(cursor):
    sqls = [
        'CREATE TABLE foo(x,y);',
        'INSERT INTO foo VALUES ' + ','.join('(1,2)' for _ in range(0, 100)) + ';',
    ] + [
        f'CREATE TABLE foo_{i}(x,y);' for i in range(0, 10)
    ]
    for sql in sqls:
        cursor.execute(sql)


def empty_db(cursor):
    sqls = [
        'DROP TABLE foo;'
    ] + [
        f'DROP TABLE foo_{i};' for i in range(0, 10)
    ]
    for sql in sqls:
        cursor.execute(sql)


@pytest.mark.parametrize(
    'page_size', PAGE_SIZES
)
@pytest.mark.parametrize(
    'block_size', BLOCK_SIZES
)
@pytest.mark.parametrize(
    'journal_mode', JOURNAL_MODES
)
def test_ddbvfs(bucket, table, page_size, block_size, journal_mode):
    ddbvfs = DDBVFS(table=table, block_size=block_size)

    # Create a database and query it
    with closing(apsw.Connection("a-test/cool.db", vfs=ddbvfs.name)) as db:
        set_pragmas(db.cursor(), page_size, journal_mode)

        with transaction(db.cursor()) as cursor:
            create_db(cursor)

        cursor.execute('SELECT * FROM foo;')
        assert cursor.fetchall() == [(1, 2)] * 100

        cursor.execute('PRAGMA integrity_check;')
        assert cursor.fetchall() == [('ok',)]

    # Query an existing database
    with \
            closing(apsw.Connection("a-test/cool.db", vfs=ddbvfs.name)) as db, \
            transaction(db.cursor()) as cursor:

        cursor = db.cursor()
        cursor.execute('SELECT * FROM foo;')

        assert cursor.fetchall() == [(1, 2)] * 100

        cursor.execute('PRAGMA integrity_check;')
        assert cursor.fetchall() == [('ok',)]

    # Serialize a database with serialize_fileobj, upload to S3, download it, and query it
    with \
            tempfile.NamedTemporaryFile() as fp_ddbvfs:

        target_obj = bucket.Object('target/cool.sqlite')
        target_obj.upload_fileobj(ddbvfs.serialize_fileobj(key_prefix='a-test/cool.db'))

        fp_ddbvfs.write(target_obj.get()['Body'].read())
        fp_ddbvfs.flush()

        with \
                closing(sqlite3.connect(fp_ddbvfs.name)) as db, \
                transaction(db.cursor()) as cursor:

            cursor.execute('SELECT * FROM foo;')
            assert cursor.fetchall() == [(1, 2)] * 100

            cursor.execute('PRAGMA integrity_check;')
            assert cursor.fetchall() == [('ok',)]

    # Serialize a database with serialize_iter and query it
    with \
            tempfile.NamedTemporaryFile() as fp_ddbvfs, \
            tempfile.NamedTemporaryFile() as fp_sqlite3:

        for chunk in ddbvfs.serialize_iter(pathname='a-test/cool.db'):
            # Empty chunks can be treated as EOF, so never output those
            assert bool(chunk)
            fp_ddbvfs.write(chunk)

        fp_ddbvfs.flush()

        with \
                closing(sqlite3.connect(fp_ddbvfs.name)) as db, \
                transaction(db.cursor()) as cursor:

            cursor.execute('SELECT * FROM foo;')
            assert cursor.fetchall() == [(1, 2)] * 100

            cursor.execute('PRAGMA integrity_check;')
            assert cursor.fetchall() == [('ok',)]

        # Serialized form should be the same length as one constructed without the VFS...
        with closing(sqlite3.connect(fp_sqlite3.name)) as db:
            set_pragmas(db.cursor(), page_size, journal_mode)

            with transaction(db.cursor()) as cursor:
                create_db(cursor)

            cursor.execute('PRAGMA integrity_check;')
            assert cursor.fetchall() == [('ok',)]

        assert os.path.getsize(fp_ddbvfs.name) == os.path.getsize(fp_sqlite3.name)

        # ...including after a VACUUM (which cannot be in a transaction)
        with closing(apsw.Connection("a-test/cool.db", vfs=ddbvfs.name)) as db:
            with transaction(db.cursor()) as cursor:
                empty_db(cursor)
            db.cursor().execute('VACUUM;')

            cursor.execute('PRAGMA integrity_check;')
            assert cursor.fetchall() == [('ok',)]

        fp_ddbvfs.truncate(0)
        fp_ddbvfs.seek(0)

        for chunk in ddbvfs.serialize_iter(pathname = 'a-test/cool.db'):
            assert bool(chunk)
            fp_ddbvfs.write(chunk)

        fp_ddbvfs.flush()

        with closing(sqlite3.connect(fp_sqlite3.name)) as db:
            with transaction(db.cursor()) as cursor:
                empty_db(cursor)

            db.cursor().execute('VACUUM;')

            cursor.execute('PRAGMA integrity_check;')
            assert cursor.fetchall() == [('ok',)]

        assert os.path.getsize(fp_ddbvfs.name) == os.path.getsize(fp_sqlite3.name)


@pytest.mark.parametrize(
    'page_size', PAGE_SIZES
)
@pytest.mark.parametrize(
    'block_size', BLOCK_SIZES
)
@pytest.mark.parametrize(
    'journal_mode', JOURNAL_MODES
)
def test_deserialize_iter(table, page_size, block_size, journal_mode):
    ddbvfs = DDBVFS(table=table, block_size=block_size)

    with tempfile.NamedTemporaryFile() as fp_sqlite3:
        with closing(sqlite3.connect(fp_sqlite3.name)) as db:
            set_pragmas(db.cursor(), page_size, journal_mode)

            with transaction(db.cursor()) as cursor:
                create_db(cursor)

        ddbvfs.deserialize_iter(key_prefix='another-test/cool.db', bytes_iter=fp_sqlite3)

    with \
            closing(apsw.Connection('another-test/cool.db', vfs=ddbvfs.name)) as db, \
            transaction(db.cursor()) as cursor:

        cursor = db.cursor()
        cursor.execute('SELECT * FROM foo;')

        assert cursor.fetchall() == [(1, 2)] * 100

        cursor.execute('PRAGMA integrity_check;')
        assert cursor.fetchall() == [('ok',)]


@pytest.mark.parametrize(
    'page_size', [65536]
)
@pytest.mark.parametrize(
    'block_size', [65536]
)
def test_byte_lock_page(table, page_size, block_size):
    ddbvfs = DDBVFS(table=table, block_size=block_size)
    empty = (bytes(4050),)

    with closing(apsw.Connection('another-test/cool.db', vfs=ddbvfs.name)) as db:
        db.cursor().execute(f'PRAGMA page_size = {page_size};')

        with transaction(db.cursor()) as cursor:
            cursor.execute('CREATE TABLE foo(content BLOB);')
            cursor.executemany('INSERT INTO foo VALUES (?);', (empty for _ in range(0, 300000)))

        cursor.execute('SELECT * FROM foo LIMIT 1;')
        assert cursor.fetchall() == [empty]

        cursor.execute('DELETE FROM foo;')
        cursor.execute('SELECT * FROM foo LIMIT 1;')
        assert cursor.fetchall() == [] 

        with transaction(db.cursor()) as cursor:
            cursor.executemany('INSERT INTO foo VALUES (?);', (empty for _ in range(0, 300000)))

        cursor.execute('SELECT * FROM foo LIMIT 1;')
        assert cursor.fetchall() == [empty]


@pytest.mark.parametrize(
    'page_size', [6144]
)
@pytest.mark.parametrize(
    'block_size', [6144]
)
def test_simple_read(table, page_size, block_size):
    ddbvfs = DDBVFS(table=table, block_size=block_size)
    canary = (uuid.uuid4().hex * 100,)

    with closing(apsw.Connection('another-test/cool.db', vfs=ddbvfs.name)) as db:
        db.cursor().execute(f'PRAGMA page_size = {page_size};')

        with transaction(db.cursor()) as cursor:
            cursor.execute('CREATE TABLE foo(content BLOB);')
            cursor.executemany('INSERT INTO foo VALUES (?);', (canary for _ in range(0, 10)))

        cursor.execute('SELECT * FROM foo LIMIT 1;')
        assert cursor.fetchall() == [canary]
        cursor.execute('SELECT count(*) FROM foo;')
        assert cursor.fetchall() == [(10,)]

        cursor.execute('DELETE FROM foo;')
        cursor.execute('SELECT * FROM foo LIMIT 1;')
        assert cursor.fetchall() == [] 

        with transaction(db.cursor()) as cursor:
            cursor.executemany('INSERT INTO foo VALUES (?);', (canary for _ in range(0, 100)))

        cursor.execute('SELECT * FROM foo LIMIT 1;')
        assert cursor.fetchall() == [canary]

def transaction_loop(table, page_size, block_size):
    canary = (uuid.uuid4().hex * 10,)
    ddbvfs = DDBVFS(table=table, block_size=block_size)

    with closing(apsw.Connection('another-test/cool.db', vfs=ddbvfs.name)) as db:
        db.cursor().execute(f'PRAGMA busy_timeout = {10000};')

        for r in range(10):
            with transaction(db.cursor()) as cursor:
                cursor.executemany('INSERT INTO foo VALUES (?);', (canary for _ in range(0, 10)))

            with closing(db.cursor()) as cursor:
                cursor.execute('SELECT * FROM foo where content = (?) LIMIT 1;', canary)
                assert cursor.fetchall() == [canary]
                cursor.execute('SELECT count(*) FROM foo where content = (?);', canary)
                assert cursor.fetchall()[0] == (10,)

            with closing(db.cursor()) as cursor:
                cursor.execute('DELETE FROM foo where content = (?);', canary)
                cursor.execute('SELECT * FROM foo where content = (?) LIMIT 1;', canary)
                assert cursor.fetchall() == []
                cursor.execute('SELECT count(*) FROM foo where content = (?);', canary)
                assert cursor.fetchall()[0] == (0,)

            with closing(db.cursor()) as cursor:
                cursor.executemany('INSERT INTO foo VALUES (?);', (canary for _ in range(0, 10)))
                cursor.execute('SELECT * FROM foo where content = (?) LIMIT 1;', canary)
                assert cursor.fetchall() == [canary]
                cursor.execute('SELECT count(*) FROM foo where content = (?);', canary)
                assert cursor.fetchall()[0] == (10,)

            with transaction(db.cursor()) as cursor:
                cursor.execute('DELETE FROM foo where content = (?);',canary)
                cursor.execute('SELECT * FROM foo where content = (?) LIMIT 1;', canary)
                assert cursor.fetchall() == []
                cursor.execute('SELECT count(*) FROM foo where content = (?);', canary)
                assert cursor.fetchall()[0] == (0,)
    
    return True

@pytest.mark.parametrize(
    'page_size', [6144]
)
@pytest.mark.parametrize(
    'block_size', [6144]
)

def test_threaded_pattern(table, page_size, block_size):
    ddbvfs = DDBVFS(table=table, block_size=block_size)
    future_result = []
    with closing(apsw.Connection('another-test/cool.db', vfs=ddbvfs.name)) as db:
        db.cursor().execute(f'PRAGMA page_size = {page_size};')
        db.cursor().execute('CREATE TABLE IF NOT EXISTS foo(content BLOB);')
    
    count_process = 2
    #execute transaction_loop in 10 threads
    with futures.ThreadPoolExecutor(max_workers=count_process) as executor:
        for _ in range(count_process):
            future_result.append(executor.submit(transaction_loop, table, page_size, block_size))
    #check all futures response when completed
    for future in future_result:
        assert future.result()






def test_set_temp_store_which_calls_xaccess(table):
    ddbvfs = DDBVFS(table=table)
    with closing(apsw.Connection('another-test/cool.db', vfs=ddbvfs.name)) as db:
        db.cursor().execute("pragma temp_store_directory = 'my-temp-store'")


@pytest.mark.parametrize(
    'page_size', [4096]
)
@pytest.mark.parametrize(
    'block_size', [4095, 4096, 4097]
)
@pytest.mark.parametrize(
    'journal_mode', [journal_mode for journal_mode in JOURNAL_MODES if journal_mode != 'OFF']
)
def test_rollback(table, page_size, block_size, journal_mode):
    ddbvfs = DDBVFS(table=table, block_size=block_size)

    with closing(apsw.Connection('another-test/cool.db', vfs=ddbvfs.name)) as db:
        db.cursor().execute(f'PRAGMA page_size = {page_size};')
        db.cursor().execute(f'PRAGMA journal_mode = {journal_mode};')
        db.cursor().execute('CREATE TABLE foo(content text);')

        try:
            with transaction(db.cursor()) as cursor:
                cursor.execute("INSERT INTO foo VALUES ('hello');");
                cursor.execute('SELECT * FROM foo;')
                assert cursor.fetchall() == [('hello',)]
                raise Exception()
        except:
            cursor.execute('SELECT * FROM foo;')
            assert cursor.fetchall() == []

        cursor.execute("INSERT INTO foo VALUES ('hello');");
        cursor.execute('SELECT * FROM foo;')
        assert cursor.fetchall() == [('hello',)]

        try:
            with transaction(db.cursor()) as cursor:
                cursor.execute("UPDATE foo SET content='goodbye'");
                cursor.execute('SELECT * FROM foo;')
                assert cursor.fetchall() == [('goodbye',)]
                raise Exception()
        except:
            cursor.execute('SELECT * FROM foo;')
            assert cursor.fetchall() == [('hello',)]

