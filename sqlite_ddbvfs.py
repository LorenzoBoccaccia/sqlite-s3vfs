import uuid
import apsw
from apsw import BusyError
from boto3.dynamodb.conditions import Key
from boto3.dynamodb.conditions import Attr
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Attr
 

class DDBVFS(apsw.VFS):  

    def __init__(self, table, block_size=4060):
        self.name = f'ddbvfs-{str(uuid.uuid4())}'
        self._table = table
        self._block_size = block_size
        super().__init__(name=self.name, base='')

    def xAccess(self, pathname, flags):
        #check that ACCESS with range pathname exists
        result = self._table.query(KeyConditionExpression=Key('key').eq('ACCESS') & Key('range').eq(pathname),
                                   ConsistentRead = True)
        return (
            flags == apsw.mapping_access["SQLITE_ACCESS_EXISTS"]
            and ( result['Count'] > 0 )
        ) or (
            flags != apsw.mapping_access["SQLITE_ACCESS_EXISTS"]
        )

    def xFullPathname(self, filename):
        return filename

    def xDelete(self, pathname, syncdir):
        lek = None
        while True:
            if lek:
                response = self._table.query( KeyConditionExpression=Key('key').eq(f'BL_{pathname}'), 
                                        ConsistentRead = True,
                                         ExclusiveStartKey = lek )
            else: 
                response = self._table.query(KeyConditionExpression=Key('key').eq(f'BL_{pathname}'),
                                        ConsistentRead = True)
            for obj in response['Items']:
                #delete
                self._table.delete_item(Key={'key': obj['key'], 'range': obj['range']})
            if 'LastEvaluatedKey' not in response:
                break
            lek = response['LastEvaluatedKey']
        self._table.delete_item(Key={'key': 'ACCESS', 'range': pathname})

    def xOpen(self, pathname, flags):
        return DDBVFSFile(pathname, flags, self._table, self._block_size)

    def serialize_iter(self, pathname):
        lek = None
        while True:
            if lek:
                response = self._table.query( KeyConditionExpression=Key('key').eq(f'BL_{pathname}'), 
                                        ConsistentRead = True,
                                         ExclusiveStartKey = lek )
            else: 
                response = self._table.query(KeyConditionExpression=Key('key').eq(f'BL_{pathname}'),
                                        ConsistentRead = True)
            for obj in response['Items']:
                yield obj['data'].value
            if 'LastEvaluatedKey' not in response:
                break
            lek = response['LastEvaluatedKey']


    def serialize_fileobj(self, key_prefix):
        chunk = b''
        offset = 0
        it = iter(self.serialize_iter(key_prefix))

        def up_to_iter(num):
            nonlocal chunk, offset

            while num:
                if offset == len(chunk):
                    try:
                        chunk = next(it)
                    except StopIteration:
                        break
                    else:
                        offset = 0
                to_yield = min(num, len(chunk) - offset)
                offset = offset + to_yield
                num -= to_yield
                yield chunk[offset - to_yield:offset]

        class FileLikeObj:
            def read(self, n=-1):
                n = \
                    n if n != -1 else \
                    4294967294 * 65536  # max size of SQLite file
                return b''.join(up_to_iter(n))

        return FileLikeObj()

    def deserialize_iter(self, key_prefix, bytes_iter):
        chunk = b''
        offset = 0
        it = iter(bytes_iter)

        def up_to_iter(num):
            nonlocal chunk, offset

            while num:
                if offset == len(chunk):
                    try:
                        chunk = next(it)
                    except StopIteration:
                        break
                    else:
                        offset = 0
                to_yield = min(num, len(chunk) - offset)
                offset = offset + to_yield
                num -= to_yield
                yield chunk[offset - to_yield:offset]

        def block_bytes_iter():
            while True:
                block = b''.join(up_to_iter(self._block_size))
                if not block:
                    break
                yield block
        total = 0
        for block, block_bytes in enumerate(block_bytes_iter()):
            size =len(block_bytes)
            total += size
            self._table.put_item(
                Item={
                    'key': f'BL_{key_prefix}',
                    'range': f'{block:010d}',
                    'data': block_bytes,
                    'size': size
                },
                ReturnValues='ALL_OLD'
            )
        #set totals
        self._table.put_item(
            Item={
                'key': f'FSIZE',
                'range': key_prefix,
                'size': total
            }

        )
        
class DDBVFSFile: 

    def __init__(self, name, flags, table, block_size):
        if name == None:
            name = f'temp-{str(uuid.uuid4())}'
        self._key_prefix = \
            self._key_prefix = name.filename() if isinstance(name, apsw.URIFilename) else \
            name
        #add random uuid for name if name is none 

        self._table = table
        self._block_size = block_size 
        self._attained_log_level = 0
        self._client_id = uuid.uuid4()
        self._table.put_item(
            Item={
                'key': f'ACCESS',
                'range': self._key_prefix
            },
        )

    def _blocks(self, offset, amount):
        while amount > 0:
            block = offset // self._block_size  # which block to get
            start = offset % self._block_size   # place in block to start
            consume = min(self._block_size - start, amount)
            yield (block, start, consume)
            amount -= consume
            offset += consume

    def _block_get_bytes(self, block):
        result =  self._table.get_item(
            Key={
                'key': f'BL_{self._key_prefix}',
                'range': f'{block:010d}'
            },
            AttributesToGet=[
                'data'
            ],
            ConsistentRead=True
        )
        ret = b''
        if 'Item' in result:
            ret = result['Item']['data'].value

        return ret
            
    def _block_put_bytes(self, block, bytes):
        size =len(bytes)
        result = self._table.put_item(
            Item={
                'key': f'BL_{self._key_prefix}',
                'range': f'{block:010d}',
                'data': bytes,
                'size': size
            },
            ReturnValues='ALL_OLD'
        )
        assert(result['ResponseMetadata']['HTTPStatusCode'] == 200)
        dsize = size
        if 'Attributes' in result:
            dsize = size-int(result['Attributes']['size'])
        return dsize
    
    def _block_delete(self, block):
        result = self._table.delete_item(
            Key={
                'key': f'BL_{self._key_prefix}',
                'range': f'{block:010d}'
            },
            ReturnValues='ALL_OLD'
        )
        assert(result['ResponseMetadata']['HTTPStatusCode'] == 200)
        dsize = -int(result['Attributes']['size'])
        return dsize

    def _update_total_size(self, dsize):
        result = self._table.update_item(
            Key={
                'key': f'FSIZE',
                'range': self._key_prefix
            },
            UpdateExpression='ADD size :dsize',
            ExpressionAttributeValues={
                ':dsize': dsize
            },
            ReturnValues='UPDATED_NEW'
        )
        assert(result['ResponseMetadata']['HTTPStatusCode'] == 200)

    def _get_total_size(self):
        result = self._table.get_item(
            Key={
                'key': f'FSIZE',
                'range': self._key_prefix
            },
            AttributesToGet=[
                'size'
            ],
            ConsistentRead=True
        )
        assert(result['ResponseMetadata']['HTTPStatusCode'] == 200)
        if 'Item' in result:
            return int(result['Item']['size'])
        else:
            return 0

    def xRead(self, amount, offset):
        try:
            def _read():
                for block, start, consume in self._blocks(offset, amount):
                    block_bytes = self._block_get_bytes(block)
                    yield block_bytes[start:start+consume]

            return b"".join(_read())
        except Exception as ex:
            print("ERROR READING ", ex )
            raise ex
    def xSectorSize(self):
        return 0

    def xFileControl(self, *args):
        return False

    SQLITE_LOCK_NONE      =    0    
    SQLITE_LOCK_SHARED    =    1    
    SQLITE_LOCK_RESERVED  =    2    
    SQLITE_LOCK_PENDING   =    3    
    SQLITE_LOCK_EXCLUSIVE =    4  

    def _get_lock_state(self):
        response = self._table.get_item(
            Key={
                'key': f'LK',
                'range': self._key_prefix
            },
            AttributesToGet=[
                'level',
                'owner'
            ],
            ConsistentRead=True
        )
        assert(response['ResponseMetadata']['HTTPStatusCode'] == 200)
        if 'Item' in response and 'level' in response['Item']:
            return int(response['Item']['level'])
        if 'Item' in response and 'count' in response['Item'] and int(response['Item']['count'])>0:
            return 1
        return 0
    
    def xCheckReservedLock(self):
        return self._get_lock_state() >= self.SQLITE_LOCK_RESERVED
 
    
    def xLock(self, level):           
        assert level != self.SQLITE_LOCK_NONE
        assert level != self.SQLITE_LOCK_PENDING
        if level == self._attained_log_level:
            return
        
        if level == self.SQLITE_LOCK_SHARED:
            try:
                response = self._table.update_item(
                    Key={
                        'key': f'LK',
                        'range': self._key_prefix
                    }, 
                    UpdateExpression='SET #cnt = if_not_exists(#cnt, :initial) + :increment',
                    ConditionExpression=Attr('level').not_exists() | Attr('level').eq(self.SQLITE_LOCK_RESERVED),
                    ExpressionAttributeValues={
                        ':initial': 0,
                        ':increment': 1
                    },
                    ExpressionAttributeNames={
                        '#cnt': 'count'
                    }
                )
                self._attained_log_level = self.SQLITE_LOCK_SHARED
                return
            except ClientError as e:
                if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
                    #someone else has reserved the lock
                    raise BusyError()
                else:
                    raise e
        else:
            #start with a pending lock if we don't already have one
            if self._attained_log_level != self.SQLITE_LOCK_PENDING:
                try:
                    response = self._table.update_item(
                            Key={
                                'key': f'LK',
                                'range': self._key_prefix,
                            },
                            UpdateExpression = 'SET #lvl = :level, #own = :owner ',
                            ConditionExpression= (Attr('owner').not_exists() | Attr('owner').eq(f'{self._client_id}')),
                            ExpressionAttributeValues={
                                ':level': self.SQLITE_LOCK_PENDING,
                                ':owner': f'{self._client_id}'
                            },
                            ExpressionAttributeNames={
                                '#lvl': 'level',
                                '#own': 'owner'
                            }
                        )
                    self._attained_log_level = self.SQLITE_LOCK_PENDING
                except ClientError as e:
                    if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
                        #someone has a strong lock already
                        raise BusyError()
                    else:
                        raise e               
            #we're pending, try to upgrade to a hard lock
            try:
                if level == self.SQLITE_LOCK_RESERVED:
                    condition=Attr('owner').eq(f'{self._client_id}')
                else:
                    #only go to exclusive lock after readers have drained (count = 1)
                    #if here we always at least own a pending lock
                    condition=Attr('owner').eq(f'{self._client_id}') & Attr('count').eq(1)
                response =self._table.update_item(
                    Key={
                        'key': f'LK',
                        'range': self._key_prefix,
                    },
                    UpdateExpression = 'SET #lvl = :level, #own = :owner ',
                    ConditionExpression= condition,
                    ExpressionAttributeValues={
                        ':level': level,
                        ':owner': f'{self._client_id}'
                    },
                    ExpressionAttributeNames={
                        '#lvl': 'level',
                        '#own': 'owner'
                    }
                )
                self._attained_log_level = level
            except ClientError as e:
                if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
                    #someone has a strong lock already
                    raise BusyError()
                else:
                    raise e    
                
    def xUnlock(self, level):
        if level == self._attained_log_level:
            return
        if level >= self.SQLITE_LOCK_RESERVED:
            #downgrading lock from exclusive, but keeping exclusive
            response = self._table.put_item(
                    Item={
                        'key': f'LK',
                        'range': self._key_prefix,
                        'level' : level,
                        'owner' : f'{self._client_id}',
                        'count': 1
                    },
                    ConditionExpression=Attr('owner').eq(f'{self._client_id}')
                )
            self._attained_log_level = level
        if level == self.SQLITE_LOCK_SHARED:
            #downgrading lock from exclusive, releasing exclusive
            response = self._table.update_item(
                    Key={
                        'key': f'LK',
                        'range': self._key_prefix,
                    },
                    UpdateExpression='REMOVE #lvl, #own',
                    ConditionExpression=Attr('owner').eq(f'{self._client_id}'),
                    ExpressionAttributeNames={
                        '#lvl': 'level',
                        '#own': 'owner'
                    }
                )
            self._attained_log_level = level
        if level == self.SQLITE_LOCK_NONE:
            if self._attained_log_level > self.SQLITE_LOCK_SHARED:
                try:
                    response = self._table.update_item(
                            Key={
                                'key': f'LK',
                                'range': self._key_prefix,
                            },
                            UpdateExpression='SET #cnt = #cnt - :increment REMOVE #lvl, #own',
                            ConditionExpression=Attr('owner').eq(f'{self._client_id}'),
                            ExpressionAttributeValues={
                                ':increment': 1
                            },
                            ExpressionAttributeNames={
                                '#cnt': 'count',
                                '#lvl': 'level',
                                '#own': 'owner'
                            }
                        )
                except Exception as e:
                    raise e
            else:
                increment = 1 if self._attained_log_level > 0 else 0
                response = self._table.update_item(
                        Key={
                            'key': f'LK',
                            'range': self._key_prefix
                        }, 
                        UpdateExpression='SET #cnt = #cnt - :increment',
                        ExpressionAttributeValues={
                            ':increment': increment
                        },
                        ExpressionAttributeNames={
                            '#cnt': 'count'
                        }
                    )
                self._attained_log_level = level
    def xClose(self):
        pass

    def xFileSize(self):
        return self._get_total_size()
        total = 0
        lek = None
        while True:
            if lek:
                response = self._table.query( KeyConditionExpression=Key('key').eq(f'BL_{self._key_prefix}'), 
                                         ExclusiveStartKey = lek,
                                         ConsistentRead = True )
            else: 
                response = self._table.query(KeyConditionExpression=Key('key').eq(f'BL_{self._key_prefix}'),
                                             ConsistentRead = True)
            for obj in response['Items']:
                total += int(obj['size'])

            if 'LastEvaluatedKey' not in response:
                break
            lek = response['LastEvaluatedKey']

        stored_total = self._get_total_size()
        return total


    def xSync(self, flags):
        return True

    def xTruncate(self, newsize):
        total = 0
        dsize = 0
        lek = None
        while True:
            if lek:
                response = self._table.query( KeyConditionExpression=Key('key').eq(f'BL_{self._key_prefix}'), 
                                         ConsistentRead = True,
                                         ExclusiveStartKey = lek )
            else: 
                response = self._table.query(KeyConditionExpression=Key('key').eq(f'BL_{self._key_prefix}'),
                                             ConsistentRead = True)
            
            for obj in response['Items']:
                total += int(obj['size'])
                to_keep = max(int(obj['size']) - total + newsize, 0)

                if to_keep == 0:
                    dsize += self._block_delete(int(obj['range']))

                elif to_keep < int(obj['size']):
                    bytes = self._block_get_bytes(int(obj['range']))
                    dsize += self._block_put_bytes(int(obj['range']), bytes[:to_keep])

            if 'LastEvaluatedKey' not in response:
                break
            lek = response['LastEvaluatedKey']
        self._update_total_size(dsize)
        return True

    def xWrite(self, data, offset):
        dsize = 0
        try:
            lock_page_offset = 1073741824
            page_size = len(data)

            if offset == lock_page_offset + page_size:
                # Ensure the previous blocks have enough bytes for size calculations and serialization.
                # SQLite seems to always write pages sequentially, except that it skips the byte-lock
                # page, so we only check previous blocks if we know we're just after the byte-lock
                # page.

                data_first_block = offset // self._block_size
                lock_page_block = lock_page_offset // self._block_size
                for block in range(data_first_block - 1, lock_page_block - 1, -1):
                    original_block_bytes = self._block_get_bytes(block)
                    if len(original_block_bytes) == self._block_size:
                        break
                    dsize += self._block_put_bytes(block, original_block_bytes + bytes(
                        self._block_size - len(original_block_bytes)
                    ))

            data_offset = 0
            for block, start, write in self._blocks(offset, len(data)):

                data_to_write = data[data_offset:data_offset+write]

                if start != 0 or len(data_to_write) != self._block_size:
                    original_block_bytes = self._block_get_bytes(block)
                    original_block_bytes = original_block_bytes + bytes(max(start - len(original_block_bytes), 0))

                    data_to_write = \
                        original_block_bytes[0:start] + \
                        data_to_write + \
                        original_block_bytes[start+write:]

                data_offset += write
                dsize += self._block_put_bytes(block, data_to_write)
            self._update_total_size(dsize)
        except Exception as ex:
            print("ERROR WRITING ", ex)
            raise ex