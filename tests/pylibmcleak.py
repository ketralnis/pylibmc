#!/usr/bin/env python

import pylibmc
from pylibmc import Client
import random
import sys

import _pylibmc
print 'Importing %r from %r' % (_pylibmc, _pylibmc.__file__)

client = Client(['localhost:11211'])
client.behaviors.update({'cas': 1})

client.delete_multi(map(str,[1,2,3]), key_prefix="test_")
client.add_multi({'1':2, '2':3, '3':4}, key_prefix="test_")
client.incr_multi(map(str, [1,2,3]), key_prefix="test_")
client.get_multi(map(str, [1,2,3]), key_prefix="test_")

for y in xrange(1000000):
    if y % 5 == 0:
        print y

    data = dict((str(random.random()), x+1)
                for x in xrange(1000))

    client.set_multi(data)
    assert client.get_multi(data.keys()) == data

    # now with a prefix
    prefix = 'prefix'
    client.set_multi(data, key_prefix = prefix)
    assert client.get_multi(data.keys(), key_prefix = prefix) == data

    bigdata = dict((key, str(value)*1000)
                   for (key, value)
                   in data.iteritems())
    client.set_multi(data, key_prefix = prefix)
    assert client.get_multi(data.keys(), key_prefix = prefix) == data

    assert client.add_multi(data, key_prefix='incrs') == []
    client.incr_multi(data.keys(), key_prefix='incrs')
    assert (client.get_multi(data.keys(), key_prefix='incrs')
            == dict((key, value+1)
                    for (key, value)
                    in data.items()))
    client.delete_multi(data.keys(), key_prefix='incrs')

    # with compression
    client.set_multi(data, key_prefix = prefix, min_compress_len = 1, compress_level=9)
    assert client.get_multi(data.keys(), key_prefix = prefix) == data

    for key, value in data.iteritems():
        client.set(key, value)
        assert client.get(key) == value

        client.set(key, bigdata[key])
        assert client.get(key) == bigdata[key]

        client.set(key, bigdata[key], min_compress_len = 1000)
        assert client.get(key) == bigdata[key]

        incr_amt = random.randint(1,10)
        client.set('incrs%s' % (key,), value)
        client.incr('incrs%s' % (key,), incr_amt)
        assert client.get('incrs%s' % (key,)) == value+incr_amt
        assert client.delete('incrs%s' % (key,))

    client.delete('foo')
    assert client.gets('foo') == (None,None)
    client.set('foo', 'bar')
    foostr, cas = client.gets('foo')
    assert client.cas('foo', 'quux', cas+1) == False
    assert client.cas('foo', 'baz', cas) == True
    assert client.get('foo') == 'baz'

    if False:
        client.delete('foo')
        #assert client.cas_or_gets('foo', 'baz', 1) == (None, None)
        client.set('foo', 'bar')
        foostr, cas = client.gets('foo')
        assert foostr == 'bar'
        assert client.cas_or_gets('foo', 'baz', cas)
        client.set('foo', 'bar')
        foostr, cas = client.cas_or_gets('foo', 'baz', cas)
        assert foostr == 'bar'
