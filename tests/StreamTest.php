<?php

namespace Bhamner\RedisStreams\Tests;

use Bhamner\RedisStreams\Entry;
use Bhamner\RedisStreams\Stream;
use Bhamner\RedisStreams\Tests\Support\FakeStreamsClient;
use PHPUnit\Framework\TestCase;

class StreamTest extends TestCase
{
    public function test_create_appends_an_entry_like_eloquent_create(): void
    {
        $redis = new FakeStreamsClient;
        $redis->responses['add'] = '10-0';

        $entry = OrderStream::create([
            'type' => 'order.placed',
            'order_id' => '123',
        ], $redis);

        $this->assertSame('10-0', $entry->id);
        $this->assertSame('orders', $entry->stream);
        $this->assertSame('123', $entry->get('order_id'));
        $this->assertSame('orders', $redis->calls[0][1]['stream']);
        $this->assertSame(['type' => 'order.placed', 'order_id' => '123'], $redis->calls[0][1]['fields']);
    }

    public function test_query_limits_and_reads_newest_first(): void
    {
        $redis = new FakeStreamsClient;
        $redis->responses['range'] = [
            ['id' => '2-0', 'fields' => ['type' => 'order.placed']],
        ];

        $entries = OrderStream::query($redis)->after('1-0')->limit(5)->latest()->get();

        $this->assertCount(1, $entries);
        $this->assertInstanceOf(Entry::class, $entries->first());
        $this->assertSame('1-0', $redis->calls[0][1]['start']);
        $this->assertSame('+', $redis->calls[0][1]['end']);
        $this->assertSame(5, $redis->calls[0][1]['count']);
        $this->assertTrue($redis->calls[0][1]['reverse']);
    }

    public function test_find_looks_up_one_id(): void
    {
        $redis = new FakeStreamsClient;
        $redis->responses['range'] = [
            ['id' => '4-1', 'fields' => ['order_id' => '9']],
        ];

        $entry = OrderStream::find('4-1', $redis);

        $this->assertSame('9', $entry?->get('order_id'));
        $this->assertSame('4-1', $redis->calls[0][1]['start']);
        $this->assertSame('4-1', $redis->calls[0][1]['end']);
    }
}

class OrderStream extends Stream
{
    protected string $name = 'orders';
}
