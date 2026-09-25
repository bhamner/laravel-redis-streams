<?php

namespace Bhamner\RedisStreams\Tests;

use Bhamner\RedisStreams\Stream;
use Bhamner\RedisStreams\Tests\Support\FakeStreamsClient;
use PHPUnit\Framework\TestCase;

class GroupTest extends TestCase
{
    public function test_read_ensures_the_group_and_acknowledges(): void
    {
        $redis = new FakeStreamsClient;
        $redis->responses['readGroup'] = [
            ['id' => '8-0', 'fields' => ['type' => 'order.placed']],
        ];

        $entry = Stream::on('orders', $redis)
            ->group('billing')
            ->consumer('worker-1')
            ->pop(block: 2000);

        $entry?->ack();

        $this->assertSame('createGroup', $redis->calls[0][0]);
        $this->assertSame('billing', $redis->calls[0][1]['group']);
        $this->assertSame('$', $redis->calls[0][1]['start']);
        $this->assertSame('readGroup', $redis->calls[1][0]);
        $this->assertSame('worker-1', $redis->calls[1][1]['consumer']);
        $this->assertSame('>', $redis->calls[1][1]['from']);
        $this->assertSame(2000, $redis->calls[1][1]['blockMilliseconds']);
        $this->assertSame(['8-0'], $redis->calls[2][1]['ids']);
    }

    public function test_claim_idle_returns_entries_that_can_be_acknowledged(): void
    {
        $redis = new FakeStreamsClient;
        $redis->responses['autoClaim'] = [
            'next' => '9-0',
            'entries' => [
                ['id' => '3-0', 'fields' => ['type' => 'order.placed']],
            ],
        ];

        $entries = Stream::on('orders', $redis)
            ->group('billing')
            ->consumer('worker-2')
            ->claimIdle(60000);

        $this->assertSame('3-0', $entries->first()?->id);
        $this->assertSame(60000, $redis->calls[0][1]['minIdleMilliseconds']);
        $this->assertSame('worker-2', $redis->calls[0][1]['consumer']);
    }
}
