<?php

namespace Bhamner\RedisStreams\Tests;

use Bhamner\RedisStreams\Consumer;
use Bhamner\RedisStreams\Entry;
use Bhamner\RedisStreams\StreamEvent;
use Bhamner\RedisStreams\Tests\Support\FakeStreamsClient;
use PHPUnit\Framework\TestCase;
use RuntimeException;

class ConsumerTest extends TestCase
{
    public function test_dispatch_stores_the_event_type_and_payload(): void
    {
        $redis = new FakeStreamsClient;
        $redis->responses['add'] = '5-0';

        $entry = OrderPlaced::dispatchFor($redis, orderId: '55', total: 4900);

        $this->assertSame('5-0', $entry->id);
        $this->assertSame('orders', $redis->calls[0][1]['stream']);
        $this->assertSame(OrderPlaced::class, $redis->calls[0][1]['fields']['type']);
        $this->assertSame('{"orderId":"55","total":4900}', $redis->calls[0][1]['fields']['payload']);
    }

    public function test_worker_acknowledges_a_handled_entry(): void
    {
        $redis = new FakeStreamsClient;
        $redis->responses['readGroup'] = [
            ['id' => '1-0', 'fields' => ['type' => OrderPlaced::class, 'payload' => '{"orderId":"1","total":10}']],
        ];

        $worker = new BillingConsumer($redis);
        $processed = $worker->once();

        $this->assertSame(1, $processed);
        $this->assertSame(['1'], $worker->handled);
        $this->assertSame('acknowledge', $redis->calls[array_key_last($redis->calls)][0]);
    }

    public function test_worker_leaves_a_failed_entry_pending(): void
    {
        $redis = new FakeStreamsClient;
        $redis->responses['readGroup'] = [
            ['id' => '2-0', 'fields' => ['type' => 'order.placed']],
        ];

        $worker = new BillingConsumer($redis);
        $worker->fail = true;
        $worker->once();

        $commands = array_column($redis->calls, 0);
        $this->assertNotContains('acknowledge', $commands);
        $this->assertSame([], $worker->failedIds);
    }

    public function test_worker_acks_and_fails_when_deliveries_are_exhausted(): void
    {
        $redis = new FakeStreamsClient;
        $redis->responses['readGroup'] = [
            ['id' => '3-0', 'fields' => ['type' => 'order.placed']],
        ];
        $redis->responses['pending'] = [
            ['id' => '3-0', 'consumer' => 'worker', 'idle' => 10, 'deliveries' => 5],
        ];

        $worker = new BillingConsumer($redis);
        $worker->fail = true;
        $worker->once();

        $this->assertSame(['3-0'], $worker->failedIds);
        $commands = array_column($redis->calls, 0);
        $this->assertContains('acknowledge', $commands);
    }
}

class OrderPlaced extends StreamEvent
{
    public function __construct(
        public string $orderId,
        public int $total,
    ) {}

    public function stream(): string
    {
        return 'orders';
    }

    public static function dispatchFor(FakeStreamsClient $client, string $orderId, int $total): Entry
    {
        return \Bhamner\RedisStreams\Stream::on('orders', $client)->add((new self($orderId, $total))->fields());
    }
}

class BillingConsumer extends Consumer
{
    protected string $stream = 'orders';

    protected string $group = 'billing';

    protected ?string $consumer = 'worker';

    protected ?int $count = 10;

    protected ?int $block = 0;

    protected ?int $claimAfter = 60000;

    protected ?int $maxDeliveries = 5;

    /** @var list<string> */
    public array $handled = [];

    /** @var list<string> */
    public array $failedIds = [];

    public bool $fail = false;

    public function __construct(FakeStreamsClient $client)
    {
        $this->client = $client;
    }

    public function handle(Entry $entry): void
    {
        if ($this->fail) {
            throw new RuntimeException('billing down');
        }

        $event = $entry->event();
        $this->handled[] = $event instanceof OrderPlaced ? $event->orderId : $entry->id;
    }

    public function failed(Entry $entry, \Throwable $exception): void
    {
        $this->failedIds[] = $entry->id;
    }
}
