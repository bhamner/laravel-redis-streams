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
        $this->assertSame(['2-0'], $worker->releasedIds);
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
        $added = $this->call($redis, 'add');
        $this->assertSame('orders:dead', $added['stream']);
        $this->assertSame('3-0', $added['fields']['dead_id']);
        $this->assertSame('billing down', $added['fields']['dead_error']);
        $this->assertSame('acknowledge', $redis->calls[array_key_last($redis->calls)][0]);
    }

    public function test_worker_acknowledges_entries_older_than_the_cutoff_without_handling_them(): void
    {
        $redis = new FakeStreamsClient;
        $redis->responses['readGroup'] = [
            ['id' => '1000-0', 'fields' => ['type' => 'order.placed']],
            ['id' => '5000000-0', 'fields' => ['type' => 'order.placed']],
        ];

        $worker = new BillingConsumer($redis);
        $worker->clock = 5_000_000;
        $worker->ignoreFor(1);
        $worker->once();

        $this->assertSame(['5000000-0'], $worker->handled);
        $acknowledged = array_values(array_filter(
            $redis->calls,
            fn (array $call) => $call[0] === 'acknowledge',
        ));
        $this->assertSame(['1000-0'], $acknowledged[0][1]['ids']);
        $this->assertSame(['5000000-0'], $acknowledged[1][1]['ids']);
    }

    public function test_worker_can_disable_the_dead_letter_stream(): void
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
        $worker->deadLettersTo('');
        $worker->once();

        $this->assertSame(['3-0'], $worker->failedIds);
        $this->assertNotContains('add', array_column($redis->calls, 0));
    }

    public function test_a_claim_error_is_reported_and_new_entries_are_still_read(): void
    {
        $redis = new FakeStreamsClient;
        $redis->autoClaimError = new RuntimeException('XPENDING failed');
        $redis->responses['readGroup'] = [
            ['id' => '9-0', 'fields' => ['type' => 'order.placed']],
        ];

        $worker = new BillingConsumer($redis);
        $processed = $worker->once();

        $this->assertSame(1, $processed);
        $this->assertSame(['9-0'], $worker->handled);
        $this->assertSame(['XPENDING failed'], $worker->reports);
    }

    public function test_work_stops_when_max_time_is_reached(): void
    {
        $redis = new FakeStreamsClient;
        $worker = new BillingConsumer($redis);
        $worker->limitPasses = true;

        $worker->work(maxSeconds: 50);

        $this->assertSame(2, $worker->passes);
    }

    /**
     * @return array<string, mixed>
     */
    private function call(FakeStreamsClient $redis, string $name): array
    {
        foreach ($redis->calls as $call) {
            if ($call[0] === $name) {
                return $call[1];
            }
        }

        $this->fail("No {$name} call was recorded.");
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

    /** @var list<string> */
    public array $releasedIds = [];

    /** @var list<string> */
    public array $reports = [];

    public bool $fail = false;

    public bool $limitPasses = false;

    public int $passes = 0;

    public int $clock = 0;

    public function ignoreFor(float $hours): void
    {
        $this->ignoreOlderThan = $hours;
    }

    public function deadLettersTo(string $stream): void
    {
        $this->deadLetterStream = $stream;
    }

    protected function nowMilliseconds(): int
    {
        return $this->clock > 0 ? $this->clock : parent::nowMilliseconds();
    }

    protected function currentTimestamp(): int
    {
        return $this->limitPasses ? $this->clock : parent::currentTimestamp();
    }

    public function once(): int
    {
        if (! $this->limitPasses) {
            return parent::once();
        }

        $this->passes++;
        $this->clock += 30;

        return 0;
    }

    public function released(Entry $entry, \Throwable $exception): void
    {
        $this->releasedIds[] = $entry->id;
        $this->report($exception);
    }

    protected function report(\Throwable $exception): void
    {
        $this->reports[] = $exception->getMessage();
    }

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
