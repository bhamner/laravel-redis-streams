<?php

namespace Bhamner\RedisStreams\Tests;

use Bhamner\RedisStreams\Redis\CommandGateway;
use Bhamner\RedisStreams\Redis\LaravelStreamsClient;
use Bhamner\RedisStreams\Redis\ResponseParser;
use PHPUnit\Framework\TestCase;
use RuntimeException;

class LaravelStreamsClientTest extends TestCase
{
    public function test_add_builds_an_xadd_command_with_the_connection_prefix(): void
    {
        $gateway = new RecordingGateway('app_');
        $client = new LaravelStreamsClient($gateway);

        $id = $client->add('orders', ['type' => 'order.placed', 'order_id' => 123], maxLength: 1000);

        $this->assertSame('11-0', $id);
        $this->assertSame([
            'XADD', 'app_orders', 'MAXLEN', '~', 1000, '*', 'type', 'order.placed', 'order_id', '123',
        ], $gateway->commands[0]);
    }

    public function test_read_group_parses_raw_and_associative_replies(): void
    {
        $raw = new RecordingGateway(replies: [[
            ['orders', [
                ['4-0', ['type', 'order.placed']],
            ]],
        ]]);
        $assoc = new RecordingGateway(replies: [[
            'orders' => [
                '4-0' => ['type' => 'order.placed'],
            ],
        ]]);

        $fromRaw = (new LaravelStreamsClient($raw))->readGroup('orders', 'billing', 'worker-1', count: 1, blockMilliseconds: 5);
        $fromAssoc = (new LaravelStreamsClient($assoc))->readGroup('orders', 'billing', 'worker-1');

        $this->assertSame($fromAssoc, $fromRaw);
        $this->assertSame('4-0', $fromRaw[0]['id']);
        $this->assertSame('order.placed', $fromRaw[0]['fields']['type']);
        $this->assertSame([
            'XREADGROUP', 'GROUP', 'billing', 'worker-1', 'COUNT', 1, 'BLOCK', 5, 'STREAMS', 'orders', '>',
        ], $raw->commands[0]);
    }

    public function test_create_group_ignores_busygroup(): void
    {
        $gateway = new RecordingGateway(replies: [new RuntimeException('BUSYGROUP Consumer Group name already exists')]);
        $client = new LaravelStreamsClient($gateway);

        $client->createGroup('orders', 'billing', '0');

        $this->assertSame(['XGROUP', 'CREATE', 'orders', 'billing', '0', 'MKSTREAM'], $gateway->commands[0]);
    }

    public function test_pending_and_autoclaim_shapes(): void
    {
        $parser = new ResponseParser;

        $summary = $parser->pendingSummary([2, '1-0', '2-0', [['worker-1', 2]]]);
        $this->assertSame(2, $summary['count']);
        $this->assertSame(['worker-1' => 2], $summary['consumers']);

        $claimed = $parser->autoClaim(['3-0', [
            ['2-0', ['type', 'order.placed']],
        ]]);
        $this->assertSame('3-0', $claimed['next']);
        $this->assertSame('order.placed', $claimed['entries'][0]['fields']['type']);

        $consumers = $parser->consumers([
            ['name', 'worker-1', 'pending', 1, 'idle', 40],
        ]);
        $this->assertSame('worker-1', $consumers[0]['name']);
        $this->assertSame(1, $consumers[0]['pending']);
    }
}

class RecordingGateway implements CommandGateway
{
    /** @var list<list<string|int>> */
    public array $commands = [];

    /**
     * @param  list<mixed>  $replies
     */
    public function __construct(private string $prefix = '', private array $replies = ['11-0']) {}

    public function prefix(): string
    {
        return $this->prefix;
    }

    public function execute(array $arguments): mixed
    {
        $this->commands[] = $arguments;
        $reply = array_shift($this->replies);

        if ($reply instanceof \Throwable) {
            throw $reply;
        }

        return $reply;
    }
}
