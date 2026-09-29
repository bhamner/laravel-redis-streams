<?php

namespace Bhamner\RedisStreams\Tests;

use Bhamner\RedisStreams\Redis\RedisCommand;
use PHPUnit\Framework\TestCase;
use RuntimeException;

class RedisCommandTest extends TestCase
{
    public function test_predis_error_flag_is_thrown_with_the_redis_message(): void
    {
        $client = new PredisErrorClient('BUSYGROUP Consumer Group name already exists');

        try {
            RedisCommand::execute($client, ['XGROUP', 'CREATE', 'orders', 'billing', '$', 'MKSTREAM']);
            $this->fail('A Predis error flag should throw.');
        } catch (RuntimeException $exception) {
            $this->assertStringContainsString('BUSYGROUP', $exception->getMessage());
        }
    }

    public function test_predis_success_returns_the_reply(): void
    {
        $client = new PredisSuccessClient(['4-0', ['type', 'order.placed']]);

        $result = RedisCommand::execute($client, ['XRANGE', 'orders', '-', '+']);

        $this->assertSame(['4-0', ['type', 'order.placed']], $result);
    }
}

class PredisErrorClient
{
    public function __construct(private string $message) {}

    public function executeRaw(array $arguments, &$error = null): string
    {
        $error = true;

        return $this->message;
    }
}

class PredisSuccessClient
{
    public function __construct(private mixed $reply) {}

    public function executeRaw(array $arguments, &$error = null): mixed
    {
        $error = false;

        return $this->reply;
    }
}
