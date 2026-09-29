<?php

namespace Bhamner\RedisStreams\Tests;

use Bhamner\RedisStreams\Redis\ConnectionPrefix;
use PHPUnit\Framework\TestCase;

class ConnectionPrefixTest extends TestCase
{
    public function test_an_empty_predis_prefix_is_not_replaced(): void
    {
        $client = new PrefixClient('');

        $this->assertSame('', ConnectionPrefix::fromClient($client));
    }

    public function test_a_connection_specific_predis_prefix_is_kept(): void
    {
        $client = new PrefixClient('event_bus_');

        $this->assertSame('event_bus_', ConnectionPrefix::fromClient($client));
    }

    public function test_a_missing_predis_prefix_is_empty(): void
    {
        $client = new PrefixClient(null);

        $this->assertSame('', ConnectionPrefix::fromClient($client));
    }
}

class PrefixClient
{
    public function __construct(private ?string $prefix) {}

    public function getOptions(): object
    {
        return new PrefixOptions($this->prefix);
    }
}

class PrefixOptions
{
    public function __construct(private ?string $prefix) {}

    public function __get(string $name): mixed
    {
        if ($name !== 'prefix' || $this->prefix === null) {
            return null;
        }

        return new PrefixProcessor($this->prefix);
    }
}

class PrefixProcessor
{
    public function __construct(private string $prefix) {}

    public function getPrefix(): string
    {
        return $this->prefix;
    }
}
