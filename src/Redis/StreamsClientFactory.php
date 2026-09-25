<?php

namespace Bhamner\RedisStreams\Redis;

use Bhamner\RedisStreams\Contracts\StreamsClient;

class StreamsClientFactory
{
    public function make(?string $connection = null): StreamsClient
    {
        return new LaravelStreamsClient(new LaravelCommandGateway(
            $connection ?? (string) config('redis-streams.connection', 'default'),
        ));
    }
}
