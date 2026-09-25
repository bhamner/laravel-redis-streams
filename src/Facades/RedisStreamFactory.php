<?php

namespace Bhamner\RedisStreams\Facades;

use Bhamner\RedisStreams\Stream;

class RedisStreamFactory
{
    public function on(string $name, ?string $connection = null): Stream
    {
        return Stream::on($name, connection: $connection);
    }
}
