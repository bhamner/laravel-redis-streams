<?php

namespace Bhamner\RedisStreams\Redis;

use Illuminate\Support\Facades\Redis;

class LaravelCommandGateway implements CommandGateway
{
    public function __construct(private string $connection) {}

    public function prefix(): string
    {
        return ConnectionPrefix::fromClient(Redis::connection($this->connection)->client());
    }

    public function execute(array $arguments): mixed
    {
        return RedisCommand::execute(Redis::connection($this->connection)->client(), $arguments);
    }
}
