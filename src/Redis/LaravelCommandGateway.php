<?php

namespace Bhamner\RedisStreams\Redis;

use Illuminate\Support\Facades\Redis;
use RuntimeException;

class LaravelCommandGateway implements CommandGateway
{
    public function __construct(private string $connection) {}

    public function prefix(): string
    {
        return ConnectionPrefix::fromClient(Redis::connection($this->connection)->client());
    }

    public function execute(array $arguments): mixed
    {
        $client = Redis::connection($this->connection)->client();

        if ($client instanceof \Redis) {
            $result = $client->rawCommand(...$arguments);

            if ($result === false) {
                $error = $client->getLastError() ?: 'Redis command failed.';
                $client->clearLastError();

                throw new RuntimeException($error);
            }

            return $result;
        }

        return $client->executeRaw($arguments);
    }
}
