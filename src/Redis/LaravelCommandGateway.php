<?php

namespace Bhamner\RedisStreams\Redis;

use Illuminate\Support\Facades\Redis;
use RuntimeException;

class LaravelCommandGateway implements CommandGateway
{
    public function __construct(private string $connection) {}

    public function prefix(): string
    {
        $client = Redis::connection($this->connection)->client();

        if ($client instanceof \Redis) {
            $prefix = $client->getOption(\Redis::OPT_PREFIX);

            return is_string($prefix) ? $prefix : '';
        }

        return (string) config('database.redis.options.prefix', '');
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
