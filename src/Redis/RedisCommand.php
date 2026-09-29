<?php

namespace Bhamner\RedisStreams\Redis;

use RuntimeException;

class RedisCommand
{
    /**
     * @param  list<string|int>  $arguments
     */
    public static function execute(object $client, array $arguments): mixed
    {
        if ($client instanceof \Redis || (class_exists(\RedisCluster::class) && $client instanceof \RedisCluster)) {
            $result = $client->rawCommand(...$arguments);

            if ($result === false) {
                $error = $client->getLastError() ?: 'Redis command failed.';
                $client->clearLastError();

                throw new RuntimeException($error);
            }

            return $result;
        }

        if (! method_exists($client, 'executeRaw')) {
            throw new RuntimeException('Redis client cannot run raw commands.');
        }

        $error = false;
        $result = $client->executeRaw($arguments, $error);

        if ($error) {
            $message = is_string($result) && $result !== '' ? $result : 'Redis command failed.';

            throw new RuntimeException($message);
        }

        return $result;
    }
}
