<?php

namespace Bhamner\RedisStreams\Redis;

class ConnectionPrefix
{
    /**
     * Prefix configured on this Redis connection.
     *
     * An empty prefix stays empty. Predis must not fall back to the global
     * `database.redis.options.prefix`, or a connection such as `event_bus`
     * with `'prefix' => ''` would read and write a different key.
     */
    public static function fromClient(object $client): string
    {
        if ($client instanceof \Redis || (class_exists(\RedisCluster::class) && $client instanceof \RedisCluster)) {
            $prefix = $client->getOption(\Redis::OPT_PREFIX);

            return is_string($prefix) ? $prefix : '';
        }

        if (! method_exists($client, 'getOptions')) {
            return '';
        }

        $options = $client->getOptions();
        $prefix = is_object($options) ? ($options->prefix ?? null) : null;

        if (is_object($prefix) && method_exists($prefix, 'getPrefix')) {
            return (string) $prefix->getPrefix();
        }

        return is_string($prefix) ? $prefix : '';
    }
}
