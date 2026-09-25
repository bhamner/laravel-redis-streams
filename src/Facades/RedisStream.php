<?php

namespace Bhamner\RedisStreams\Facades;

use Bhamner\RedisStreams\Stream as StreamBuilder;
use Illuminate\Support\Facades\Facade;

/**
 * @method static StreamBuilder on(string $name, ?string $connection = null)
 */
class RedisStream extends Facade
{
    protected static function getFacadeAccessor(): string
    {
        return RedisStreamFactory::class;
    }
}
