<?php

namespace Bhamner\RedisStreams\Redis;

interface CommandGateway
{
    public function prefix(): string;

    /**
     * @param  list<string|int>  $arguments
     */
    public function execute(array $arguments): mixed;
}
