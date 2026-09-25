<?php

namespace Bhamner\RedisStreams;

class PendingMessage
{
    public function __construct(
        public readonly string $id,
        public readonly string $consumer,
        public readonly int $idle,
        public readonly int $deliveries,
    ) {}
}
