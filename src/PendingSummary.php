<?php

namespace Bhamner\RedisStreams;

class PendingSummary
{
    /**
     * @param  array<string, int>  $consumers
     */
    public function __construct(
        public readonly int $count,
        public readonly ?string $minId,
        public readonly ?string $maxId,
        public readonly array $consumers,
    ) {}
}
