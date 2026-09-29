<?php

namespace Bhamner\RedisStreams;

use Closure;
use LogicException;

class Entry
{
    /**
     * @param  array<string, string>  $fields
     */
    public function __construct(
        public readonly string $stream,
        public readonly string $id,
        public readonly array $fields,
        private readonly ?Closure $acknowledger = null,
        public readonly ?int $deliveries = null,
    ) {}

    public function get(string $key, mixed $default = null): mixed
    {
        return $this->fields[$key] ?? $default;
    }

    /**
     * Milliseconds encoded in a Redis stream id, when the id has that form.
     */
    public function milliseconds(): ?int
    {
        if (! preg_match('/^(\d+)-\d+$/', $this->id, $matches)) {
            return null;
        }

        return (int) $matches[1];
    }

    public function ack(): int
    {
        if ($this->acknowledger === null) {
            throw new LogicException('This entry was not read from a consumer group.');
        }

        return ($this->acknowledger)($this->id);
    }

    public function event(): ?StreamEvent
    {
        $type = $this->fields['type'] ?? null;

        if (! is_string($type) || ! class_exists($type) || ! is_subclass_of($type, StreamEvent::class)) {
            return null;
        }

        return $type::fromEntry($this);
    }
}
