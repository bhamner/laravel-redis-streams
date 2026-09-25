<?php

namespace Bhamner\RedisStreams\Tests\Support;

use Bhamner\RedisStreams\Contracts\StreamsClient;

class FakeStreamsClient implements StreamsClient
{
    /** @var list<array{0: string, 1: array<string, mixed>}> */
    public array $calls = [];

    /** @var array<string, mixed> */
    public array $responses = [];

    public function add(string $stream, array $fields, string $id = '*', ?int $maxLength = null, bool $approximate = true): string
    {
        $this->calls[] = ['add', compact('stream', 'fields', 'id', 'maxLength', 'approximate')];

        return $this->responses['add'] ?? '1-0';
    }

    public function range(string $stream, string $start, string $end, ?int $count = null, bool $reverse = false): array
    {
        $this->calls[] = ['range', compact('stream', 'start', 'end', 'count', 'reverse')];

        return $this->responses['range'] ?? [];
    }

    public function length(string $stream): int
    {
        $this->calls[] = ['length', compact('stream')];

        return $this->responses['length'] ?? 0;
    }

    public function delete(string $stream, array $ids): int
    {
        $this->calls[] = ['delete', compact('stream', 'ids')];

        return $this->responses['delete'] ?? count($ids);
    }

    public function trim(string $stream, int $maxLength, bool $approximate = true): int
    {
        $this->calls[] = ['trim', compact('stream', 'maxLength', 'approximate')];

        return $this->responses['trim'] ?? 0;
    }

    public function createGroup(string $stream, string $group, string $start = '$'): void
    {
        $this->calls[] = ['createGroup', compact('stream', 'group', 'start')];
    }

    public function destroyGroup(string $stream, string $group): void
    {
        $this->calls[] = ['destroyGroup', compact('stream', 'group')];
    }

    public function readGroup(string $stream, string $group, string $consumer, string $from = '>', ?int $count = null, ?int $blockMilliseconds = null): array
    {
        $this->calls[] = ['readGroup', compact('stream', 'group', 'consumer', 'from', 'count', 'blockMilliseconds')];

        return $this->responses['readGroup'] ?? [];
    }

    public function acknowledge(string $stream, string $group, array $ids): int
    {
        $this->calls[] = ['acknowledge', compact('stream', 'group', 'ids')];

        return $this->responses['acknowledge'] ?? count($ids);
    }

    public function pendingSummary(string $stream, string $group): array
    {
        $this->calls[] = ['pendingSummary', compact('stream', 'group')];

        return $this->responses['pendingSummary'] ?? ['count' => 0, 'min_id' => null, 'max_id' => null, 'consumers' => []];
    }

    public function pending(string $stream, string $group, string $start, string $end, int $count, ?string $consumer = null): array
    {
        $this->calls[] = ['pending', compact('stream', 'group', 'start', 'end', 'count', 'consumer')];

        return $this->responses['pending'] ?? [];
    }

    public function autoClaim(string $stream, string $group, string $consumer, int $minIdleMilliseconds, string $start = '0-0', int $count = 10): array
    {
        $this->calls[] = ['autoClaim', compact('stream', 'group', 'consumer', 'minIdleMilliseconds', 'start', 'count')];

        return $this->responses['autoClaim'] ?? ['next' => '0-0', 'entries' => []];
    }

    public function consumers(string $stream, string $group): array
    {
        $this->calls[] = ['consumers', compact('stream', 'group')];

        return $this->responses['consumers'] ?? [];
    }
}
