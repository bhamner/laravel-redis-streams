<?php

namespace Bhamner\RedisStreams\Contracts;

interface StreamsClient
{
    /**
     * @param  array<string, scalar|null>  $fields
     */
    public function add(string $stream, array $fields, string $id = '*', ?int $maxLength = null, bool $approximate = true): string;

    /**
     * @return list<array{id: string, fields: array<string, string>}>
     */
    public function range(string $stream, string $start, string $end, ?int $count = null, bool $reverse = false): array;

    public function length(string $stream): int;

    /**
     * @param  list<string>  $ids
     */
    public function delete(string $stream, array $ids): int;

    public function trim(string $stream, int $maxLength, bool $approximate = true): int;

    public function createGroup(string $stream, string $group, string $start = '$'): void;

    public function destroyGroup(string $stream, string $group): void;

    /**
     * @return list<array{id: string, fields: array<string, string>}>
     */
    public function readGroup(
        string $stream,
        string $group,
        string $consumer,
        string $from = '>',
        ?int $count = null,
        ?int $blockMilliseconds = null,
    ): array;

    /**
     * @param  list<string>  $ids
     */
    public function acknowledge(string $stream, string $group, array $ids): int;

    /**
     * @return array{count: int, min_id: ?string, max_id: ?string, consumers: array<string, int>}
     */
    public function pendingSummary(string $stream, string $group): array;

    /**
     * @return list<array{id: string, consumer: string, idle: int, deliveries: int}>
     */
    public function pending(
        string $stream,
        string $group,
        string $start,
        string $end,
        int $count,
        ?string $consumer = null,
    ): array;

    /**
     * @return array{next: string, entries: list<array{id: string, fields: array<string, string>}>}
     */
    public function autoClaim(
        string $stream,
        string $group,
        string $consumer,
        int $minIdleMilliseconds,
        string $start = '0-0',
        int $count = 10,
    ): array;

    /**
     * @return list<array{name: string, pending: int, idle: int}>
     */
    public function consumers(string $stream, string $group): array;
}
