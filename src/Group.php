<?php

namespace Bhamner\RedisStreams;

use Bhamner\RedisStreams\Contracts\StreamsClient;
use Illuminate\Support\Collection;

class Group
{
    private ?string $consumer = null;

    public function __construct(
        private StreamsClient $client,
        private string $stream,
        private string $group,
    ) {}

    public function consumer(string $name): static
    {
        $this->consumer = $name;

        return $this;
    }

    public function ensure(string $start = '$'): static
    {
        $this->client->createGroup($this->stream, $this->group, $start);

        return $this;
    }

    public function destroy(): void
    {
        $this->client->destroyGroup($this->stream, $this->group);
    }

    /**
     * @return Collection<int, Entry>
     */
    public function read(int $count = 1, ?int $block = null, string $from = '>'): Collection
    {
        $this->ensure();

        try {
            $rows = $this->readRows($count, $block, $from);
        } catch (\Throwable $exception) {
            if (! str_contains($exception->getMessage(), 'NOGROUP')) {
                throw $exception;
            }

            $this->ensure();
            $rows = $this->readRows($count, $block, $from);
        }

        return new Collection(array_map(
            fn (array $row) => $this->entry($row['id'], $row['fields']),
            $rows,
        ));
    }

    public function pop(?int $block = null): ?Entry
    {
        return $this->read(1, $block)->first();
    }

    /**
     * @return Collection<int, Entry>
     */
    public function claimIdle(int $minIdleMilliseconds, int $count = 10, string $start = '0-0'): Collection
    {
        $claimed = $this->client->autoClaim(
            $this->stream,
            $this->group,
            $this->consumerName(),
            $minIdleMilliseconds,
            $start,
            $count,
        );

        return new Collection(array_map(
            fn (array $row) => $this->entry($row['id'], $row['fields']),
            $claimed['entries'],
        ));
    }

    public function acknowledge(string ...$ids): int
    {
        return $this->client->acknowledge($this->stream, $this->group, array_values($ids));
    }

    public function summary(): PendingSummary
    {
        $summary = $this->client->pendingSummary($this->stream, $this->group);

        return new PendingSummary(
            $summary['count'],
            $summary['min_id'],
            $summary['max_id'],
            $summary['consumers'],
        );
    }

    /**
     * @return Collection<int, PendingMessage>
     */
    public function pending(int $count = 10, string $start = '-', string $end = '+', ?string $consumer = null): Collection
    {
        $rows = $this->client->pending($this->stream, $this->group, $start, $end, $count, $consumer);

        return new Collection(array_map(
            fn (array $row) => new PendingMessage($row['id'], $row['consumer'], $row['idle'], $row['deliveries']),
            $rows,
        ));
    }

    /**
     * @return Collection<int, array{name: string, pending: int, idle: int}>
     */
    public function consumers(): Collection
    {
        return new Collection($this->client->consumers($this->stream, $this->group));
    }

    /**
     * @param  array<string, string>  $fields
     */
    private function entry(string $id, array $fields, ?int $deliveries = null): Entry
    {
        return new Entry(
            $this->stream,
            $id,
            $fields,
            fn (string $entryId) => $this->acknowledge($entryId),
            $deliveries,
        );
    }

    /**
     * @return list<array{id: string, fields: array<string, string>}>
     */
    private function readRows(int $count, ?int $block, string $from): array
    {
        return $this->client->readGroup(
            $this->stream,
            $this->group,
            $this->consumerName(),
            $from,
            $count,
            $block,
        );
    }

    private function consumerName(): string
    {
        return $this->consumer ?? gethostname().'-'.getmypid();
    }
}
