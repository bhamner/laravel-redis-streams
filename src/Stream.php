<?php

namespace Bhamner\RedisStreams;

use Bhamner\RedisStreams\Contracts\StreamsClient;
use Illuminate\Support\Collection;
use LogicException;

class Stream
{
    protected string $name = '';

    private string $start = '-';

    private string $end = '+';

    private ?int $limit = null;

    private bool $reverse = false;

    private ?string $connection = null;

    public function __construct(private ?StreamsClient $client = null) {}

    public static function on(string $name, ?StreamsClient $client = null, ?string $connection = null): static
    {
        $stream = static::query($client, $connection);
        $stream->name = $name;

        return $stream;
    }

    public static function query(?StreamsClient $client = null, ?string $connection = null): static
    {
        $stream = new static($client);
        $stream->connection = $connection;

        return $stream;
    }

    /**
     * @param  array<string, scalar|null>  $fields
     */
    public static function create(array $fields, ?StreamsClient $client = null, ?string $connection = null): Entry
    {
        return static::query($client, $connection)->add($fields);
    }

    public static function find(string $id, ?StreamsClient $client = null, ?string $connection = null): ?Entry
    {
        return static::query($client, $connection)->whereId($id)->first();
    }

    public function connection(?string $connection): static
    {
        $this->connection = $connection;

        return $this;
    }

    public function after(string $id): static
    {
        $this->start = $id;
        $this->end = '+';

        return $this;
    }

    public function before(string $id): static
    {
        $this->start = '-';
        $this->end = $id;

        return $this;
    }

    public function whereId(string $id): static
    {
        $this->start = $id;
        $this->end = $id;

        return $this;
    }

    public function limit(int $count): static
    {
        $this->limit = $count;

        return $this;
    }

    public function latest(): static
    {
        $this->reverse = true;

        return $this;
    }

    /**
     * @param  array<string, scalar|null>  $fields
     */
    public function add(array $fields, ?string $id = null, ?int $maxLength = null, bool $approximate = true): Entry
    {
        $entryId = $this->client()->add($this->streamName(), $fields, $id ?? '*', $maxLength, $approximate);

        return new Entry($this->streamName(), $entryId, $this->stringFields($fields));
    }

    public function first(): ?Entry
    {
        return $this->limit(1)->get()->first();
    }

    /**
     * @return Collection<int, Entry>
     */
    public function get(): Collection
    {
        $rows = $this->client()->range(
            $this->streamName(),
            $this->start,
            $this->end,
            $this->limit,
            $this->reverse,
        );

        return new Collection(array_map(
            fn (array $row) => new Entry($this->streamName(), $row['id'], $row['fields']),
            $rows,
        ));
    }

    public function length(): int
    {
        return $this->client()->length($this->streamName());
    }

    public function trim(int $maxLength, bool $approximate = true): int
    {
        return $this->client()->trim($this->streamName(), $maxLength, $approximate);
    }

    public function delete(string ...$ids): int
    {
        return $this->client()->delete($this->streamName(), array_values($ids));
    }

    public function group(string $name): Group
    {
        return new Group($this->client(), $this->streamName(), $name);
    }

    private function client(): StreamsClient
    {
        if ($this->client !== null) {
            return $this->client;
        }

        return app(Redis\StreamsClientFactory::class)->make($this->connection);
    }

    private function streamName(): string
    {
        if ($this->name === '') {
            throw new LogicException(static::class.' has no stream name. Set $name or call Stream::on().');
        }

        return $this->name;
    }

    /**
     * @param  array<string, scalar|null>  $fields
     * @return array<string, string>
     */
    private function stringFields(array $fields): array
    {
        $stringFields = [];

        foreach ($fields as $key => $value) {
            $stringFields[(string) $key] = is_scalar($value) || $value === null
                ? (string) $value
                : json_encode($value, JSON_THROW_ON_ERROR);
        }

        return $stringFields;
    }
}
