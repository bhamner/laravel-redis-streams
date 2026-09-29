<?php

namespace Bhamner\RedisStreams;

use Bhamner\RedisStreams\Contracts\StreamsClient;
use Throwable;

abstract class Consumer
{
    protected string $stream;

    protected string $group;

    protected ?string $connection = null;

    protected ?string $consumer = null;

    protected ?StreamsClient $client = null;

    protected ?int $count = null;

    protected ?int $block = null;

    protected ?int $claimAfter = null;

    protected ?int $maxDeliveries = null;

    /**
     * Ignore entries older than this many hours. Null uses the config value.
     * Zero keeps every entry.
     */
    protected ?float $ignoreOlderThan = null;

    /**
     * Dead-letter stream name. Null uses the config value. An empty string
     * disables it. `{stream}` is replaced with the source stream name.
     */
    protected ?string $deadLetterStream = null;

    protected string $start = '$';

    abstract public function handle(Entry $entry): void;

    public function failed(Entry $entry, Throwable $exception): void {}

    public function as(string $consumer): static
    {
        $this->consumer = $consumer;

        return $this;
    }

    public function once(): int
    {
        $this->fillDefaults();

        $group = $this->group();
        $processed = 0;

        foreach ($group->claimIdle($this->claimAfter, $this->count) as $entry) {
            $this->process($entry, $group);
            $processed++;
        }

        foreach ($group->read($this->count, $this->block) as $entry) {
            $this->process($entry, $group);
            $processed++;
        }

        return $processed;
    }

    /**
     * @param  (callable(): bool)|null  $shouldQuit
     */
    public function work(?callable $shouldQuit = null): void
    {
        while (! ($shouldQuit && $shouldQuit())) {
            $this->once();
        }
    }

    private function process(Entry $entry, Group $group): void
    {
        if ($this->isOlderThanCutoff($entry)) {
            $entry->ack();

            return;
        }

        $deliveries = $entry->deliveries ?? $group->pending(1, $entry->id, $entry->id)->first()?->deliveries ?? 1;

        if ($deliveries > $this->maxDeliveries) {
            $exception = new \RuntimeException("Entry {$entry->id} exceeded {$this->maxDeliveries} deliveries.");
            $this->deadLetter($entry, $exception, $deliveries);
            $entry->ack();
            $this->failed($entry, $exception);

            return;
        }

        try {
            $this->handle($entry);
            $entry->ack();
        } catch (Throwable $exception) {
            if ($deliveries >= $this->maxDeliveries) {
                $this->deadLetter($entry, $exception, $deliveries);
                $entry->ack();
                $this->failed($entry, $exception);
            }
        }
    }

    private function isOlderThanCutoff(Entry $entry): bool
    {
        $hours = $this->ignoreOlderThanHours();

        if ($hours <= 0) {
            return false;
        }

        $timestamp = $entry->milliseconds();

        if ($timestamp === null) {
            return false;
        }

        $cutoff = $this->nowMilliseconds() - (int) round($hours * 3_600_000);

        return $timestamp < $cutoff;
    }

    private function ignoreOlderThanHours(): float
    {
        if ($this->ignoreOlderThan !== null) {
            return $this->ignoreOlderThan;
        }

        $configured = $this->configured('redis-streams.ignore_older_than');

        if (! is_numeric($configured) || (float) $configured <= 0) {
            return 0.0;
        }

        return (float) $configured;
    }

    private function deadLetter(Entry $entry, Throwable $exception, int $deliveries): void
    {
        $stream = $this->deadLetterStreamName();

        if ($stream === null) {
            return;
        }

        Stream::on($stream, $this->client, $this->connection)->add([
            ...$entry->fields,
            'dead_stream' => $entry->stream,
            'dead_id' => $entry->id,
            'dead_group' => $this->group,
            'dead_consumer' => $this->consumerName(),
            'dead_deliveries' => (string) $deliveries,
            'dead_error' => $exception->getMessage(),
            'dead_failed_at' => (string) $this->nowMilliseconds(),
        ]);
    }

    private function deadLetterStreamName(): ?string
    {
        if ($this->deadLetterStream !== null) {
            $name = $this->deadLetterStream;
        } else {
            $configured = $this->configured('redis-streams.dead_letter_stream', '{stream}:dead');
            $name = ($configured === null || $configured === false) ? '{stream}:dead' : (string) $configured;
        }

        if ($name === '') {
            return null;
        }

        return str_replace('{stream}', $this->stream, $name);
    }

    protected function nowMilliseconds(): int
    {
        return (int) floor(microtime(true) * 1000);
    }

    private function configured(string $key, mixed $default = null): mixed
    {
        try {
            if (! function_exists('app') || ! app()->bound('config')) {
                return $default;
            }

            return config($key, $default);
        } catch (Throwable) {
            return $default;
        }
    }

    private function group(): Group
    {
        $stream = Stream::on($this->stream, $this->client, $this->connection);

        return $stream->group($this->group)->consumer($this->consumerName())->ensure($this->start);
    }

    private function consumerName(): string
    {
        return $this->consumer ?? gethostname().'-'.getmypid();
    }

    private function fillDefaults(): void
    {
        $this->count ??= (int) $this->configured('redis-streams.count', 10);
        $this->block ??= (int) $this->configured('redis-streams.block', 2000);
        $this->claimAfter ??= (int) $this->configured('redis-streams.claim_after', 60000);
        $this->maxDeliveries ??= (int) $this->configured('redis-streams.max_deliveries', 5);
    }
}
