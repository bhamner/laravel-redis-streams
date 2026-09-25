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
        $deliveries = $entry->deliveries ?? $group->pending(1, $entry->id, $entry->id)->first()?->deliveries ?? 1;

        if ($deliveries > $this->maxDeliveries) {
            $this->failed($entry, new \RuntimeException("Entry {$entry->id} exceeded {$this->maxDeliveries} deliveries."));
            $entry->ack();

            return;
        }

        try {
            $this->handle($entry);
            $entry->ack();
        } catch (Throwable $exception) {
            if ($deliveries >= $this->maxDeliveries) {
                $this->failed($entry, $exception);
                $entry->ack();
            }
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
        $this->count ??= (int) config('redis-streams.count', 10);
        $this->block ??= (int) config('redis-streams.block', 2000);
        $this->claimAfter ??= (int) config('redis-streams.claim_after', 60000);
        $this->maxDeliveries ??= (int) config('redis-streams.max_deliveries', 5);
    }
}
