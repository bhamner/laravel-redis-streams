<?php

namespace Bhamner\RedisStreams\Redis;

use Bhamner\RedisStreams\Contracts\StreamsClient;
use RuntimeException;

class LaravelStreamsClient implements StreamsClient
{
    public function __construct(
        private CommandGateway $gateway,
        private ResponseParser $parser = new ResponseParser,
    ) {}

    public function add(string $stream, array $fields, string $id = '*', ?int $maxLength = null, bool $approximate = true): string
    {
        $arguments = ['XADD', $this->key($stream)];

        if ($maxLength !== null) {
            $arguments[] = 'MAXLEN';

            if ($approximate) {
                $arguments[] = '~';
            }

            $arguments[] = $maxLength;
        }

        $arguments[] = $id;

        foreach ($fields as $field => $value) {
            $arguments[] = (string) $field;
            $arguments[] = is_scalar($value) || $value === null
                ? (string) $value
                : json_encode($value, JSON_THROW_ON_ERROR);
        }

        $id = $this->gateway->execute($arguments);

        if (! is_string($id) || $id === '') {
            throw new RuntimeException('Redis did not return a stream entry id.');
        }

        return $id;
    }

    public function range(string $stream, string $start, string $end, ?int $count = null, bool $reverse = false): array
    {
        $arguments = [$reverse ? 'XREVRANGE' : 'XRANGE', $this->key($stream), $start, $end];

        if ($count !== null) {
            $arguments[] = 'COUNT';
            $arguments[] = $count;
        }

        return $this->parser->entries($this->gateway->execute($arguments));
    }

    public function length(string $stream): int
    {
        return (int) $this->gateway->execute(['XLEN', $this->key($stream)]);
    }

    public function delete(string $stream, array $ids): int
    {
        if ($ids === []) {
            return 0;
        }

        return (int) $this->gateway->execute(['XDEL', $this->key($stream), ...$ids]);
    }

    public function trim(string $stream, int $maxLength, bool $approximate = true): int
    {
        $arguments = ['XTRIM', $this->key($stream), 'MAXLEN'];

        if ($approximate) {
            $arguments[] = '~';
        }

        $arguments[] = $maxLength;

        return (int) $this->gateway->execute($arguments);
    }

    public function createGroup(string $stream, string $group, string $start = '$'): void
    {
        try {
            $this->gateway->execute([
                'XGROUP', 'CREATE', $this->key($stream), $group, $start, 'MKSTREAM',
            ]);
        } catch (\Throwable $exception) {
            if (! str_contains($exception->getMessage(), 'BUSYGROUP')) {
                throw $exception;
            }
        }
    }

    public function destroyGroup(string $stream, string $group): void
    {
        $this->gateway->execute(['XGROUP', 'DESTROY', $this->key($stream), $group]);
    }

    public function readGroup(
        string $stream,
        string $group,
        string $consumer,
        string $from = '>',
        ?int $count = null,
        ?int $blockMilliseconds = null,
    ): array {
        $arguments = ['XREADGROUP', 'GROUP', $group, $consumer];

        if ($count !== null) {
            $arguments[] = 'COUNT';
            $arguments[] = $count;
        }

        if ($blockMilliseconds !== null) {
            $arguments[] = 'BLOCK';
            $arguments[] = $blockMilliseconds;
        }

        $arguments[] = 'STREAMS';
        $arguments[] = $this->key($stream);
        $arguments[] = $from;

        return $this->parser->read($this->gateway->execute($arguments));
    }

    public function acknowledge(string $stream, string $group, array $ids): int
    {
        if ($ids === []) {
            return 0;
        }

        return (int) $this->gateway->execute(['XACK', $this->key($stream), $group, ...$ids]);
    }

    public function pendingSummary(string $stream, string $group): array
    {
        return $this->parser->pendingSummary(
            $this->gateway->execute(['XPENDING', $this->key($stream), $group]),
        );
    }

    public function pending(
        string $stream,
        string $group,
        string $start,
        string $end,
        int $count,
        ?string $consumer = null,
    ): array {
        $arguments = ['XPENDING', $this->key($stream), $group, $start, $end, $count];

        if ($consumer !== null) {
            $arguments[] = $consumer;
        }

        return $this->parser->pending($this->gateway->execute($arguments));
    }

    public function autoClaim(
        string $stream,
        string $group,
        string $consumer,
        int $minIdleMilliseconds,
        string $start = '0-0',
        int $count = 10,
    ): array {
        return $this->parser->autoClaim($this->gateway->execute([
            'XAUTOCLAIM',
            $this->key($stream),
            $group,
            $consumer,
            $minIdleMilliseconds,
            $start,
            'COUNT',
            $count,
        ]));
    }

    public function consumers(string $stream, string $group): array
    {
        return $this->parser->consumers(
            $this->gateway->execute(['XINFO', 'CONSUMERS', $this->key($stream), $group]),
        );
    }

    private function key(string $stream): string
    {
        return $this->gateway->prefix().$stream;
    }
}
