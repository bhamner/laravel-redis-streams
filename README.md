# Laravel Redis Streams

A Composer package for [Redis Streams](https://redis.io/docs/latest/develop/data-types/streams/) consumer groups. `create()` appends an entry. A group delivers each new entry to one consumer, and the entry stays pending until that consumer calls `ack()`.

Laravel's Redis connection already forwards the raw commands (`XADD`, `XREADGROUP`, `XACK`, `XAUTOCLAIM`, and the rest). This package is the fluent layer on top of them. It works with both the PhpRedis extension and Predis. Stream keys use the prefix on the chosen Redis connection, including an empty prefix.

## Install

```bash
composer require bhamner/laravel-redis-streams
```

The package is on [Packagist](https://packagist.org/packages/bhamner/laravel-redis-streams).

Laravel discovers the service provider. Publish the config if you want to change the connection or worker defaults:

```bash
php artisan vendor:publish --tag=redis-streams-config
```

The package uses the Redis connection named in `REDIS_STREAM_CONNECTION` (`default` when unset).

## Append entries

```php
use Bhamner\RedisStreams\Facades\RedisStream;

$entry = RedisStream::on('orders')->add([
    'type' => 'order.placed',
    'order_id' => '123',
    'total' => '4900',
]);

$entry->id; // Redis id, such as 1710000000000-0
```

A stream class sets the Redis key:

```php
use Bhamner\RedisStreams\Stream;

class OrderStream extends Stream
{
    protected string $name = 'orders';
}

OrderStream::create([
    'type' => 'order.placed',
    'order_id' => '123',
]);

$recent = OrderStream::query()->after('0-0')->limit(20)->latest()->get();
$one = OrderStream::find('1710000000000-0');
```

`add()` accepts `maxLength` when the stream should be trimmed as it grows:

```php
OrderStream::query()->add(['type' => 'order.placed'], maxLength: 10000);
```

## Typed events

`dispatch()` stores the class name and its public properties on the stream:

```php
use Bhamner\RedisStreams\StreamEvent;

class OrderPlaced extends StreamEvent
{
    public function __construct(
        public string $orderId,
        public int $total,
    ) {}

    public function stream(): string
    {
        return 'orders';
    }
}

OrderPlaced::dispatch(orderId: '123', total: 4900);
```

## Consumer groups

Each new entry is delivered to one consumer in the group. It stays in the pending list until `ack()`. Another consumer can claim it with `claimIdle()` after the idle time has passed.

```php
$entry = OrderStream::group('billing')
    ->consumer('worker-1')
    ->pop(block: 2000);

$entry?->ack();
```

`pop()` and `read()` create the group the first time (`XGROUP CREATE ... MKSTREAM`), starting at `$` so only new entries are delivered. Pass `ensure('0')` when this group should start from the beginning of the stream.

```php
$entries = OrderStream::group('billing')->consumer('worker-1')->ensure('0')->read(count: 10);

foreach ($entries as $entry) {
    $entry->ack();
}
```

Pending work and idle claims:

```php
$summary = OrderStream::group('billing')->summary();
$waiting = OrderStream::group('billing')->pending();

$claimed = OrderStream::group('billing')
    ->consumer('worker-2')
    ->claimIdle(minIdleMilliseconds: 60_000);
```

## A worker

Extend `Consumer` and run it with `redis-streams:work`. The command claims entries that have been pending longer than `claim_after`, reads new ones, calls `handle`, and acknowledges on success. Claiming uses `XPENDING` and `XCLAIM`, which work on Redis 5 and newer. `XAUTOCLAIM` is not required.

An exception before `max_deliveries` leaves the entry pending and calls `released()`. The default `released()` writes a warning to the Laravel log. After `max_deliveries` (default 5) the worker appends the entry to the dead-letter stream, calls `failed()`, and acknowledges it. A failed claim is logged, and the worker still reads new entries.

Set `REDIS_STREAM_IGNORE_OLDER_THAN` to a number of hours to acknowledge older entries without calling `handle()`. Age is the timestamp in the Redis stream id. Leave it unset to keep every entry.

The dead-letter stream defaults to `{stream}:dead`, so entries from `orders` land on `orders:dead`. The copy keeps the original fields and adds `dead_stream`, `dead_id`, `dead_group`, `dead_consumer`, `dead_deliveries`, `dead_error`, and `dead_failed_at`. Set `REDIS_STREAM_DEAD_LETTER` to another name, or to an empty string to disable it. `{stream}` is replaced with the source stream name.

```php
use Bhamner\RedisStreams\Consumer;
use Bhamner\RedisStreams\Entry;

class BillingConsumer extends Consumer
{
    protected string $stream = 'orders';

    protected string $group = 'billing';

    public function handle(Entry $entry): void
    {
        $event = $entry->event(); // OrderPlaced, when the entry was dispatched that way
    }

    public function released(Entry $entry, \Throwable $exception): void
    {
        // another attempt will claim this entry
    }

    public function failed(Entry $entry, \Throwable $exception): void
    {
        // deliveries exhausted
    }
}
```

```bash
php artisan redis-streams:work "App\Streams\BillingConsumer" --consumer=billing
php artisan redis-streams:work "App\Streams\BillingConsumer" --once --consumer=worker-1
```

`SIGTERM` and `SIGINT` stop the loop when the `pcntl` extension is loaded.

## Scheduler

`--once` claims one batch and reads one batch, then exits. On a one-minute schedule that drains a backlog slowly. `--max-time=50` keeps reading batches for 50 seconds and then exits, so the next run is not blocked by a process that never stops. Pass a stable `--consumer`. The default name is `hostname-pid`, and a new name on every cron run is a new member of the group.

```bash
php artisan redis-streams:work "App\Streams\BillingConsumer" --consumer=billing --max-time=50
```

```php
class BillingConsumer extends Consumer
{
    protected string $stream = 'orders';

    protected string $group = 'billing';

    protected string $start = '0';

    protected ?int $count = 50;

    protected ?float $ignoreOlderThan = 24;
}
```

`$start` is `$` unless you set it, so a new group reads only entries that arrive after it is created. `0` starts at the beginning of the stream. `ignore_older_than` is off unless you set the hours. Stream ids that are not `{milliseconds}-{sequence}` are still processed.

The dead-letter stream name is configurable. The fields written there stay `dead_stream`, `dead_id`, `dead_group`, `dead_consumer`, `dead_deliveries`, `dead_error`, and `dead_failed_at`, plus the original entry fields.

## Redis and Predis

PhpRedis and Predis both use raw commands. Predis `executeRaw` does not throw: it sets an error flag and returns the Redis error string. This package reads that flag and throws. The flag is the same in Predis 2.4 and current Predis releases, so the package does not require a newer Predis. A `BUSYGROUP` error while creating a group is ignored. Any other Redis error, including a failed acknowledge or dead-letter append, is thrown.

## Defaults

| Config | Env | Default |
| --- | --- | --- |
| `connection` | `REDIS_STREAM_CONNECTION` | `default` |
| `block` | `REDIS_STREAM_BLOCK` | `2000` milliseconds |
| `count` | `REDIS_STREAM_COUNT` | `10` |
| `claim_after` | `REDIS_STREAM_CLAIM_AFTER` | `60000` milliseconds |
| `max_deliveries` | `REDIS_STREAM_MAX_DELIVERIES` | `5` |
| `ignore_older_than` | `REDIS_STREAM_IGNORE_OLDER_THAN` | unset |
| `dead_letter_stream` | `REDIS_STREAM_DEAD_LETTER` | `{stream}:dead` |

Override any of those on the consumer class with `$block`, `$count`, `$claimAfter`, `$maxDeliveries`, `$ignoreOlderThan`, `$deadLetterStream`, or `$start`. An empty `$deadLetterStream` disables the dead letter for that consumer. `$ignoreOlderThan` of `0` keeps every entry. `$start` of `0` creates the group at the beginning of the stream.

## Contributing

Bug reports and pull requests are welcome on [GitHub](https://github.com/bhamner/laravel-redis-streams).

Open an [issue](https://github.com/bhamner/laravel-redis-streams/issues) for a bug. Include the Laravel and Redis versions, whether you use PhpRedis or Predis, and the smallest snippet that shows the failure.

To send a change:

1. Fork the repository and create a branch.
2. Add or update a test when the change affects behavior.
3. Run `vendor/bin/phpunit`. The suite needs PHP 8.2 or newer.
4. Open a pull request against `main` and describe what the change does.
