# Laravel Redis Streams

A Composer package for [Redis Streams](https://redis.io/docs/latest/develop/data-types/streams/) consumer groups. `create()` appends an entry. A group delivers each new entry to one consumer, and the entry stays pending until that consumer calls `ack()`.

Laravel's Redis connection already forwards the raw commands (`XADD`, `XREADGROUP`, `XACK`, `XAUTOCLAIM`, and the rest). This package is the fluent layer on top of them. It works with both the PhpRedis extension and Predis, and it keeps your configured Redis key prefix.

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

Extend `Consumer` and run it with `redis-streams:work`. The command claims entries that have been pending longer than `claim_after`, reads new ones, calls `handle`, and acknowledges on success. An exception leaves the entry pending. After `max_deliveries` (default 5) the worker calls `failed()` and acknowledges the entry.

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

    public function failed(Entry $entry, \Throwable $exception): void
    {
        // deliveries exhausted
    }
}
```

```bash
php artisan redis-streams:work "App\Streams\BillingConsumer"
php artisan redis-streams:work "App\Streams\BillingConsumer" --once --consumer=worker-1
```

`SIGTERM` and `SIGINT` stop the loop when the `pcntl` extension is loaded.

## Defaults

| Config | Env | Default |
| --- | --- | --- |
| `connection` | `REDIS_STREAM_CONNECTION` | `default` |
| `block` | `REDIS_STREAM_BLOCK` | `2000` milliseconds |
| `count` | `REDIS_STREAM_COUNT` | `10` |
| `claim_after` | `REDIS_STREAM_CLAIM_AFTER` | `60000` milliseconds |
| `max_deliveries` | `REDIS_STREAM_MAX_DELIVERIES` | `5` |

Override any of those on the consumer class with `$block`, `$count`, `$claimAfter`, `$maxDeliveries`, or `$start`.

## Contributing

Bug reports and pull requests are welcome on [GitHub](https://github.com/bhamner/laravel-redis-streams).

Open an [issue](https://github.com/bhamner/laravel-redis-streams/issues) for a bug. Include the Laravel and Redis versions, whether you use PhpRedis or Predis, and the smallest snippet that shows the failure.

To send a change:

1. Fork the repository and create a branch.
2. Add or update a test when the change affects behavior.
3. Run `vendor/bin/phpunit`. The suite needs PHP 8.2 or newer.
4. Open a pull request against `main` and describe what the change does.
