# Laravel Redis Streams

A Composer package for [Redis Streams](https://redis.io/docs/latest/develop/data-types/streams/) consumer groups. Publishing an entry feels like `Model::create`. Reading a group feels like `queue:work`: one consumer in the group gets each new entry, and entries stay pending until they are acknowledged.

Laravel's Redis connection already forwards the raw commands (`XADD`, `XREADGROUP`, `XACK`, `XAUTOCLAIM`, and the rest). This package is the fluent layer on top of them. It works with both the PhpRedis extension and Predis, and it keeps your configured Redis key prefix.

## Install

Submit this repository to [Packagist](https://packagist.org/packages/submit) once, then:

```bash
composer require bhamner/laravel-redis-streams
```

Until it is on Packagist, point Composer at the GitHub repository:

```bash
composer config repositories.bhamner/laravel-redis-streams vcs https://github.com/bhamner/laravel-redis-streams
composer require bhamner/laravel-redis-streams:dev-main
```

Laravel discovers the service provider. Publish the config if you want to change the connection or worker defaults:

```bash
php artisan vendor:publish --tag=redis-streams-config
```

The package uses the Redis connection named in `REDIS_STREAM_CONNECTION` (`default` when unset).

## Append entries

Like creating a row:

```php
use Bhamner\RedisStreams\Facades\RedisStream;

$entry = RedisStream::on('orders')->add([
    'type' => 'order.placed',
    'order_id' => '123',
    'total' => '4900',
]);

$entry->id; // Redis id, such as 1710000000000-0
```

Or give the stream a class, the way a model names its table:

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

Like dispatching a job, with the class name and public properties stored on the stream:

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

A group is the queue. Each new entry is delivered to one consumer in the group. `ack()` is the successful delete. Until then the entry stays in the pending list and another worker can claim it.

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

$stolen = OrderStream::group('billing')
    ->consumer('worker-2')
    ->claimIdle(minIdleMilliseconds: 60_000);
```

## A worker

Subclass `Consumer` the way you would subclass a job. `redis-streams:work` loops: claim entries that have been pending too long, read new ones, call `handle`, and acknowledge on success. An exception leaves the entry pending so another attempt can claim it. After `max_deliveries` (default 5) the worker calls `failed()` and acknowledges it so the group does not retry it forever.

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
