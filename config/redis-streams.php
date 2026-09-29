<?php

return [

    /*
    |--------------------------------------------------------------------------
    | Redis connection
    |--------------------------------------------------------------------------
    |
    | Name of a connection from config/database.php. Stream keys use that
    | connection's prefix. A connection with an empty prefix, such as an
    | event bus, is not given the global Redis prefix.
    |
    */

    'connection' => env('REDIS_STREAM_CONNECTION', 'default'),

    /*
    |--------------------------------------------------------------------------
    | Worker defaults
    |--------------------------------------------------------------------------
    |
    | These match a queue worker: read a batch, block while the stream is
    | idle, then claim messages another consumer left pending for too long.
    |
    */

    'block' => (int) env('REDIS_STREAM_BLOCK', 2000),

    'count' => (int) env('REDIS_STREAM_COUNT', 10),

    'claim_after' => (int) env('REDIS_STREAM_CLAIM_AFTER', 60000),

    'max_deliveries' => (int) env('REDIS_STREAM_MAX_DELIVERIES', 5),

    /*
    |--------------------------------------------------------------------------
    | Ignore old entries
    |--------------------------------------------------------------------------
    |
    | Hours after which a worker acknowledges an entry without calling handle().
    | The age comes from the Redis stream id. Null keeps every entry.
    |
    */

    'ignore_older_than' => env('REDIS_STREAM_IGNORE_OLDER_THAN'),

    /*
    |--------------------------------------------------------------------------
    | Dead-letter stream
    |--------------------------------------------------------------------------
    |
    | Stream that receives an entry after max_deliveries. {stream} is replaced
    | with the source stream name. An empty value disables the dead letter.
    |
    */

    'dead_letter_stream' => env('REDIS_STREAM_DEAD_LETTER', '{stream}:dead'),

];
