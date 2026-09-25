<?php

return [

    /*
    |--------------------------------------------------------------------------
    | Redis connection
    |--------------------------------------------------------------------------
    |
    | Name of a connection from config/database.php. Stream keys use that
    | connection's prefix, the same way the rest of the application does.
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

];
