<?php

namespace Bhamner\RedisStreams;

use Bhamner\RedisStreams\Console\WorkCommand;
use Bhamner\RedisStreams\Redis\StreamsClientFactory;
use Illuminate\Support\ServiceProvider;

class RedisStreamsServiceProvider extends ServiceProvider
{
    public function register(): void
    {
        $this->mergeConfigFrom(__DIR__.'/../config/redis-streams.php', 'redis-streams');

        $this->app->singleton(StreamsClientFactory::class);
    }

    public function boot(): void
    {
        if (! $this->app->runningInConsole()) {
            return;
        }

        $this->publishes([
            __DIR__.'/../config/redis-streams.php' => config_path('redis-streams.php'),
        ], 'redis-streams-config');

        $this->commands([
            WorkCommand::class,
        ]);
    }
}
