<?php

namespace Bhamner\RedisStreams\Console;

use Bhamner\RedisStreams\Consumer;
use Illuminate\Console\Command;
use InvalidArgumentException;

class WorkCommand extends Command
{
    protected $signature = 'redis-streams:work
        {consumer : Consumer class that handles entries from one group}
        {--once : Process a single read, then exit}
        {--consumer= : Name of this worker inside the consumer group}';

    protected $description = 'Read a Redis stream consumer group, like queue:work';

    private bool $shouldQuit = false;

    public function handle(): int
    {
        $class = $this->argument('consumer');

        if (! is_string($class) || ! class_exists($class) || ! is_subclass_of($class, Consumer::class)) {
            throw new InvalidArgumentException("{$class} must extend ".Consumer::class.'.');
        }

        /** @var Consumer $worker */
        $worker = $this->laravel->make($class);

        if (is_string($this->option('consumer')) && $this->option('consumer') !== '') {
            $worker->as($this->option('consumer'));
        }

        $this->listenForSignals();

        if ($this->option('once')) {
            $count = $worker->once();
            $this->info("Processed {$count} ".str('entry')->plural($count).'.');

            return self::SUCCESS;
        }

        $worker->work(fn () => $this->shouldQuit);

        return self::SUCCESS;
    }

    private function listenForSignals(): void
    {
        if (! extension_loaded('pcntl')) {
            return;
        }

        pcntl_async_signals(true);

        $stop = function (): void {
            $this->shouldQuit = true;
        };

        pcntl_signal(SIGTERM, $stop);
        pcntl_signal(SIGINT, $stop);
    }
}
