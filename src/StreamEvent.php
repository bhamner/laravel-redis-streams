<?php

namespace Bhamner\RedisStreams;

use ReflectionClass;
use ReflectionProperty;

abstract class StreamEvent
{
    abstract public function stream(): string;

    public static function dispatch(mixed ...$arguments): Entry
    {
        $event = new static(...$arguments);

        return Stream::on($event->stream())->add($event->fields());
    }

    /**
     * @return array{type: string, payload: string}
     */
    public function fields(): array
    {
        return [
            'type' => static::class,
            'payload' => json_encode($this->payload(), JSON_THROW_ON_ERROR),
        ];
    }

    /**
     * @return array<string, mixed>
     */
    public function payload(): array
    {
        $values = [];

        foreach ((new ReflectionClass($this))->getProperties(ReflectionProperty::IS_PUBLIC) as $property) {
            if ($property->isStatic()) {
                continue;
            }

            $values[$property->getName()] = $property->getValue($this);
        }

        return $values;
    }

    public static function fromEntry(Entry $entry): static
    {
        /** @var array<string, mixed> $payload */
        $payload = json_decode((string) $entry->get('payload', '{}'), true, 512, JSON_THROW_ON_ERROR);

        return new static(...$payload);
    }
}
