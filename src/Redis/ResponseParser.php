<?php

namespace Bhamner\RedisStreams\Redis;

class ResponseParser
{
    /**
     * @return list<array{id: string, fields: array<string, string>}>
     */
    public function entries(mixed $response): array
    {
        if (! is_array($response) || $response === []) {
            return [];
        }

        $entries = [];

        foreach ($response as $id => $fields) {
            if (is_string($id) && is_array($fields) && ! array_is_list($fields)) {
                $entries[] = ['id' => $id, 'fields' => $this->stringify($fields)];

                continue;
            }

            if (
                is_array($fields)
                && isset($fields[0], $fields[1])
                && is_string($fields[0])
                && is_array($fields[1])
            ) {
                $entries[] = [
                    'id' => $fields[0],
                    'fields' => $this->pairs($fields[1]),
                ];
            }
        }

        return $entries;
    }

    /**
     * PhpRedis XREADGROUP is [stream => entries]. Raw RESP is [[stream, entries]].
     *
     * @return list<array{id: string, fields: array<string, string>}>
     */
    public function read(mixed $response): array
    {
        if (! is_array($response) || $response === []) {
            return [];
        }

        $entries = [];

        foreach ($response as $stream => $messages) {
            if (is_string($stream)) {
                $entries = array_merge($entries, $this->entries($messages));

                continue;
            }

            if (is_array($messages) && isset($messages[1]) && is_array($messages[1])) {
                $entries = array_merge($entries, $this->entries($messages[1]));
            }
        }

        return $entries;
    }

    /**
     * @return array{count: int, min_id: ?string, max_id: ?string, consumers: array<string, int>}
     */
    public function pendingSummary(mixed $response): array
    {
        $empty = ['count' => 0, 'min_id' => null, 'max_id' => null, 'consumers' => []];

        if (! is_array($response) || $response === [] || $response === [0, null, null, null]) {
            return $empty;
        }

        $consumers = [];
        $rawConsumers = $response[3] ?? [];

        if (is_array($rawConsumers)) {
            if (array_is_list($rawConsumers)) {
                foreach ($rawConsumers as $consumer) {
                    if (is_array($consumer) && isset($consumer[0])) {
                        $consumers[(string) $consumer[0]] = (int) ($consumer[1] ?? 0);
                    }
                }
            } else {
                foreach ($rawConsumers as $name => $count) {
                    $consumers[(string) $name] = (int) $count;
                }
            }
        }

        return [
            'count' => (int) ($response[0] ?? 0),
            'min_id' => isset($response[1]) ? (string) $response[1] : null,
            'max_id' => isset($response[2]) ? (string) $response[2] : null,
            'consumers' => $consumers,
        ];
    }

    /**
     * @return list<array{id: string, consumer: string, idle: int, deliveries: int}>
     */
    public function pending(mixed $response): array
    {
        if (! is_array($response)) {
            return [];
        }

        $messages = [];

        foreach ($response as $row) {
            if (! is_array($row) || ! isset($row[0], $row[1])) {
                continue;
            }

            $messages[] = [
                'id' => (string) $row[0],
                'consumer' => (string) $row[1],
                'idle' => (int) ($row[2] ?? 0),
                'deliveries' => (int) ($row[3] ?? 1),
            ];
        }

        return $messages;
    }

    /**
     * @return array{next: string, entries: list<array{id: string, fields: array<string, string>}>}
     */
    public function autoClaim(mixed $response): array
    {
        if (! is_array($response)) {
            return ['next' => '0-0', 'entries' => []];
        }

        return [
            'next' => (string) ($response[0] ?? '0-0'),
            'entries' => $this->entries($response[1] ?? []),
        ];
    }

    /**
     * @return list<array{name: string, pending: int, idle: int}>
     */
    public function consumers(mixed $response): array
    {
        if (! is_array($response)) {
            return [];
        }

        $consumers = [];

        foreach ($response as $row) {
            if (! is_array($row)) {
                continue;
            }

            if (isset($row['name'])) {
                $consumers[] = [
                    'name' => (string) $row['name'],
                    'pending' => (int) ($row['pending'] ?? 0),
                    'idle' => (int) ($row['idle'] ?? 0),
                ];

                continue;
            }

            $pairs = $this->pairs($row);

            if ($pairs === []) {
                continue;
            }

            $consumers[] = [
                'name' => (string) ($pairs['name'] ?? ''),
                'pending' => (int) ($pairs['pending'] ?? 0),
                'idle' => (int) ($pairs['idle'] ?? 0),
            ];
        }

        return $consumers;
    }

    /**
     * @param  array<string, mixed>  $fields
     * @return array<string, string>
     */
    private function stringify(array $fields): array
    {
        $stringFields = [];

        foreach ($fields as $key => $value) {
            $stringFields[(string) $key] = (string) $value;
        }

        return $stringFields;
    }

    /**
     * @param  array<int|string, mixed>  $pairs
     * @return array<string, string>
     */
    private function pairs(array $pairs): array
    {
        if (! array_is_list($pairs)) {
            return $this->stringify($pairs);
        }

        $fields = [];

        for ($index = 0; $index < count($pairs); $index += 2) {
            $fields[(string) $pairs[$index]] = (string) ($pairs[$index + 1] ?? '');
        }

        return $fields;
    }
}
