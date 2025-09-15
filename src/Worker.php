<?php

namespace MichaelLedin\LaravelQueueRateLimit;

use Illuminate\Cache\RateLimiter;
use Illuminate\Contracts\Debug\ExceptionHandler;
use Illuminate\Contracts\Events\Dispatcher;
use Illuminate\Contracts\Queue\Job;
use Illuminate\Queue\QueueManager;
use Psr\Log\LoggerInterface;

class Worker extends \Illuminate\Queue\Worker
{
    /**
     * The rate limits configuration
     */
    private array $rateLimits;

    /**
     * The rate limiter instance
     */
    private RateLimiter $rateLimiter;

    /**
     * The logger instance
     */
    private ?LoggerInterface $logger;

    /**
     * Create a new queue worker.
     */
    public function __construct(
        QueueManager $manager,
        Dispatcher $events,
        ExceptionHandler $exceptions,
        callable $isDownForMaintenance,
        ?array $rateLimits,
        RateLimiter $rateLimiter,
        ?LoggerInterface $logger
    ) {
        parent::__construct($manager, $events, $exceptions, $isDownForMaintenance);

        $this->rateLimits = $rateLimits ?? [];
        $this->rateLimiter = $rateLimiter;
        $this->logger = $logger;
    }

    protected function getNextJob($connection, $queue): ?Job
    {
        $job = null;
        foreach (explode(',', $queue) as $queue) {
            $rateLimit = $this->rateLimits[$queue] ?? null;
            if ($rateLimit) {
                if (!isset($rateLimit['allows']) || !isset($rateLimit['every'])) {
                    throw new \RuntimeException('Set "allows" and "every" fields for "' . $queue . '" rate limit.');
                }
                $this->log('Rate limit is set for queue ' . $queue);
                if ($this->rateLimiter->tooManyAttempts($queue, $rateLimit['allows'])) {
                    $availableIn = $this->rateLimiter->availableIn($queue);
                    $this->log('Rate limit is reached for queue ' . $queue . '. Next job will be started in ' . $availableIn . ' seconds');
                    continue;
                } else {
                    $this->log('Rate limit check is passed for queue ' . $queue);
                }
            } else {
                $this->log('No rate limit is set for queue ' . $queue . '.');
            }

            $job = parent::getNextJob($connection, $queue);
            if ($job) {
                if ($rateLimit) {
                    $this->rateLimiter->hit($queue, $rateLimit['every']);
                }
                $this->log('Running job ' . $job->getJobId() . ' on queue ' . $queue);
                break;
            } else {
                $this->log('No available jobs on queue ' . $queue);
            }
        }
        return $job;
    }


    private function log(string $message): void
    {
        if ($this->logger) {
            $this->logger->debug($message);
        }
    }
}
