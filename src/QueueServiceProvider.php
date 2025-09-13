<?php

namespace MichaelLedin\LaravelQueueRateLimit;

use Illuminate\Cache\RateLimiter;
use Illuminate\Contracts\Debug\ExceptionHandler;
use Psr\Log\LoggerInterface;

class QueueServiceProvider extends \Illuminate\Queue\QueueServiceProvider
{
    public function register(): void
    {
        $this->registerLogger();
        parent::register();
    }

    protected function registerWorker(): void
    {
        $this->app->singleton('queue.worker', function () {
            $isDownForMaintenance = function (): bool {
                return $this->app->isDownForMaintenance();
            };

            return new Worker(
                $this->app['queue'],
                $this->app['events'],
                $this->app[ExceptionHandler::class],
                $isDownForMaintenance,
                $this->app['config']->get('queue.rateLimits'),
                $this->app[RateLimiter::class],
                $this->app['queue.logger']
            );
        });
    }

    protected function registerLogger(): void {
        $this->app->singleton('queue.logger', function () {
            return $this->app[LoggerInterface::class];
        });
    }
}
