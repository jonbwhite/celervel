<?php namespace Celervel;

use Illuminate\Support\ServiceProvider;


class CeleryQueueServiceProvider extends ServiceProvider {

    /**
     * Bootstrap the application events.
     *
     * @return void
     */
    public function boot()
    {
    }

    /**
     * Register the service provider.
     *
     * @return void
     */
    public function register()
    {
        $this->app->booted(function () {
            /**
             * @var \Illuminate\Queue\QueueManager $manager
             */
            $manager = $this->app['queue'];
            $manager->addConnector('celery', function () {
                return new CeleryConnector;
            });
        });
    }
}
