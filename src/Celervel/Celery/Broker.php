<?php namespace Celervel\Celery;

/*
 * Broker object for Celery connection
 */
abstract class Broker
{
    protected $config = array(); // array of strings required to connect

    /**
     * Post a task signature to Celery
     * @param array $signature Array of arguments (args, kwargs)
     */
    abstract function sendTaskRaw($signature);

    /**
     * Consume tasks from the broker
     * @return null | task signatures
     */
    abstract function consume($queue, $exchange, $routing_key);

}

