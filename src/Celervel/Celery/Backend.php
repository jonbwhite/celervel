<?php namespace Celervel\Celery;

/*
 * Backend object for Celery connection
 */
abstract class Backend
{
    protected $config = array(); // array of strings required to connect

    /**
     * Get AsyncResult, return false if it doesn't exist
     * @return array || false
     */
    abstract function getResult($task_id);
}

