<?php namespace Celervel;

use Illuminate\Queue\Queue;
use Illuminate\Queue\QueueInterface;

class CeleryQueue extends Queue {

    /**
     * @param Celery         $celery
     * @param array          $config
     */
    public function __construct($celery, $config)
    {
        $this->connection = $celery;
        $this->config = $config;
    }

    /**
     * Push a raw payload onto the queue.
     *
     * @param  string $job
     * @param  array  $data
     * @param  string $queue
     * @param  array  $options
     *
     * @return mixed
     */
    public function pushRaw($payload, $queue = null, array $options = [])
    {
        // Set queue if supplied, but don't override if in data
        if ($queue && !isset($payload['queue'])) {
            $payload['queue'] = $queue;
        }

        // push job to a queue
        $task = $this->connection->PostTask($payload["task"], [], true, "celery", $payload);

        return $task;
    }

    /**
     * Create a payload string from the given job and data.
     *
     * @param  string  $job
     * @param  mixed   $data
     * @param  string  $queue
     * @return string
     */
    protected function createPayload($job, $data, $queue = null)
    {
        if ($job instanceof Closure)
        {
            throw new Exception("Not implemented");
        }
        // Set queue if supplied, but don't override if in data
        if ($queue && !isset($payload['queue'])) {
            $payload['queue'] = $queue;
        }
        $data['task'] = $job;
        return json_encode($data);
    }

    /**
     * Push a new job onto the queue.
     *
     * @param  string $job
     * @param  mixed  $data
     * @param  string $queue
     *
     * @return bool
     */
    public function push($job, $data = array(), $queue = null)
    {
        return $this->pushRaw($this->createPayload($job, $data), $queue, []);
    }

    /**
     * Push a new job onto the queue after a delay.
     *
     * @param  \DateTime|int $delay
     * @param  string        $job
     * @param  mixed         $data
     * @param  string        $queue
     *
     * @return mixed
     */
    public function later($delay, $job, $data = '', $queue = null)
    {
        throw new Exception("Not implemented");
    }

    /**
     * Pop the next job off of the queue.
     *
     * @param string|null $queue
     *
     * @return \Illuminate\Queue\Jobs\Job|null
     */
    public function pop($queue = null)
    {
        throw new Exception("Not implemented");
    }

}
