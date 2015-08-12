<?php namespace Celervel\Celery;

/*
 * Celery object
 */
class Celery
{
    protected $config = array(); // array of strings required to connect

    function __construct($config)
    {
        $this->config = array(
            'default_exchange' => 'celery',
            'default_queue' => 'celery',
            'default_routing_key' => 'celery',
        );

        $backends = array(
            'mongo' => function($config) 
                {
                    return new \Celervel\Celery\Backends\MongoBackend($config);
                },
            );

        $brokers = array(
            'amqp' => function($config) 
                {
                    return new \Celervel\Celery\Brokers\AmqpBroker($config);
                },
            );

        $this->config = array_merge($this->config, $config);

        // TODO IoC containerize
        $this->broker = $brokers[$this->config['broker']['driver']]($this->config['broker']);
        $this->backend = $backends[$this->config['backend']['driver']]($this->config['backend']);

    }

    /**
     * Get an broker from stored settings
     * @return Broker
     */
    function getBroker()
    {
        return $this->broker;
    }

    /**
     * Get a MongoDb connection from stored settings
     * @return MongoConnection
     */
    function getBackend()
    {
        return $this->backend;
    }

    /**
     * Post a task to Celery
     * @param string $task Name of the task, prefixed with module name (like tasks.add for function add() in task.py)
     * @param array $args Array of arguments (args, kwargs)
     * @return AsyncResult
     */
    function sendTask($task, $args=[])
    {
        // Allow tasks to specify queue, exchange, and routing_key in the task details
        // TODO: should routing_key be a default param? also rename queue to queue
        $queue = $this->config['default_queue'];
        $exchange = $this->config['default_exchange'];
        $routing_key = $this->config['default_routing_key'];

        if (isset($args['queue'])) {
            $queue = $args['queue'];
            $exchange = $args['queue'];
            $routing_key = $args['queue'];
            unset($args['queue']);
        }
        if (isset($args['exchange'])) {
            $exchange = $args['exchange'];
            unset($args['exchange']);
        }
        if (isset($args['routing_key'])) {
            $routing_key = $args['routing_key'];
            unset($args['routing_key']);
        }

        $id = uniqid('php_', TRUE);
        if (isset($args['task_id'])) {
            $id = $args['task_id'];
            unset($args['task_id']);
        }

        $task_array = array_merge(
            array(
                'id' => $id,
                'task' => $task,
                'args' => array(),
                'kwargs' => (object) array(),
                'queue' => $queue,
                'exchange' => $exchange,
                'routing_key' => $routing_key,
            ),
            $args
        );

        $this->broker->sendTaskRaw($task_array);

        $result = new AsyncResult($id, $this);

        return $result;

    }

    function apply($signature) {
        $fun = $this->tasks[$signature['task']];
        $status = 'SUCCESS';
        $result = null;
        $traceback = null;
        try {
            $result = call_user_func_array($fun, $signature['args']);
        } catch (\Exception $e) {
            $traceback = debug_backtrace();
            $status = 'FAILED';
        }
        $this->backend->storeResult($signature['id'], $result, $status, $traceback);
        return $result;
    }


    /**
     * Implement pass-through properties
     */
    function __call($name, $args)
    {
        $obj = null;
        if (in_array($name, get_class_methods('\Celervel\Celery\Broker'))) {
            $obj = $this->broker;
        } else if (in_array($name, get_class_methods('\Celervel\Celery\Backend'))) {
            $obj = $this->backend;
        } else if (in_array($name, get_class_methods($this->broker))) {
            $obj = $this->broker;
        } else if (in_array($name, get_class_methods($this->backend))) {
            $obj = $this->backend;
        } else {
            $obj = $this->broker;
        }

        return call_user_func_array([$obj, $name], $args);
    }
}

