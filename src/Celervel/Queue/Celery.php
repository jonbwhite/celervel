<?php namespace Celervel\Queue;

class Celery extends \Celery
{
	protected $connection_details = array();
    function __construct($host, $login, $password, $vhost, $exchange='celery', $binding='celery', $port=5672, $connector=false, $persistent_messages=false, $result_expire=0, $ssl_options = array() )
    {
        parent::__construct($host, $login, $password, $vhost, $exchange, $binding, $port, $connector, $persistent_messages, $result_expire, $ssl_options);
        $this->default_connection_details = $this->connection_details;
    }
    /**
     * Post a task to Celery
     * @param string $task Name of the task, prefixed with module name (like tasks.add for function add() in task.py)
     * @param array $args Array of arguments (kwargs call when $args is associative)
     * @param bool $async_result Set to false if you don't need the AsyncResult object returned
     * @param string $routing_key Set to routing key name if you're using something other than "celery"
     * @param array $task_args Additional settings for Celery - normally not needed
     * @return AsyncResult
     */
    function PostTask($task, $args, $async_result=true, $routing_key="celery", $task_args=array())
    {
        // Allow tasks to specify queues and routing_key in the task details because celery-php is DUMB.  Yeah, that's right I said it Dumb with a capital D.
        if (isset($task_args['queue'])) {
            $this->connection_details['exchange'] = $task_args['queue'];
            $this->connection_details['binding'] = $task_args['queue'];
            unset($task_args['queue']);
        }
        if (isset($task_args['exchange'])) {
            $this->connection_details['exchange'] = $task_args['exchange'];
            unset($task_args['exchange']);
        }
        if (isset($task_args['routing_key'])) {
            $routing_key = $task_args['routing_key'];
            unset($task_args['routing_key']);
        }

        $result = parent::PostTask($task, $args, $async_result, $routing_key, $task_args);
        $this->connection_details = $this->default_connection_details;
        return $result;
    }
}
