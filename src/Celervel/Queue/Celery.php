<?php namespace Celervel\Queue;

use PhpAmqpLib\Connection\AMQPConnection;
use PhpAmqpLib\Message\AMQPMessage;

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

        $this->config = array_merge($this->config, $config);

        //$this->broker = $this->getBroker();
        //$this->backend = $this->getBackend();

    }

    /**
     * Get an AMQP connection from stored settings
     * @return AMQPConnection
     */
	function getBroker()
	{
		return new AMQPConnection(
            $this->config['broker']['host'],
			$this->config['broker']['port'],
			$this->config['broker']['login'],
			$this->config['broker']['password'],
			$this->config['broker']['vhost']
		);
	}

    /**
     * Get a MongoDb connection from stored settings
     * @return MongoConnection
     */
	function getBackend()
	{
        $connection = new \MongoClient($this->config['backend']['host']);
        $db = $connection->selectDB($this->config['backend']['database']); 
        $collection = new \MongoCollection($db, $this->config['backend']['collection']); 
        return $collection;
	}

    /**
     * Post a task to Celery
     * @param string $task Name of the task, prefixed with module name (like tasks.add for function add() in task.py)
     * @param array $args Array of arguments (args, kwargs)
     * TODO: @return AsyncResult
     */
    function postTask($task, $args=[])
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

		$task_array = array_merge(
			array(
				'id' => $id,
				'task' => $task,
			),
			$args
		);

		$task_body = json_encode($task_array);
        $params = $this->buildParams();

        $ch = $this->declareTask($queue, $exchange, $routing_key);
		$msg = new AMQPMessage($task_body, $params);
		$ch->basic_publish($msg, $exchange, $routing_key);
		$ch->close();

        // TODO: Async result
        return new AsyncResult($id, $this);

    }

    /**
     * Declare queue/exchange/routing_key
     * @param string $queue Name of the queue
     * @param string $exchange Name of the exchange
     * @param string $routing_key Name of the routing key
     * @return channel
     */
    function declareTask($queue, $exchange, $routing_key)
    {
		$ch = $this->broker->channel();

		$ch->queue_declare(
			$queue,      	        /* queue name */
			false,					/* passive */
			true,					/* durable */
			false,					/* exclusive */
			false					/* auto_delete */
		);

		$ch->exchange_declare(
			$exchange,	            /* exchange name */
			'direct',				/* type */
			false,					/* passive */
			true,					/* durable */
			false					/* auto_delete */
		);

		$ch->queue_bind($queue, $exchange, $routing_key);

        return $ch;

    }

    /**
     * Build AMQP message params
     * @return array
     */
    function buildParams()
    {
        $params = array('content_type' => 'application/json',
			'content_encoding' => 'UTF-8',
			'immediate' => false,
		 );

        /*
		if($this->config['persistent_messages'])
		{
			$params['delivery_mode'] = 2;
        }
         */

        return $params;
    }

    /**
     * Get AsyncResult, return false if it doesn't exist
     * @return array || false
     */
    function getAsyncResult($task_id)
    {
        $backend = $this->getBackend();
        $decoded = false;
        $raw = $backend->findOne(['_id' => $task_id]);
        if ($raw) {
            $decoded = array(
                'task_id' => $raw['_id'],
                'status' => $raw['status'],
                'result' => json_decode($raw['result']->bin, true),
                'date_done' => $raw['date_done']->toDateTime(),
                'traceback' => json_decode($raw['traceback']->bin, true),
                'children' => json_decode($raw['children']->bin, true),
            );
        }
        return $decoded;
    }
}

/*
 * Asynchronous result of Celery task
 * @package celery-php
 */
class AsyncResult 
{
    protected $task_id; // string, queue name
    protected $celery; // Celery instance
    protected $body; // decoded array with message body (whatever Celery task returned)

    /**
     * Celery's AsyncResult result
     * @param string $id Task ID in Celery
     * @param celery $celery Celery instance
     */
    function __construct($id, $celery)
    {
        $this->task_id = $id;
        $this->celery = $celery;
    }

    /**
     * Connect to queue, see if there's a result waiting for us
     * Private - to be used internally
     */
    protected function getCompleteResult()
    {
        if($this->body)
        {
            return $this->body;
        }

        $this->body = $this->celery->getAsyncResult($this->task_id);
        
        return $this->body;message;
    }
    /**
     * Helper function to return current microseconds time as float 
     */
    static protected function getmicrotime()
    {
            list($usec, $sec) = explode(" ",microtime());
            return ((float)$usec + (float)$sec); 
    }
    /**
     * Get the Task Id
     * @return string
     */
     function getId()
     {
        return $this->task_id;
     }
    /**
     * Check if a task result is ready
     * @return bool
     */
    function isReady()
    {
        return ($this->getCompleteResult() !== false);
    }
    /**
     * Return task status (needs to be called after isReady() returned true)
     * @return string 'SUCCESS', 'FAILURE' etc - see Celery source
     */
    function getStatus()
    {
        if(!$this->body)
        {
            throw new CeleryException('Called getStatus before task was ready');
        }
        return $this->body['status'];
    }
    /**
     * Check if task execution has been successful or resulted in an error
     * @return bool
     */
    function isSuccess()
    {
        return($this->getStatus() == 'SUCCESS');
    }
    /**
     * If task execution wasn't successful, return a Python traceback
     * @return string
     */
    function getTraceback()
    {
        if(!$this->body)
        {
            throw new CeleryException('Called getTraceback before task was ready');
        }
        return $this->body['traceback'];
    }
    /**
     * Return a result of successful execution.
     * In case of failure, this returns an exception object
     * @return mixed Whatever the task returned
     */
    function getResult()
    {
        if(!$this->body)
        {
            throw new CeleryException('Called getResult before task was ready');
        }
        return $this->body['result'];
    }
    /****************************************************************************
     * Python API emulation                                                     *
     * http://ask.github.com/celery/reference/celery.result.html                *
     ****************************************************************************/
    /**
     * Returns TRUE if the task failed
     */
    function failed()
    {
        return $this->isReady() && !$this->isSuccess();
    }
    /**
     * Forget about (and possibly remove the result of) this task
     * Currently does nothing in PHP client
     */
    function forget()
    {
    }
    /**
     * Wait until task is ready, and return its result.
     * @param float $timeout How long to wait, in seconds, before the operation times out
     * @param bool $propagate (TODO - not working) Re-raise exception if the task failed.
     * @param float $interval Time to wait (in seconds) before retrying to retrieve the result
     * @throws CeleryTimeoutException on timeout
     * @return mixed result on both success and failure
     */
    function get($timeout=10, $propagate=TRUE, $interval=0.5)
    {
        $interval_us = (int)($interval * 1000000);
        $start_time = self::getmicrotime();
        while(self::getmicrotime() - $start_time < $timeout)
        {
                if($this->isReady())
                {
                        break;
                }
                usleep($interval_us);
        }
        if(!$this->isReady())
        {
                throw new CeleryTimeoutException(sprintf('AMQP task %s(%s) did not return after %d seconds', $this->task_name, json_encode($this->task_args), $timeout), 4);
        }
        return $this->getResult();
    }
    /**
     * Implementation of Python's properties: result, state/status
     */
    public function __get($property)
    {
        /**
         * When the task has been executed, this contains the return value. 
         * If the task raised an exception, this will be the exception instance.
         */
        if($property == 'result')
        {
            if($this->isReady())
            {
                return $this->getResult();
            }
            else
            {
                return NULL;
            }
        }
        /**
         * state: The tasks current state.
         *
         * Possible values includes:
         *
         * PENDING
         * The task is waiting for execution.
         *
         * STARTED
         * The task has been started.
         *
         * RETRY
         * The task is to be retried, possibly because of failure.
         *
         * FAILURE
         * The task raised an exception, or has exceeded the retry limit. The result attribute then contains the exception raised by the task.
         *
         * SUCCESS
         * The task executed successfully. The result attribute then contains the tasks return value.
         *
         * status: Deprecated alias of state.
         */
        elseif($property == 'state' || $property == 'status')
        {
            if($this->isReady())
            {
                return $this->getStatus();
            }
            else
            {
                return 'PENDING';
            }
        }
        return $this->$property;
    }
    /**
     * Returns True if the task has been executed.
     * If the task is still running, pending, or is waiting for retry then False is returned.
     */
    function ready()
    {
        return $this->isReady();
    }
    /**
     * Send revoke signal to all workers
     * Does nothing in PHP client
     */
    function revoke()
    {
    }
    /**
     * Returns True if the task executed successfully.
     */
    function successful()
    {
        return $this->isSuccess();
    }
    /**
     * Deprecated alias to get()
     */
    function wait($timeout=10, $propagate=TRUE, $interval=0.5)
    {
        return $this->get($timeout, $propagate, $interval);
    }
}
