<?php namespace Celervel\Queue;

use PhpAmqpLib\Connection\AMQPConnection;
use PhpAmqpLib\Message\AMQPMessage;

class Celery
{
	protected $connection_details = array(); // array of strings required to connect

    function __construct($host, $login, $password, $vhost, $exchange='celery', $queue='celery', $port=5672, $connector=false, $persistent_messages=false, $result_expire=0, $ssl_options = array() )
    {
		foreach(array('host', 'login', 'password', 'vhost', 'exchange', 'queue', 'port', 'connector', 'persistent_messages', 'result_expire', 'ssl_options') as $detail)
		{
			$this->connection_details[$detail] = $$detail;
		}
        $this->connection = $this->getConnection();

    }

    /**
     * Get an AMQP connection from stored settings
     * @return AMQPConnection
     */
	function getConnection()
	{
		return new AMQPConnection(
            $this->connection_details['host'],
			$this->connection_details['port'],
			$this->connection_details['login'],
			$this->connection_details['password'],
			$this->connection_details['vhost']
		);
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
        $queue = $this->connection_details['queue'];
        $exchange = $this->connection_details['exchange'];
        $routing_key = $this->connection_details['queue'];

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
		$ch = $this->connection->channel();

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

		if($this->connection_details['persistent_messages'])
		{
			$params['delivery_mode'] = 2;
        }

        return $params;
    }
}
