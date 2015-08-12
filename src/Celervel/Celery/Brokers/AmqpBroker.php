<?php namespace Celervel\Celery\Brokers;

use PhpAmqpLib\Connection\AMQPConnection;
use PhpAmqpLib\Message\AMQPMessage;
use Celervel\Celery\Broker;

class AmqpBroker extends Broker {

    protected $config = array(); // array of strings required to connect

    function __construct($config)
    {
        $this->config = array(
            'port' => 5672,
        );
        $this->config = array_merge($this->config, $config);

        $this->connection = $this->getConnection();

    }

    /**
     * Get an AMQP connection from stored settings
     * @return AMQPConnection
     */
    function getConnection()
    {
        return new AMQPConnection(
            $this->config['host'],
            $this->config['port'],
            $this->config['login'],
            $this->config['password'],
            $this->config['vhost']
        );
    }

    /**
     * Declare queue/exchange/routing_key
     * @param string $queue Name of the queue
     * @param string $exchange Name of the exchange
     * @param string $routing_key Name of the routing key
     * @return channel
     */
    function declareQueue($queue, $exchange, $routing_key)
    {
        $ch = $this->connection->channel();

        $ch->queue_declare(
            $queue,                 /* queue name */
            false,                  /* passive */
            true,                   /* durable */
            false,                  /* exclusive */
            false                   /* auto_delete */
        );

        $ch->exchange_declare(
            $exchange,              /* exchange name */
            'direct',               /* type */
            false,                  /* passive */
            true,                   /* durable */
            false                   /* auto_delete */
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
     * Post a task signature to Celery broker
     * @param array $signature Array of arguments (args, kwargs, etc)
     */
    function sendTaskRaw($signature)
    {
        $params = $this->buildParams();
        $queue = $signature['queue'];
        $exchange = $signature['exchange'];
        $routing_key = $signature['routing_key'];
        unset($signature['queue']);
        unset($signature['exchange']);
        unset($signature['routing_key']);
        $task_body = json_encode($signature, JSON_UNESCAPED_SLASHES);

        $ch = $this->declareQueue($queue, $exchange, $routing_key);
        $msg = new AMQPMessage($task_body, $params);
        $ch->basic_publish($msg, $exchange, $routing_key);
        $ch->close();
    }

    /**
     * Consume tasks from the broker
     * @return null | task signatures
     */
    function consume($queue, $exchange, $routing_key)
    {
        $ch = $this->declareQueue($queue, $exchange, $routing_key);
        $msg = $ch->basic_get($queue);
        if ($msg instanceof AMQPMessage) {
            return json_decode($msg->body, true);
        }
        return null;
    }
}
