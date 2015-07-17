<?php namespace Celervel;

use Illuminate\Queue\Connectors\ConnectorInterface;

class CeleryConnector implements ConnectorInterface
{
    /**
     * Establish a queue connection.
     *
     * @param  array $config
     *
     * @return \Illuminate\Contracts\Queue\Queue
     */
    public function connect(array $config)
    {
        # $host, $login, $password, $vhost, $exchange='celery', $binding='celery', $port=5672, $connector=false, $persistent_messages=false, $result_expire=0, $ssl_options = array() 
        $defaultConfig = array(
            'exchange'              => 'celery', 
            'binding'               => 'celery', 
            'port'                  => 5672, 
            'connector'             => false, 
            'persistent_messages'   => false, 
            'result_expire'         => 0, 
            'ssl_options'            => array()
        );
        $config = array_merge($defaultConfig, $config);
        $celery = new Celery(  
            $config['host'], 
            $config['login'], 
            $config['password'],
            $config['vhost'], 
            $config['exchange'], 
            $config['binding'], 
            $config['port'], 
            $config['connector'], 
            $config['persistent_messages'], 
            $config['result_expire'], 
            $config['ssl_options']
        );

        return new CeleryQueue($celery, $config);
    }
}
