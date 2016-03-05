<?php namespace Celervel\Celery\Backends;

use Celervel\Celery\Backend;

class MongoBackend extends Backend
{
    function __construct($config)
    {
        $this->config = array(
        );
        $this->config = array_merge($this->config, $config);

        $this->connection = new \MongoClient($this->config['host']);
        $this->db = $this->connection->selectDB($this->config['database']); 
        $this->collection = new \MongoCollection($this->db, $this->config['collection']); 
    }

    /**
     * Get AsyncResult, return false if it doesn't exist
     * @return array || false
     */
    function getResult($task_id)
    {
        $backend = $this->collection;
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

