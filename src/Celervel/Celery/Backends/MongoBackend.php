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

    /**
     * Post task results to the backend
     * @return result
     */
    function storeResult($task_id, $result, $status, $traceback=null, $request=null)
    {
        $meta = array(
            '_id' => $task_id,
            'status' => $status,
            'result' => new \MongoBinData(json_encode($result, JSON_UNESCAPED_SLASHES), 0),
            'date_done' => new \MongoDate(),
            'traceback' => new \MongoBinData(json_encode($traceback, JSON_UNESCAPED_SLASHES), 0),
            'children' => new \MongoBinData(json_encode($request, JSON_UNESCAPED_SLASHES), 0),
        );
        $this->collection->save($meta);

        return $result;
    }

}

