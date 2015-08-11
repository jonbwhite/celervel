<?php namespace Celervel\Celery;

/*
 * Asynchronous result of Celery task
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

        $this->body = $this->celery->getBackend()->getResult($this->task_id);

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

