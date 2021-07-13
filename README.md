# Celery ETA Scheduler
## Summary
An alternative way to schedule ETA/Countdown on Celery by piggybacking on the TTL functionality of Redis keys.

## The Problem
The native ETA solution in Celery send ETA tasks to workers right away, and relies on the worker to keep the task in memory, constantly looping over all the tasks in memory, once the ETA of the task has reached, it will execute it.     
When scheduling many tasks and tasks with different ETA time in the future, this can lead to high CPU usage on the workers and eventually an increasing gap between the scheduled execution time and the actual execution time.     
In addition, having varying ETA times(t+1 minute, t+2 hours), setting the **visibility timeout** parameter becomes tricky, often having to choose between duplicate execution of tasks and resiliency against worker failure.

## The Solution
