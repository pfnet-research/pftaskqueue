# pftaskqueue: Lightweight task queue tool. Feel free to process embarrassingly-parallel tasks at scale.

[![CI](https://github.com/pfnet-research/pftaskqueue/workflows/CI/badge.svg)](https://github.com/pfnet-research/pftaskqueue/actions?query=workflow%3ACI)

```
              +-------------------------+
+----+          +----+ +----+    +----+
|User+-----+--> |Task+-+Task|....|Task| +------+---------------------------------------+
+----+     |    +----+ +----+    +----+        |                                       |
           |  +-------------------------+      |                                       |
           |                                   |                                       |
           |                                   |                                       |
           |     +-----------------------------v-+       +-----------------------------v-+
           |     | pftaskqueue worker            |       | pftaskqueue worker            |
           |     |    +-----------------------+  |       |    +-----------------------+  |
           |     |    |-----+                 |  |       |    |-----+                 |  |
           |     |    ||Task| handler process |  |       |    ||Task| handler process |  |
           |     |    |-----+                 |  |       |    |-----+                 |  |
 Automatic |     |    +-----------------------+  | ....  |    +-----------------------+  |
     Retry |     |      ︙                       |       |       ︙                      |
           |     |    +-----------------------+  |       |    +-----------------------+  |
           |     |    |-----+                 |  |       |    |-----+                 |  |
           |     |    ||Task| handler process |  |       |    ||Task| handler process |  |
           |     |    |-----+                 |  |       |    |-----+                 |  |
           |     |    |-----------------------+  |       |    |-----------------------+  |
           |     +-+-----------------------------+       +--+----------------------------+
           |       |                                        |
           +-------+----------------------------------------+
```

<!-- toc -->

- [Feature](#feature)
- [Installation](#installation)
  * [Download released binary](#download-released-binary)
  * [Docker image](#docker-image)
  * [Build from the source](#build-from-the-source)
- [Getting Started](#getting-started)
  * [Example: `pftaskqueue` in Kubernetes cluster](#example-pftaskqueue-in-kubernetes-cluster)
- [Concepts](#concepts)
  * [Queue](#queue)
    + [Queue operations](#queue-operations)
  * [Tasks](#tasks)
    + [`TaskSpec`](#taskspec)
    + [`Task`](#task)
    + [Task lifecycle](#task-lifecycle)
    + [Task operations](#task-operations)
  * [Workers](#workers)
    + [Worker Configuration](#worker-configuration)
    + [Worker status](#worker-status)
    + [Worker lifecycle](#worker-lifecycle)
    + [Worker operations](#worker-operations)
- [Task handler specification](#task-handler-specification)
  * [Success/Failure](#successfailure)
  * [Input/Output](#inputoutput)
- [Dead letters](#dead-letters)
- [Managing configurations](#managing-configurations)
  * [Command line flags](#command-line-flags)
  * [Eenvironment variables](#eenvironment-variables)
  * [Config file](#config-file)
- [Backend configuration reference](#backend-configuration-reference)
  * [Redis](#redis)
- [Bash/Zsh completion](#bashzsh-completion)
- [Develop](#develop)
  * [Build](#build)
  * [Unit/Integration Test](#unitintegration-test)
  * [How to make release](#how-to-make-release)
- [License](#license)

<!-- tocstop -->

<!-- this toc is generated automatically in "make build" -->
<!-- by https://github.com/jonschlinkert/markdown-toc    -->

## Feature

- __Handy__: No dependency. Single binary.
- __Simple__: `pftaskqueue` fits to process _embarrassingly parallel tasks_ in distributed manner.  It can process bunch of task configurations with one single task handler commands for simplicity.
- __Scalable__: multiple `pftaskqueue` workers can consume single queue.
- __Reliable__: `pftaskqueue` never lost tasks even when worker exited abnormally.
- __Automatic Retry__: `pftaskqueue` can retry (re-queue) tasks with respect to its retry limit configuration if tasks failed.
- __Task Chaining__: `pftaskqueue` accepts some extra succeeding tasks (called `postHooks`) from task handler on their success/failure. `pftaskqueue` will queue them automatically.  This means your task handler can chain succeeding tasks programmatically. 
- __Automatic Worker Salvation__: `pftaskqueue` detects some lost workers (with timeout basis) and resurrects the tasks (re-queue) on th workers.
- __Suspend/Resume__: `pftaskqueue` can suspend/resume queue.  Once you suspend a queue, worker will stop pulling new tasks (you can configure to keep running too).  Once you resume a queue, worker will start pulling.
- __Backend Independent__: `pftaskqueue` supports multiple queue backend (to plan. now only supports `redis`).

## Installation

### Download released binary

Just download a tar ball for your platform from [releases](./../../releases) page, decompress the archive and place a binary to your `PATH`.

### Docker image

`pftaskqueue` publishes two kind of docker iamges in [GitHub Packages](https://github.com/pfnet-research/pftaskqueue/packages)

- `docker.pkg.github.com/pfnet-research/pftaskqueue/dev:latest`: built on the latest master branch
  - `docker.pkg.github.com/pfnet-research/pftaskqueue/dev:vx.y.z-alppha.n` is also available for old dev images
- `docker.pkg.github.com/pfnet-research/pftaskqueue/release:latest`: the latest release image
  -  `docker.pkg.github.com/pfnet-research/pftaskqueue/release:vx.y.z` is also available for previous/specific release images

To pull those images, you would need `docker login docker.pkg.github.com` first.  Please see [Configuring Docker for use with GitHub Packages](https://help.github.com/en/packages/using-github-packages-with-your-projects-ecosystem/configuring-docker-for-use-with-github-packages) if you are not familiar wit it.

```bash
$ docker pull docker.pkg.github.com/pfnet-research/pftaskqueue/release:latest
```

To build docker image by yourself:

```bash
docker build . -t YOUR_TAG
```

### Build from the source

```bash
# clone this repository
$ make

# copy pftaskqueue binary to anywhere you want
$ cp ./dist/pftaskqueue /anywhere/you/want
```

## Getting Started

1. Prepare Redis for sample backend (on default listen address `localhost:6379`)

```bash
# docker
$ docker run --name redis -d -p 6379:6379 redis redis-server --appendonly yes

# local on Mac (Homebrew)
$ brew install redis
$ redis-server
```

2. Please setup pftaksqueue configfile and set your redis key prefix

```bash
$ pftaskqueue print-default-config > ~/.pftaskqueue.yaml
```

If you shared the same redis DB among others, you must avoid key collisions.  As a safeguard, `pftaskqueue` redis backend must set key prefix.  Please see [Managing Configurations](#managing-configurations) section how to configure the option.  Let us use environment variable here.
 
```bash
export PFTQ_REDIS_KEYPREFIX=${___YOUR___USERNAME___}
```

3. Create a task queue and push your tasks

```bash
# create a queue
$ pftaskqueue create-queue foo
4:44PM INF Queue created successfully queueName=foo queueState=active queueUID=8aae8e25-998d-44e9-8ce8-7792c70e9c67

# add tasks to the queue ('-f -' means read from stdin)
$ cat << EOT | pftaskqueue add-task foo -f -
payload: payload
retryLimit: 0
timeoutSeconds: 100
EOT
4:45PM INF Task added to queue pipe=stdin queueName=foo queueUID=8aae8e25-998d-44e9-8ce8-7792c70e9c67 task={"spec":{"payload":"payload","timeoutSeconds":100},"status":{"failureCount":0,"phase":"Pending","salvageCount":0},"uid":"12333130-0b42-4724-b6d2-3a1c36ed3561"}
```

4. Start pftaskqueue worker

```bash
# start worker with task handler commands (you can stop the worker with Ctrl-C) 
# see 'Task Handler Specification' section for details.
$ pftaskqueue start-worker --name=$(hostname) --queue-name foo -- cat
4:45PM INF Start processing a task component=worker processUID=7cca2a80-155f-4527-ae61-02a8c3c03110 queueName=foo queueUID=8aae8e25-998d-44e9-8ce8-7792c70e9c67 taskHandlerCommands=["cat"] taskSpec={"payload":"payload","timeoutSeconds":100} taskUID=12333130-0b42-4724-b6d2-3a1c36ed3561 workerName=everpeace-macbookpro-2018.local workerUID=06c333f8-aeab-4a3d-8624-064a305c53ff
4:45PM INF "/tmp/06c333f8-aeab-4a3d-8624-064a305c53ff/7cca2a80-155f-4527-ae61-02a8c3c03110" pipe=stdout processUID=7cca2a80-155f-4527-ae61-02a8c3c03110 taskUID=12333130-0b42-4724-b6d2-3a1c36ed3561
4:45PM INF Task marked Succeeded component=worker processUID=7cca2a80-155f-4527-ae61-02a8c3c03110 queueName=foo queueUID=8aae8e25-998d-44e9-8ce8-7792c70e9c67 taskResult={"reason":"Succeeded","type":"Success"} taskSpec={"payload":"payload","timeoutSeconds":100} taskUID=12333130-0b42-4724-b6d2-3a1c36ed3561 workerName=everpeace-macbookpro-2018.local workerUID=06c333f8-aeab-4a3d-8624-064a305c53ff
^C4:45PM INF Signal received. Stopping all the on-going tasks signal=interrupt
4:45PM INF Completed processing configured number of tasks component=worker numTasks=100 queueName=foo queueUID=8aae8e25-998d-44e9-8ce8-7792c70e9c67 workerName=everpeace-macbookpro-2018.local workerUID=06c333f8-aeab-4a3d-8624-064a305c53ff
4:45PM INF Waiting for all the ongoing tasks finished component=worker queueName=foo queueUID=8aae8e25-998d-44e9-8ce8-7792c70e9c67 workerName=everpeace-macbookpro-2018.local workerUID=06c333f8-aeab-4a3d-8624-064a305c53ff
4:45PM INF Worker stopped component=worker queueName=foo queueUID=8aae8e25-998d-44e9-8ce8-7792c70e9c67 workerName=everpeace-macbookpro-2018.local workerUID=06c333f8-aeab-4a3d-8624-064a305c53ff
```

Please use `help` for detailed tool usage.

```bash
pftaskqueue help
```

### Example: `pftaskqueue` in Kubernetes cluster

`pftaskqueue` can use in kubernetes.  It recommends the below structure:

- `ConfigMap`: holds `pfatskqueue` configuration except for password.
  - __You should NOT include redis password in the configmap.  Use `Secret` instead.__
- `Job`: runs `pftaskqueue` workers .

Assume you have created some queue and the queue is active.
 
1. create `Secret` which holds redis secret

   ```bash
   $ kubectl create secret generic redis-password --from-literal=password =.......
   ```

1. creates `ConfigMap` which holds `pftaskqueue` configuration 

   ```bash
   $ pftaskqueue print-default-config > config.yaml
   
   # Edit the file 

   $ kubectl create configmap pftaskqueue-config --from-file=pftaskqueue.config=config.yaml
   ```

1. create worker `Job`

   ```yaml
   # job.yaml
   apiVersion: batch/v1
   kind: Job
   metadata:
     name: redis-job
   spec:
     # This will run 4 pods.  It will processes tasks with 4 * worker.concurrency concurrent task handlers. 
     parallelism: 4
     # The Job will recreate until 4 Pods succeeded
     completions: 4
     # The Job will give up retrying to create Pods with the limit 
     backoffLimit: 4
     template:
       spec:
         restartPolicy: Never
         containers:
           - name: ctr
             image: <your_container_which_includes_pftaskqueue>
             args:
               - start-worker
               - --config=/pftaskqueue-config/pftaskqueue.yaml
               - --exit-on-empty=true
               - --exit-on-suspend=true
             env:
                 # You can override configurations by envvars
               - name: PFTQ_WORKER_QUEUENAME
                 value: test
                 # pod name will be suitable for worker name
               - name: PFTQ_WORKER_NAME
                 valueFrom:
                   fieldRef:
                     fieldPath: metadata.name
                 # inject rerdis password 
               - name: PFTQ_REDIS_PASSWORD
                 valueFrom:
                   secretKeyRef:
                     name: redis-password
                     key: password
             resources:
               limits:
                 cpu: "1000m"
                 memory: "256Mi"
             # configmap as volume
             volumeMounts:
               - mountPath: /pftaskqueue-config
                 name: pftaskqueue-config
         volumes:
           - name: pftaskqueue-config
             configMap:
               name: pftaskqueue-config
   ``` 
   
   ```bash
   $ kubectl apply -f job.yaml
   ```

## Concepts

### Queue 

_see [queue.go](pkg/apis/taskqueue/queue.go) for full data structure._

```
    +--------+       +---------+
    | Active <-------> Suspend |
    +--------+       +---------+

 *Queue can be created in each state
```

`pftaskqueue`'s queue data model is very simple.  Each queue only has its name and state.  Queues are distinguished by its names. However, each queue has UID internally two distinguish two queues with different lifecycle but the same name.  State is one of `active` and `suspend`.  In `active` state,  `pftaskqueue` worker can consume tasks from the queue.  Otherwise (`suspend` state), `pftaskqueue` worker won't consume tasks. 

Every `Task` belongs to only single queue.  Once queue was deleted, all the information will be erased.

#### Queue operations

Below queue operation are supported.

```bash
$ pftaskqueue create-queue [queue] --state=[active|suspend]
$ pftaskqueue get-queue-state [queue] 
$ pftaskqueue suspend-queue [queue]
$ pftaskqueue resume-queue [queue] 
$ pftaskqueue delete-queue [queue]
$ pftaskqueue get-all-queues [queue] --output=[yaml,json,table]
```

Please remember `delete-queue` removes all the information from the backend.
 
### Tasks

_see [task.go](pkg/apis/task/task.go) for full data structure._

#### `TaskSpec`

You can specify any task payload in [`TaskSpec`](pkg/task/task.go). `TaskSpec` can convey any payload as blow.  Once you added `TaskSpec` to queue, `pftaskqueue` assigns UID for it automatically.  `pftasqueue` distinguishes tasks by the UIDs. See the next section for UID. Thus, if you queued multiple identical task specs, `pftaskqueue` recognizes them different tasks.

```yaml
# name is just a display name.  the field is NOT for identifying tasks.
# Tasks are identified by UID described in the next section.
# Max length is 1024.
name: "this is just an display name" 
# payload is the conveyor of task parameters.
# Max byte size varies on backend type to prevent from overloading backend.
#  redis: 1KB
payload: |
  You can define any task information in payload
# retryLimit is the maximum number of retry (negative number means infinite)
# NOTE: only the limited number of recent task records will be recorded in its status.
#       so, if you set large value or infinite here, you will loose old task records.
#       please see the description of status.history field in the next section.
retryLimit: 3
# timeoutSeconds is for task handler timeout.
# If set positive value, task handler timeout for processing this task.
# Otherwise, worker's default timeout will be applied. (See 'Task Queue Worker' section)
timeoutSeconds: 600
```

#### `Task`

`Task` holds various metadata for tracking task lifecycle. See inline comments below.

```yaml
# UID generated pftaskqueue for posted taskspec
uid: c7062138-fc38-4988-9f94-6e377d9855c3
# Posted TaskSpec
spec:
  name: myname
  payload: payload
  rertryLimit: 3
  timeoutSeconds: 100
status:
  # Phase of the task.
  # See below section for task lifecycle
  phase: Processing
  createdAt: 2020-02-12T20:20:29.350631+09:00
  # Failure count of the task
  failureCount: 1
  # Count the task was salvaged
  # Please see "Salvaging lost worker" section below for datails
  salvagedCount: 0
  # current processing task handler record
  currentRecord:
    processUID: 7b7b39f5-da66-4380-8002-033dff0e0f26
    # worker name received the task 
    workerName: everpeace-macbookpro-2018.local
    # This value is unique among pftaskqueue worker processes
    workerUID: 15bfe424-889a-49ca-88d7-fb0fc51f68d
    # timestamps
    receivedAt: 2020-02-12T20:20:39.350631+09:00
    startedAt: 2020-02-12T20:20:39.351479+09:00
  # history of recent records of processing the task.
  # the limited number of recent records are recorded in this field.
  # the value varies on backend types to prvent overloading backends:
  # - redis: 10 entries
  # NOTE: so, if you set larger value than this limit in spec.rertryLimit,
  #       you will loose old task records.
  history:
    # TaskRecord:
    #   this represents a record of task handler invocation
    #   in a specific worker.
    # UID of process(generated by pftaskqueue)
  - processUID: 194b8ad9-543b-4d01-a571-f8d1db2e74e6
    # worker name & UID which received the task
    workerName: everpeace-macbookpro-2018.local
    workerUID: 06c333f8-aeab-4a3d-8624-064a305c53ff
    # timestamps
    receivedAt: 2020-02-12T20:18:39.350631+09:00
    startedAt: 2020-02-12T20:18:39.351479+09:00
    finishedAt: 2020-02-12T20:18:39.365945+09:00
    # TaskResult:
    #   this represents a result of single task handler invocation
    result:
      # type is one of Success, Failure
      type: Success
      # reason is one of:
      #  - Success: success case
      #  - Failed: task handler failed
      #  - Timeout: task handler timeout (defined in spec)
      #  - Signaled: pftaskqueue worker signaled and the task handler was interrupted
      #  - InternalError: pftaskqueue worker faced some error processing a task
      reason: Succeeded
      # Returned values from the task handlers.
      # See 'Task Handler Specification' section for how pftaskqueue worker communicates
      # with its task handler processes.
      # payload max size varies on backend type to prevent from overloading backend.
      #  redis: 1KB
      # If size exceeded, the contents will be truncated automatically
      payload: ""
      # message max size varies on backend type to prevent from overloading backend.
      #  redis: 1KB
      # If size exceeded, the contents will be truncated automatically
      message: ""
      # Below two fields will be set if the worker which processes the task was
      # lost and salvaged by the other worker.
      # See "Worker lifecycle" section below for details.
      # salvagedBy: <workerUID>
      # salvagedAt: <timestamp>
```

#### Task lifecycle

```
+---------+    +----------+    +------------+      +-----------+
| Pending +--->+ Received +--->+ Processing +----->+ Succeeded |
+----^----+    +-------+--+    +-----+------+      +-----------+
     |                 |             |
     |                 |             |
     | Retry           |Retry        |Retry
     | Remaining       |Exceeded     |Exceeded     +--------+
     +-----------------+-------------+------------>+ Failed |
                                                   +--------+
```

If you queued your `TaskSpec`,  `pftaskqueue` assign UID to it and generate `Task` with `Pending` phase for it.  Some worker pulled a `Pending` task from the queue,  `Task` transits to `Received` phase.  When `Task` actually stared to be processed by task handler process,  it transits to `Processing` phase.

Once task handler process succeeded, `Task` transits to `Succeeded` phase.  If task handler process failed, `pftaskqueue` can handle automatic retry feature with respect to `TaskSpec.retryLimit`.  If the task handler process failed and it didn't reach at its retry limit,  `pftaskqueue` re-queue the task with setting `Pending` phase again.  Otherwise `pftaskqueue` will give up retry and mark it `Failed` phase.  You can see all the process record of the `Task` status.

If worker was signaled, tasks in `Received` or `Processing` phase will be treated as failure and `pftaskqueue` will handle automatic retry feature.

```yaml
$ pftaskqueue get-task [queue] --state=failed -o yaml
...
- uid: c5eff8d1-93f0-480b-81ed-89b50e89720c
  spec:
    name: myname
    payload: foo
    retryLimit: 1
    timeout: 10m
  status:
    phase: Failed
    createdAt: 2020-02-12T14:34:13.302071+09:00
    failureCount: 2
    history:
    - processUID: b8e9d028-0413-497a-b7df-27ab40b20a6b
      workerName: some.worker.name.you.set
      receivedAt: 2020-02-12T14:34:23.302071+09:00
      startedAt: 2020-02-12T14:34:23.302741+09:00
      finishedAt: 2020-02-12T14:34:23.303892+09:00
      result:
        type: Failure
        reason: Failed
        message: "..."
        payload: "..."
    - processUID: 417022f3-5dce-4125-afd1-47ed2480bfd8
      workerName: some.worker.name.you.set
      receivedAt: 2020-02-12T14:34:23.291144+09:00
      startedAt: 2020-02-12T14:34:23.291869+09:00
      finishedAt: 2020-02-12T14:34:23.301319+09:00
      result:
        type: Failure
        reason: Failed
        message: "..."
        payload: "..."
```

#### Task operations

Below operations are supported:

```bash
# this read taskspec in yaml format from stdin
$ pftaskqueue add-task [queue]

# this prints entries in yaml format
$ pftaskqueue get-task [queue] --state=[all,pending,completed,succeeded,failed,workerlost,deadletter] --output=[yaml,json,table]

# To be implemented
# $ pftaskqueue salvage-workerlost-tasks [queue]
```

See also [Dead Letter](#dead-letter) section.

### Workers

`pftaskqueue` worker is a queue consumer process.  This keeps fetching `Pending` tasks and spawns task handler processes up to configured concurrency and manage task lifecycle and its metadata. You can run multiple workers across multiple servers.

`pftaskqueue` worker can start with 

```bash
pftaskqueue start-worker -- [task handler commands]
```

#### Worker Configuration

You can configure worker behaviour by these parameters below. Please note that these parameter can be configured by cli flags, environment variables or config files. See [Managing Configurations](#managing-configurations) section below.

```yaml
# .pftaskqueue.yaml
...
worker:
  # Queue name to consume
  queueName: ""
  # Worker name
  #   This is used just to stamp into TaskRecord
  #   worker UID will be generated in each run
  name: ""
  # Concurrency of task handler processes
  concurrency: 1
  # TaskHandler configuration
  taskHandler:
    # Default timeout of the task handler.
    # This value will be used when TaskSpec.timeoutSeconds is not set or 0.
    defaultTimeout: 30m0s
    # Task Handler Command
    # A Worker spawns a process with the command for each received tasks
    commands:
    - cat
  # Worker heartbeat configuration to detect worker process existence
  # Please see "Worker lifecycle" section
  heartBeat:
    # A Worker process tries to update its Worker.Status.lastHeartBeatAt field
    # stored in queue backend in this interval
    interval: 2s
    # A Worker.Status.lastHeartBeatAt will be determined "expired"
    # when lastHeartBeatAt timestamp was not updated in this duration
    # Workers with expired heartbeat will be defined as "lost" state. (and exited)
    expirationDuration: 10s
    # The duration after heart beat expiration to make the worker target for salvation
    salvageDuration: 15s
  # If true, worker exits when the queue was suspended
  exitOnSuspend: true
  # If true, worker exits when the queue was empty
  exitOnEmpty: false
  # If exitOnEmpty is true, worker waits for exit in the grace period
  exitOnEmptyGracePeriod: 10s
  # If the value was positive, worker will exit
  # after processing the number of tasks
  numTasks: 1000
  # Base directory to create workspace for task handler processes
  workDir: /tmp
  # Worker normally to perform worker salvation on startup
  # this can limit the number of workers to salvage. -1 means all
  numWorkerSalvageOnStartup: -1
```

#### Worker status

Worker stores its information in the queue and you can query worker state.  See inline comments below:

```yaml
# The uid of the worker
# This will be assigned by pftaskqueue on the startup
uid: 06c333f8-aeab-4a3d-8624-064a305c53ff
# The queue UID which the worker can work on
queueUID: 8aae8e25-998d-44e9-8ce8-7792c70e9c67
spec:
...This is parameters described by the above section...
status:
  # Worker phase: one of Runinng, Succeeded or Failed
  # See the next section for Worker lifecycle
  phase: Succeeded
  # one of Success, Failure, Lost or Salvaged
  reason: Success
  # timestamps
  startedAt: 2020-02-17T16:45:43.979554+09:00
  finishedAt: 2020-02-17T16:45:46.534106+09:00
  # Last heartbeated timestamp
  # This is used to deternmine the worker process existence
  lastHeartBeatAt: 2020-02-17T16:45:46.534106+09:00
  # Below two fields will be set when the worker
  # was salvaged by other worker
  # salvagedBy: <workerUID>
  # salvagedAt: <timestamp>
```

#### Worker lifecycle

```
                       +-----------+
          +------------> Succeeded |
          |            +-----------+
     +----+----+       +--------+
+----> Running +-------> Failed |
     +----+----+       +--------+
          |            +--------+
          +------------> Failed |     Other worker salvages
    HeartBeat expired  | (Lost) |     after salvageDurationtion
                       +----+---+     from its expiration
                            |           +------------+
                            +----------->   Failed   |
                                        | (Salvaged) |
                                        +------------+
```

Once worker started, it starts with `Running` phase.  In the startup, a worker register self to the queue and get its UID. The UID becomes the identifier of workers.  If worker exited normally (with `exit-code=0`), it transits `Succeeded` phase. If `exit-code` was not 0, it transits to `Failed` phase.

However, worker process was go away by various reasons (`SIGKILL`-ed, `OOMKiller`, etc.).  Then, how to detect those worker's sate?  `pftaskqueue` applies simple timeout based heuristics.  A worker process keeps sending heartbeat during it runs, with configured interval, to the queue by updating its `Status.lastHeartBeatAt` field.  If the heartbeat became older then configured expiration duration,  the worker was determined as 'Lost' state (`phase=Failed, reason=Lost`).  Moreover when a worker detects their own heartbeat expired, they exited by their selves to wait they will be salvaged by other workers.

On every worker startup,  a worker tries to find `Lost` workers which are safe to be salvaged.  `pftaskqueue` also used simple timeout-based heuristics in salvation, too.  If time passed `Worker.HeartBeat.SalvagedDuration` after its heartbeat expiration, the worker is determined as a salvation target.  Once the worker finds some salvation target workers,  it will salvage the worker.  "Salvation" means

- marks the target `Salvaged` phase (`phase=Failed, reason=Salvaged`)
- re-queues all the non-Completed tasks to the pending queue which are handled in the target
  - it also stamp `salvagedAt, salvagedBy` fields in each `TaskRecord`

#### Worker operations

Most worker management are performed inside of `pfatakqueue`.  The allowed operation is seeing workers:

```
pftaskqueue get-worker [queue] --state=[all,running,succeeded,failed,lost,tosalvage] --output=[yaml,json,table]
```

## Task handler specification

`pftaskqueue` forks a subprocess to execute task handler commands for each received task.

### Success/Failure

`pftaskqueue` recognize success/failure of task handler invocations by its exit code.

- `0`: success
- otherwise: failure

### Input/Output

`pftaskqueue` communicates with task handler process via files.  `pftaskqueue` passes its workspace directory to `stdin` of task handler process.  It also defines `PFTQ_TASK_HANDLER_WORKSPACE_DIR` environment variable for each task handler process. The directory structure as below. All the inputs are also exported as environment variables whose names are prefixed with `PFTQ_TASK_HANDLER_INPUT_`.

```
┌ {workspace direcoty}             # pftaskqueue passes the dir name to stdin of task handler process
│                                  # also exported as PFTQ_TASK_HANDLER_WORKSPACE_DIR
│
│   # pftaskqueue prepares whole the contents
├── input
│   ├── payload                  # TaskSpec.payload in text format 
│   │　　                          # also exported as PFTQ_TASK_HANDLER_INPUT_PAYLOAD
│   ├── retryLimit               # TaskSpec.retryLimit in text format
│   │　　                          # also exported as PFTQ_TASK_HANDLER_INPUT_RETRY_LIMIT
│   ├── timeoutSeconds           # TaskSpec.timeoutSeconds in text format
│   │　　                          # also exported as PFTQ_TASK_HANDLER_INPUT_TIMEOUT_SECONDS
│   └── meta
│       ├── taskUID            # taskUID of the task in text format
│       │　　                    # also exported as PFTQ_TASK_HANDLER_INPUT_TASK_UID
│       ├── processUID         # prrocessUID of the task handler process
│       │　　                    # also exported as PFTQ_TASK_HANDLER_INPUT_PROCESS_UID
│       ├── task.json          # whole task information in JSON format
│       │　　                    # also exported as PFTQ_TASK_HANDLER_INPUT_TASK_JSON
│       ├── workerName         # workerName of the worker process
│       │　　                    # also exported as PFTQ_TASK_HANDLER_INPUT_WORKER_NAME
│       ├── workerUID          # workerUID of the worker process
│       │　　                    # also exported as PFTQ_TASK_HANDLER_INPUT_WORKER_UID
│       └── workerConfig.json  # whole workerConfig information in JSON format
│       　　　                    # also exported as PFTQ_TASK_HANDLER_INPUT_WORKER_CONFIG_JSON
│
│   # pftaskqueue just creates the directory
│   # If any error happened in reading files in the directory, the task fails with the TaskResult below.
│   #   type: "Failure"
│   #   reason: "InternalError"
│   #   message: "...error message..."
│   #   payload: null
└── output
    ├── payload                     # If the file exists, the contents will record in TaskResult.payload. Null otherwise.
    │　　                             # Max size of the payload varies on backend type to avoid from overloading backend
    │　　                             #   redis: 1KB
    │　　                             # If size exceeded, truncated contents will be recorded.
    ├── message                     # If the file exists, the contents will record in TaskResult.message. Null otherwise.
    │　　                             # Max size of the payload varies on backend type to avoid from overloading backend
    │　　                             #   redis: 1KB
    │　　                             # If size exceeded, truncated contents will be recorded.
    └── postHooks.json              # postHook TaskSpecs in JSON array format
    　　　                             # If the file exists, the contents will be queued automatically.
    　　　                             # e.g. [{"payload": "foo", "retryLimit": "3", "timeout": "10m"}]

3 directories, 12 files
```

## Dead letters

If `pftaskqueue` faced at some corrupted data for various reasons (e.g. manually edited backend data or etc.),  `pftaskqueue` delivers the corrupted data to dead letter queue.  Entries in dead letter queue is the form of:

```yaml
body: "...corrupted data...."
error: "...error message..."
```

Please note that dead letter item can't include task uid or specs generally because it should store any corrupted data.  If you are lucky, you can see task information in `body` section.

You can see dead letter items as below:

```bash
$ pftaskqueue get-task [queue] --state=deadletter --output yaml
- body: "..."
  err: "..."
...
```

## Managing configurations

`pftaskqueue` has a lot of configuration parameters. `pftaskqueue` provides multiple ways to configure them. `pftaskqueue` reads configuraton parameter in the following precedence order. Each item takes precedence over the item below it:

- Command line flags
- Environment variables
- Config file 

### Command line flags

See:

```bash
pftaskqueue --help
```

### Eenvironment variables

`pftaskqueue` reads environment variables prefixed with `PFTQ_`.  Environment variable names are derived from parameter key in the configuration file explained the below section.  If you want to set `redis.addr`,  please set `PFTQ_REDIS_ADDR`.  Please capitalize parameter key and replace `.` with `_`.

 __IMPORTANT__
 
Due to [the bug](https://github.com/spf13/viper/issues/584) of [viper](https://github.com/spf13/viper) used by`pftaskqueue`, setting configuration via environment variables works only the case when some config file.  In this case,  it recommends to create a config file in your home directory like this:

```bash
$ pftaskqueue print-default-config > ~/.pftaskqueue.yaml

# Then this works
$ PFTQ_REDIS_ADDR=... pftaskqueue ...
```

### Config file

`pftaskqueue` automatically reads `${HOME}/.pftaskqueue.yaml` if exists.
Or, you can also set any configuration path with `--config=${CONFIG_FILE_PATH}` flag or `PFTQCONFIG` environment variable.
`--config` flag is prioritized over `PFTQCONFIG` environment variable.

To generate config file with default values, please run `print-default-config` command.

```bash
$ pftaskqueue print-default-config > ${WHEREVER_YOU_WANT}
```

## Backend configuration reference

### Redis

see inline comments below,

```yaml
# backend type is 'redis' 
backend: redis
# all the configuration relates to redis 
redis:
  # key prefix of redis database
  # all the key used pftaskqueue was prefixed by '_pftaskqueue:{keyPrefix}:`
  keyPrefix: omura

  # redis server information(addr, password, db)
  addr: ""
  password: ""
  db: 0

  #
  # timeout/connection pool setting
  # see also: https://github.com/go-redis/redis/blob/a579d58c59af2f8cefbb7f90b8adc4df97f4fd8f/options.go#L59-L95
  #
  dialTimeout: 5s
  readTimeout: 3s
  writeTimeout: 3s
  poolSize: 0
  minIdleConns: 0
  maxConnAge: 0s
  poolTimeout: 4s
  idleTimeout: 5m0s
  idleCheckFrequency: 1m0s

  #
  # pftaskqueue will retry when redis operation failed
  # in exponential backoff manner.
  # you can configure backoff parameters below
  #
  backoff:
    initialInterval: 500ms
    randomizationFactor: 0.5
    multiplier: 1.2
    maxInterval: 1m0s
    maxElapsedTime: 1m0s
    # max retry count. -1 means no limit.
    maxRetry: -1
```

## Bash/Zsh completion

`pftaskqueue` provides bash/zsh completion.

```bash
# for bash
. <(pftaskqueue completion bash)

# for zsh
. <(pftaskqueue completion zsh)
```

To configure your shell to load completions for each session add to your rc file (`~/.bashrc` or `~/.zshrc`)

## Develop

### Build

```bash
# only for the first time
# $ make setup

$ make build
```

### Unit/Integration Test

You require `docker` in your environment.

```bash
make test
```

### How to make release

`release` target tags a commit and push the tag to `origin`.  Release process will run in GitHub Action.

```bash
$ RELEASE_TAG=$(git semv patch) # next patch version.  bump major/minor version if necessary
$ make release RELEASE=true RELEASE_TAG=${RELEASE_TAG}
```

## License

```
Copyright 2020 Preferred Networks, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0
```
