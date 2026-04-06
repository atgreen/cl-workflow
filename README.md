# cl-workflow

A Common Lisp durable workflow engine.

Workflows survive process crashes, retry activity failures with exponential backoff, and resume exactly where they left off via deterministic replay. Designed for agentic AI workflows, business process orchestration, and any long-running multi-step computation.

## Quick Start

```sh
# Install dependencies
ocicl install

# Load and use from the REPL
sbcl --eval '(asdf:load-system :cl-workflow)'
```

## Usage

### Define activities (side effects)

```lisp
(defactivity call-llm ((messages list))
  :retry-policy (:max-attempts 3 :initial-interval 2 :backoff-coefficient 2.0)
  :timeout 120
  (my-llm-client:complete messages))

(defactivity save-to-db ((key string) (value string))
  :retry-policy (:max-attempts 5 :initial-interval 1)
  (my-db:put key value))
```

### Define workflows (deterministic orchestration)

```lisp
(defworkflow summarize-and-store ((url string))
  "Fetch a URL, summarize it with an LLM, and store the result."
  (let* ((content (execute-activity 'fetch-url :input (list url)))
         (summary (execute-activity 'call-llm
                    :input (list `((:role "user"
                                    :content ,(format nil "Summarize: ~A" content)))))))
    (execute-activity 'save-to-db :input (list url summary))
    summary))
```

### Start the engine and run workflows

```lisp
(defvar *engine* (make-engine :db-path "/tmp/cl-workflow.db"))

;; Start a workflow
(start-workflow *engine* 'summarize-and-store
                :workflow-id "job-42"
                :input '("https://example.com/article"))

;; Send signals to running workflows
(signal-workflow *engine* "job-42" "user-feedback"
                 :payload '(:approved t))

;; Query live workflow state
(query-workflow *engine* "job-42" 'current-phase)

;; Check status
(get-workflow-status *engine* "job-42")  ; => "COMPLETED"

;; Inspect event history
(get-workflow-history *engine* "job-42")

;; Shut down
(stop-engine *engine*)
```

### Workflow commands

These are callable only inside `defworkflow` bodies:

| Function | Purpose |
|---|---|
| `(execute-activity name :input args)` | Run an activity with retry. Durable. |
| `(workflow-sleep seconds)` | Durable sleep that survives restarts. |
| `(workflow-receive signal-name :timeout N)` | Wait for an external signal. |
| `(workflow-now)` | Current time (deterministic on replay). |
| `(workflow-random &optional limit)` | Random number (deterministic on replay). |
| `(workflow-side-effect thunk)` | Arbitrary non-deterministic value (recorded). |
| `(setf (workflow-state key) val)` | Set queryable state. |
| `(workflow-state key)` | Read queryable state. |

### Queries

```lisp
(defquery my-workflow current-phase ()
  (workflow-state :phase))

;; Live query (against RUNNING workflow)
(query-workflow *engine* "job-42" 'current-phase)

;; Historical query (replays a completed run)
(query-workflow *engine* "job-42" 'current-phase :run-id "run-123")
```

## How It Works

**Deterministic replay.** Every workflow command (activity call, timer, signal receipt) is recorded as an event in SQLite. On process restart, the workflow function is re-invoked from the top. Completed events are served from history instead of re-executing. Execution resumes at the first incomplete step.

**One thread per workflow.** Each running workflow gets a dedicated thread. Commands like `execute-activity` block on a condition variable until the engine completes them. Simple and debuggable.

**At-least-once activities.** Activities may be retried after transient failures. Design activity implementations to be idempotent.

## Architecture

```
┌─────────────────────────────────┐
│         Lisp Image              │
│  ┌───────────┐  ┌───────────┐  │
│  │ Workflow   │  │ Activity  │  │
│  │ Workers    │  │ Workers   │  │
│  └─────┬─────┘  └─────┬─────┘  │
│        │               │        │
│  ┌─────▼───────────────▼─────┐  │
│  │         Engine            │  │
│  │  scheduler + state mgmt   │  │
│  └───────────┬───────────────┘  │
│              │                  │
│  ┌───────────▼───────────────┐  │
│  │   SQLite (WAL mode)       │  │
│  └───────────────────────────┘  │
└─────────────────────────────────┘
```

## Dependencies

| Library | Role |
|---|---|
| bordeaux-threads | Threading primitives |
| cl-conspack | Serialization of workflow data |
| clingon | CLI framework |
| local-time | Timer and deadline management |
| sqlite | Persistence backend |

## Status

v0.1 -- Embedded mode MVP.

## Author and License

cl-workflow was written by Anthony Green and is distributed under the terms of the MIT license.
