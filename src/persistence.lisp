;;; persistence.lisp -- SQLite persistence layer
;;;
;;; SPDX-License-Identifier: MIT
;;;
;;; Copyright (C) 2026 Anthony Green

(in-package #:cl-workflow)

;;; ─── Schema ──��──────────────────────────────────────────────────────────────

(defparameter +schema-sql+
  '("CREATE TABLE IF NOT EXISTS workflow_runs (
       run_id        TEXT PRIMARY KEY,
       workflow_id   TEXT NOT NULL,
       workflow_type TEXT NOT NULL,
       task_queue    TEXT NOT NULL,
       status        TEXT NOT NULL,
       input         BLOB,
       result        BLOB,
       error_message TEXT,
       started_at    TEXT NOT NULL,
       closed_at     TEXT,
       memo          TEXT,
       deadline      INTEGER)"

    "CREATE UNIQUE INDEX IF NOT EXISTS idx_workflow_running
       ON workflow_runs(workflow_id) WHERE status = 'RUNNING'"

    "CREATE INDEX IF NOT EXISTS idx_workflow_id
       ON workflow_runs(workflow_id, started_at DESC)"

    "CREATE TABLE IF NOT EXISTS events (
       id         INTEGER PRIMARY KEY AUTOINCREMENT,
       run_id     TEXT NOT NULL REFERENCES workflow_runs(run_id),
       event_id   INTEGER NOT NULL,
       event_type TEXT NOT NULL,
       timestamp  TEXT NOT NULL,
       attributes BLOB,
       UNIQUE(run_id, event_id))"

    "CREATE TABLE IF NOT EXISTS activity_tasks (
       id            INTEGER PRIMARY KEY AUTOINCREMENT,
       run_id        TEXT NOT NULL REFERENCES workflow_runs(run_id),
       event_id      INTEGER NOT NULL,
       activity_type TEXT NOT NULL,
       task_queue    TEXT NOT NULL,
       input         BLOB,
       status        TEXT NOT NULL,
       attempt       INTEGER NOT NULL DEFAULT 1,
       result        BLOB,
       error_message TEXT,
       scheduled_at  TEXT NOT NULL,
       started_at    TEXT,
       completed_at  TEXT,
       next_retry_at TEXT)"

    "CREATE INDEX IF NOT EXISTS idx_activity_pending
       ON activity_tasks(task_queue, scheduled_at)
       WHERE status = 'PENDING'"

    "CREATE INDEX IF NOT EXISTS idx_activity_retry
       ON activity_tasks(next_retry_at)
       WHERE status = 'FAILED' AND next_retry_at IS NOT NULL"

    "CREATE TABLE IF NOT EXISTS timers (
       id       INTEGER PRIMARY KEY AUTOINCREMENT,
       run_id   TEXT NOT NULL REFERENCES workflow_runs(run_id),
       event_id INTEGER NOT NULL,
       fire_at  TEXT NOT NULL,
       fired    INTEGER NOT NULL DEFAULT 0)"

    "CREATE INDEX IF NOT EXISTS idx_timers_pending
       ON timers(fire_at) WHERE fired = 0"

    "CREATE TABLE IF NOT EXISTS signals (
       id          INTEGER PRIMARY KEY AUTOINCREMENT,
       run_id      TEXT NOT NULL REFERENCES workflow_runs(run_id),
       signal_name TEXT NOT NULL,
       payload     BLOB,
       received_at TEXT NOT NULL,
       consumed    INTEGER NOT NULL DEFAULT 0)"))

;;; ─── Database ─────────���─────────────────────────────────────────────────────

(defstruct db
  "SQLite persistence backend."
  (handle nil)
  (lock (bt:make-lock "cl-workflow-db")))

(defun make-sqlite-backend (path)
  "Open (or create) a SQLite database at PATH and initialize the schema."
  (let* ((handle (sqlite:connect path))
         (db (make-db :handle handle)))
    (sqlite:execute-non-query handle "PRAGMA journal_mode=WAL")
    (sqlite:execute-non-query handle "PRAGMA foreign_keys=ON")
    (sqlite:execute-non-query handle "PRAGMA busy_timeout=5000")
    (dolist (sql +schema-sql+)
      (sqlite:execute-non-query handle sql))
    db))

(defun close-db (db)
  "Close the database connection."
  (when (db-handle db)
    (sqlite:disconnect (db-handle db))
    (setf (db-handle db) nil)))

(defmacro with-db-lock ((db) &body body)
  "Execute BODY while holding the database lock."
  `(bt:with-lock-held ((db-lock ,db))
     ,@body))

(defmacro with-transaction ((db) &body body)
  "Execute BODY within a SQLite transaction, holding the DB lock."
  (let ((handle-var (gensym "HANDLE"))
        (committed-var (gensym "COMMITTED")))
    `(with-db-lock (,db)
       (let ((,handle-var (db-handle ,db))
             (,committed-var nil))
         (sqlite:execute-non-query ,handle-var "BEGIN IMMEDIATE")
         (unwind-protect
              (multiple-value-prog1
                  (progn ,@body)
                (sqlite:execute-non-query ,handle-var "COMMIT")
                (setf ,committed-var t))
           (unless ,committed-var
             (sqlite:execute-non-query ,handle-var "ROLLBACK")))))))

;;; ─── Timestamp Helpers ───────���──────────────────────────────────────────────

(defun now-iso8601 ()
  "Return current time as ISO 8601 string."
  (local-time:format-timestring nil (local-time:now)
                                :format local-time:+iso-8601-format+))

;;; ─── Workflow Runs ──��───────────────────────────────────────────────────────

(defun db-create-workflow-run (db run-id workflow-id workflow-type task-queue input
                               &key deadline)
  "Insert a new workflow run. Must be called within a transaction."
  (sqlite:execute-non-query
   (db-handle db)
   "INSERT INTO workflow_runs (run_id, workflow_id, workflow_type, task_queue, status, input, started_at, deadline)
    VALUES (?, ?, ?, ?, 'RUNNING', ?, ?, ?)"
   run-id workflow-id workflow-type task-queue
   (serialize input)
   (now-iso8601)
   deadline))

(defun db-complete-workflow-run (db run-id result)
  "Mark a workflow run as COMPLETED."
  (sqlite:execute-non-query
   (db-handle db)
   "UPDATE workflow_runs SET status = 'COMPLETED', result = ?, closed_at = ? WHERE run_id = ?"
   (serialize result)
   (now-iso8601)
   run-id))

(defun db-fail-workflow-run (db run-id error-message)
  "Mark a workflow run as FAILED."
  (sqlite:execute-non-query
   (db-handle db)
   "UPDATE workflow_runs SET status = 'FAILED', error_message = ?, closed_at = ? WHERE run_id = ?"
   error-message
   (now-iso8601)
   run-id))

(defun db-timeout-workflow-run (db run-id)
  "Mark a workflow run as TIMED_OUT."
  (sqlite:execute-non-query
   (db-handle db)
   "UPDATE workflow_runs SET status = 'TIMED_OUT', closed_at = ? WHERE run_id = ?"
   (now-iso8601)
   run-id))

(defun db-find-running-run (db workflow-id)
  "Find the RUNNING run for a workflow-id. Returns (run-id workflow-type task-queue input) or NIL."
  (with-db-lock (db)
    (let ((rows (sqlite:execute-to-list
                 (db-handle db)
                 "SELECT run_id, workflow_type, task_queue, input
                  FROM workflow_runs WHERE workflow_id = ? AND status = 'RUNNING'"
                 workflow-id)))
      (when rows
        (destructuring-bind (run-id wtype tq input) (first rows)
          (list :run-id run-id
                :workflow-type wtype
                :task-queue tq
                :input (deserialize input)))))))

(defun db-get-workflow-run (db run-id)
  "Get a workflow run by run-id."
  (with-db-lock (db)
    (let ((rows (sqlite:execute-to-list
                 (db-handle db)
                 "SELECT run_id, workflow_id, workflow_type, task_queue, status,
                         input, result, error_message, started_at, closed_at
                  FROM workflow_runs WHERE run_id = ?"
                 run-id)))
      (when rows
        (destructuring-bind (rid wid wtype tq status input result err started closed) (first rows)
          (list :run-id rid :workflow-id wid :workflow-type wtype
                :task-queue tq :status status
                :input (deserialize input) :result (deserialize result)
                :error-message err :started-at started :closed-at closed))))))

(defun db-list-workflow-runs (db &key workflow-id (limit 50))
  "List workflow runs, optionally filtered by workflow-id."
  (with-db-lock (db)
    (if workflow-id
        (sqlite:execute-to-list
         (db-handle db)
         "SELECT run_id, workflow_id, workflow_type, status, started_at, closed_at
          FROM workflow_runs WHERE workflow_id = ? ORDER BY started_at DESC LIMIT ?"
         workflow-id limit)
        (sqlite:execute-to-list
         (db-handle db)
         "SELECT run_id, workflow_id, workflow_type, status, started_at, closed_at
          FROM workflow_runs ORDER BY started_at DESC LIMIT ?"
         limit))))

;;; ─── Events ────────────────────────────────────────────────────────────���────

(defun db-append-event (db run-id event-id event-type attributes)
  "Append an event to a run's history. Must be called within a transaction."
  (sqlite:execute-non-query
   (db-handle db)
   "INSERT INTO events (run_id, event_id, event_type, timestamp, attributes)
    VALUES (?, ?, ?, ?, ?)"
   run-id event-id event-type (now-iso8601)
   (when attributes (serialize attributes))))

(defun db-get-event (db run-id event-id)
  "Get a specific event by run-id and event-id."
  (with-db-lock (db)
    (let ((rows (sqlite:execute-to-list
                 (db-handle db)
                 "SELECT event_type, attributes FROM events
                  WHERE run_id = ? AND event_id = ?"
                 run-id event-id)))
      (when rows
        (destructuring-bind (etype attrs) (first rows)
          (list :event-type etype
                :attributes (deserialize attrs)))))))

(defun db-load-event-history (db run-id &key include-external)
  "Load events for a run, ordered by event-id.
   By default, only loads replay-relevant events (event_id >= 0).
   With INCLUDE-EXTERNAL, also loads external events (SIGNAL_RECEIVED etc.)
   which have negative event_ids."
  (with-db-lock (db)
    (let ((rows (sqlite:execute-to-list
                 (db-handle db)
                 (if include-external
                     "SELECT event_id, event_type, timestamp, attributes
                      FROM events WHERE run_id = ? ORDER BY id"
                     "SELECT event_id, event_type, timestamp, attributes
                      FROM events WHERE run_id = ? AND event_id >= 0 ORDER BY event_id")
                 run-id)))
      (mapcar (lambda (row)
                (destructuring-bind (eid etype ts attrs) row
                  (list :event-id eid :event-type etype :timestamp ts
                        :attributes (deserialize attrs))))
              rows))))

;;; ─── Activity Tasks ─────────────────────────────────────────────────────────

(defun db-create-activity-task (db run-id event-id activity-type task-queue input)
  "Create a pending activity task. Must be called within a transaction."
  (sqlite:execute-non-query
   (db-handle db)
   "INSERT INTO activity_tasks (run_id, event_id, activity_type, task_queue, input, status, scheduled_at)
    VALUES (?, ?, ?, ?, ?, 'PENDING', ?)"
   run-id event-id activity-type task-queue
   (serialize input)
   (now-iso8601)))

(defun db-poll-pending-activities (db task-queue &key (limit 10))
  "Poll for pending activity tasks on a queue.
   Excludes tasks with a future next_retry_at (waiting for backoff)."
  (with-db-lock (db)
    (sqlite:execute-to-list
     (db-handle db)
     "SELECT id, run_id, event_id, activity_type, input, attempt
      FROM activity_tasks
      WHERE task_queue = ? AND status = 'PENDING'
        AND (next_retry_at IS NULL OR next_retry_at <= ?)
      ORDER BY scheduled_at LIMIT ?"
     task-queue (now-iso8601) limit)))

(defun db-claim-activity-task (db task-id)
  "Atomically claim a pending task by setting status to RUNNING.
   Returns T if the task was claimed, NIL if it was already claimed by another worker."
  (with-db-lock (db)
    ;; Check-then-update under the DB lock
    (let ((status (sqlite:execute-single
                   (db-handle db)
                   "SELECT status FROM activity_tasks WHERE id = ?"
                   task-id)))
      (when (and status (string= status "PENDING"))
        (sqlite:execute-non-query
         (db-handle db)
         "UPDATE activity_tasks SET status = 'RUNNING', started_at = ?
          WHERE id = ? AND status = 'PENDING'"
         (now-iso8601)
         task-id)
        t))))

(defun db-complete-activity-task (db task-id result)
  "Mark an activity task as completed with a result."
  (sqlite:execute-non-query
   (db-handle db)
   "UPDATE activity_tasks SET status = 'COMPLETED', result = ?, completed_at = ?
    WHERE id = ?"
   (serialize result)
   (now-iso8601)
   task-id))

(defun db-fail-activity-task (db task-id error-message attempt max-attempts
                              initial-interval backoff-coefficient max-interval)
  "Mark an activity task as failed. If retries remain, set next_retry_at and reset to PENDING."
  (if (< attempt max-attempts)
      (let* ((delay (min (* initial-interval (expt backoff-coefficient (1- attempt)))
                         max-interval))
             (retry-at (local-time:format-timestring
                        nil
                        (local-time:adjust-timestamp (local-time:now)
                          (offset :sec (ceiling delay)))
                        :format local-time:+iso-8601-format+)))
        (sqlite:execute-non-query
         (db-handle db)
         "UPDATE activity_tasks SET status = 'PENDING', error_message = ?,
                 attempt = ?, next_retry_at = ?, completed_at = NULL, started_at = NULL
          WHERE id = ?"
         error-message (1+ attempt) retry-at task-id)
        :retrying)
      (progn
        (sqlite:execute-non-query
         (db-handle db)
         "UPDATE activity_tasks SET status = 'FAILED', error_message = ?, completed_at = ?
          WHERE id = ?"
         error-message (now-iso8601) task-id)
        :exhausted)))

(defun db-poll-retry-activities (db &key (limit 10))
  "Poll for failed activities that are due for retry."
  (with-db-lock (db)
    (sqlite:execute-to-list
     (db-handle db)
     "SELECT id, run_id, event_id, activity_type, task_queue, input, attempt
      FROM activity_tasks
      WHERE status = 'PENDING' AND next_retry_at IS NOT NULL AND next_retry_at <= ?
      ORDER BY next_retry_at LIMIT ?"
     (now-iso8601)
     limit)))

;;; ─── Timers ────────────────────────────────��────────────────────────────────

(defun db-create-timer (db run-id event-id fire-at)
  "Create a timer. Must be called within a transaction."
  (sqlite:execute-non-query
   (db-handle db)
   "INSERT INTO timers (run_id, event_id, fire_at) VALUES (?, ?, ?)"
   run-id event-id fire-at))

(defun db-poll-due-timers (db &key (limit 50))
  "Poll for timers that should fire."
  (with-db-lock (db)
    (sqlite:execute-to-list
     (db-handle db)
     "SELECT id, run_id, event_id FROM timers
      WHERE fired = 0 AND fire_at <= ?
      ORDER BY fire_at LIMIT ?"
     (now-iso8601)
     limit)))

(defun db-fire-timer (db timer-id)
  "Mark a timer as fired."
  (sqlite:execute-non-query
   (db-handle db)
   "UPDATE timers SET fired = 1 WHERE id = ?"
   timer-id))

;;; ─── Signals ───────────────────────────────────────���────────────────────────

(defun db-create-signal (db run-id signal-name payload)
  "Store a signal for a run. Must be called within a transaction."
  (sqlite:execute-non-query
   (db-handle db)
   "INSERT INTO signals (run_id, signal_name, payload, received_at) VALUES (?, ?, ?, ?)"
   run-id signal-name
   (when payload (serialize payload))
   (now-iso8601)))

(defun db-find-unconsumed-signal (db run-id signal-name)
  "Find the oldest unconsumed signal matching run-id and signal-name."
  (let ((rows (sqlite:execute-to-list
               (db-handle db)
               "SELECT id, payload FROM signals
                WHERE run_id = ? AND signal_name = ? AND consumed = 0
                ORDER BY received_at LIMIT 1"
               run-id signal-name)))
    (when rows
      (destructuring-bind (id payload) (first rows)
        (list :id id :payload (deserialize payload))))))

(defun db-consume-signal (db signal-id)
  "Mark a signal as consumed."
  (sqlite:execute-non-query
   (db-handle db)
   "UPDATE signals SET consumed = 1 WHERE id = ?"
   signal-id))
