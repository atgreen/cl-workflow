;;; engine.lisp -- Top-level engine, workers, scheduler
;;;
;;; SPDX-License-Identifier: MIT
;;;
;;; Copyright (C) 2026 Anthony Green

(in-package #:cl-workflow)

;;; ─── Engine Struct ─────────────────────────────────────────────────────────

(defstruct (workflow-engine (:constructor %make-engine))
  "The workflow engine."
  (db nil)
  (running-p nil)
  (lock (bt:make-lock "engine"))
  ;; run-id -> workflow-context
  (contexts (make-hash-table :test 'equal))
  (scheduler-thread nil)
  (activity-threads nil)
  (activity-thread-count 4)
  (task-queues (list "default"))
  (shutdown-lock (bt:make-lock "engine-shutdown"))
  (shutdown-condvar (bt:make-condition-variable :name "engine-shutdown")))

(defun engine-db (engine)
  (workflow-engine-db engine))

(defun next-external-event-id (engine run-id)
  "Generate a negative event-id for external events (signals, etc.).
   These don't participate in replay but are recorded for history/debugging.
   Must be called while already holding the DB lock (within a transaction)."
  (let ((min-id (sqlite:execute-single
                 (db-handle (workflow-engine-db engine))
                 "SELECT COALESCE(MIN(event_id), 0) FROM events WHERE run_id = ? AND event_id < 0"
                 run-id)))
    (1- (or min-id 0))))

;;; ─── Public API ────────────────────────────────────────────────────────────

(defun make-engine (&key persistence db-path (activity-threads 4) (task-queues '("default")))
  "Create and start a workflow engine."
  (let* ((db (or persistence
                 (make-sqlite-backend (or db-path "cl-workflow.db"))))
         (engine (%make-engine :db db
                               :activity-thread-count activity-threads
                               :task-queues task-queues)))
    (start-engine engine)
    engine))

(defun join-thread-with-timeout (thread timeout-seconds)
  "Try to join THREAD, giving up after TIMEOUT-SECONDS.
   Returns T if joined, NIL if timed out."
  (let ((deadline (+ (get-internal-real-time)
                     (ceiling (* timeout-seconds internal-time-units-per-second)))))
    (loop
      (unless (bt:thread-alive-p thread)
        (ignore-errors (bt:join-thread thread))
        (return t))
      (when (>= (get-internal-real-time) deadline)
        (return nil))
      (sleep 0.1))))

(defun stop-engine (engine &key (timeout 30))
  "Stop the engine, draining in-flight work up to TIMEOUT seconds."
  (setf (workflow-engine-running-p engine) nil)
  ;; Wake the scheduler so it can exit its loop
  (bt:with-lock-held ((workflow-engine-shutdown-lock engine))
    (bt:condition-notify (workflow-engine-shutdown-condvar engine)))
  ;; Wake any blocked workflow threads so they can notice shutdown
  (bt:with-lock-held ((workflow-engine-lock engine))
    (maphash (lambda (run-id ctx)
               (declare (ignore run-id))
               (ignore-errors
                 (bt:with-lock-held ((workflow-context-lock ctx))
                   (bt:condition-notify (workflow-context-condvar ctx)))))
             (workflow-engine-contexts engine)))
  ;; Join threads with a shared deadline
  (let ((per-thread-timeout (max 1 (/ timeout
                                      (+ 1  ; scheduler
                                         (length (workflow-engine-activity-threads engine)))))))
    (when (workflow-engine-scheduler-thread engine)
      (join-thread-with-timeout (workflow-engine-scheduler-thread engine) per-thread-timeout)
      (setf (workflow-engine-scheduler-thread engine) nil))
    (dolist (th (workflow-engine-activity-threads engine))
      (join-thread-with-timeout th per-thread-timeout))
    (setf (workflow-engine-activity-threads engine) nil))
  (close-db (workflow-engine-db engine)))

;;; ─── Engine Startup ────────────────────────────────────────────────────────

(defun start-engine (engine)
  (setf (workflow-engine-running-p engine) t)
  (setf (workflow-engine-scheduler-thread engine)
        (bt:make-thread (lambda () (scheduler-loop engine))
                        :name "cl-workflow-scheduler"))
  (setf (workflow-engine-activity-threads engine)
        (loop for i below (workflow-engine-activity-thread-count engine)
              collect (bt:make-thread
                       (lambda () (activity-worker-loop engine))
                       :name (format nil "cl-workflow-activity-~D" i))))
  (resume-running-workflows engine))

(defun resume-running-workflows (engine)
  "Resume any RUNNING workflows from the database on startup."
  (let ((rows (with-db-lock ((workflow-engine-db engine))
                (sqlite:execute-to-list
                 (db-handle (workflow-engine-db engine))
                 "SELECT run_id, workflow_id, workflow_type, task_queue, input, deadline
                  FROM workflow_runs WHERE status = 'RUNNING'"))))
    (dolist (row rows)
      (destructuring-bind (run-id workflow-id workflow-type task-queue input deadline) row
        (handler-case
            (progn
              (find-workflow workflow-type)
              (launch-workflow-thread engine run-id workflow-id workflow-type
                                      task-queue (deserialize input) t deadline))
          (error (e)
            (log-message "Failed to resume run ~A: ~A" run-id e)
            (with-transaction ((workflow-engine-db engine))
              (db-fail-workflow-run (workflow-engine-db engine) run-id
                                   (format nil "Resume failed: ~A" e)))))))))

;;; ─── Start Workflow ────────────────────────────────────────────────────────

(defun generate-run-id ()
  (format nil "run-~A-~A" (get-universal-time) (random 1000000)))

(defun start-workflow (engine workflow-name &key workflow-id task-queue input execution-timeout)
  "Start a new workflow run. Returns the run-id.
   EXECUTION-TIMEOUT, if provided, is the number of seconds before the
   workflow is forcibly transitioned to TIMED_OUT."
  (find-workflow workflow-name)
  (let* ((wid (or workflow-id (format nil "wf-~A" (generate-run-id))))
         (tq (or task-queue "default"))
         (run-id (generate-run-id))
         (wtype-str (symbol-name workflow-name))
         (deadline (when execution-timeout
                     (+ (get-universal-time) execution-timeout))))
    (with-transaction ((workflow-engine-db engine))
      (db-create-workflow-run (workflow-engine-db engine)
                              run-id wid wtype-str tq input
                              :deadline deadline)
      (db-append-event (workflow-engine-db engine)
                       run-id 0 "WORKFLOW_STARTED"
                       (list :workflow-type wtype-str :input input
                             :execution-timeout execution-timeout)))
    (launch-workflow-thread engine run-id wid workflow-name tq input nil deadline)
    run-id))

(defun launch-workflow-thread (engine run-id workflow-id workflow-type task-queue input replay-p
                               &optional deadline)
  (let* ((history (when replay-p
                    (db-load-event-history (workflow-engine-db engine) run-id)))
         (ctx (make-workflow-context
                :engine engine
                :run-id run-id
                :workflow-id workflow-id
                :workflow-type workflow-type
                :task-queue task-queue
                :event-counter 0
                :history history
                :history-length (length history)
                :deadline deadline)))
    (bt:with-lock-held ((workflow-engine-lock engine))
      (setf (gethash run-id (workflow-engine-contexts engine)) ctx))
    (bt:make-thread
     (lambda () (run-workflow engine ctx workflow-type input))
     :name (format nil "wf-~A" run-id))))

(defun run-workflow (engine ctx workflow-type input)
  "Execute a workflow function on its dedicated thread."
  (let ((*workflow-context* ctx))
    (handler-case
        (let* ((wdef (find-workflow workflow-type))
               (result (apply (workflow-def-function wdef)
                              (if (listp input) input (list input)))))
          (with-transaction ((workflow-engine-db engine))
            (db-append-event (workflow-engine-db engine)
                             (workflow-context-run-id ctx)
                             (next-event-id ctx)
                             "WORKFLOW_COMPLETED"
                             (list :result result))
            (db-complete-workflow-run (workflow-engine-db engine)
                                     (workflow-context-run-id ctx) result)))
      (workflow-execution-timeout ()
        ;; Already handled by check-execution-timeouts -- just exit the thread.
        nil)
      (error (e)
        (with-transaction ((workflow-engine-db engine))
          (db-append-event (workflow-engine-db engine)
                           (workflow-context-run-id ctx)
                           (next-event-id ctx)
                           "WORKFLOW_FAILED"
                           (list :error (format nil "~A" e)))
          (db-fail-workflow-run (workflow-engine-db engine)
                               (workflow-context-run-id ctx)
                               (format nil "~A" e))))))
  ;; Unregister context
  (bt:with-lock-held ((workflow-engine-lock engine))
    (remhash (workflow-context-run-id ctx)
             (workflow-engine-contexts engine))))

;;; ─── Engine Callbacks (from workflow commands) ─────────────────────────────

(defun engine-schedule-activity (engine ctx event-id name name-str task-queue input activity-def)
  "Schedule an activity from within a workflow."
  (declare (ignore name activity-def))
  (with-transaction ((workflow-engine-db engine))
    (db-append-event (workflow-engine-db engine)
                     (workflow-context-run-id ctx)
                     event-id "ACTIVITY_SCHEDULED"
                     (list :name name-str :input input :task-queue task-queue))
    (db-create-activity-task (workflow-engine-db engine)
                             (workflow-context-run-id ctx)
                             event-id name-str task-queue input)))

(defun engine-wait-timer (engine ctx event-id seconds already-started-p)
  "Create a durable timer and block the workflow."
  (let ((fire-at (local-time:format-timestring
                  nil
                  (local-time:adjust-timestamp (local-time:now) (offset :sec seconds))
                  :format local-time:+iso-8601-format+)))
    (unless already-started-p
      (with-transaction ((workflow-engine-db engine))
        (db-append-event (workflow-engine-db engine)
                         (workflow-context-run-id ctx) event-id
                         "TIMER_STARTED"
                         (list :seconds seconds :fire-at fire-at))
        (db-create-timer (workflow-engine-db engine)
                         (workflow-context-run-id ctx) event-id fire-at))))
  (block-workflow ctx))

(defun engine-wait-signal (engine ctx event-id signal-name timeout)
  "Set up signal waiting, check for existing signal, or block.
   Sets waiting-for-signal so signal-workflow can find us."
  (when timeout
    (let ((fire-at (local-time:format-timestring
                    nil
                    (local-time:adjust-timestamp (local-time:now) (offset :sec timeout))
                    :format local-time:+iso-8601-format+)))
      (with-transaction ((workflow-engine-db engine))
        (db-append-event (workflow-engine-db engine)
                         (workflow-context-run-id ctx) event-id
                         "TIMER_STARTED"
                         (list :signal-name signal-name :seconds timeout :fire-at fire-at))
        (db-create-timer (workflow-engine-db engine)
                         (workflow-context-run-id ctx) event-id fire-at))))
  ;; Check for already-arrived signal (sent before we started waiting)
  (let ((sig (with-db-lock ((workflow-engine-db engine))
               (db-find-unconsumed-signal (workflow-engine-db engine)
                                          (workflow-context-run-id ctx)
                                          signal-name))))
    (when sig
      (let ((result-event-id (next-event-id ctx)))
        (with-transaction ((workflow-engine-db engine))
          (db-consume-signal (workflow-engine-db engine) (getf sig :id))
          (db-append-event (workflow-engine-db engine)
                           (workflow-context-run-id ctx) result-event-id
                           "SIGNAL_CONSUMED"
                           (list :signal-name signal-name
                                 :payload (getf sig :payload))))
        (wake-workflow ctx :signal-consumed (getf sig :payload))
        (return-from engine-wait-signal))))
  ;; No signal yet. Mark what we're waiting for so signal-workflow can find us.
  (setf (workflow-context-waiting-for-signal ctx) signal-name))

;;; ─── Scheduler ─────────────────────────────────────────────────────────────

(defun scheduler-loop (engine)
  (loop while (workflow-engine-running-p engine) do
    (handler-case
        (progn
          (fire-due-timers engine)
          (check-execution-timeouts engine)
          ;; Sleep briefly
          (bt:with-lock-held ((workflow-engine-shutdown-lock engine))
            (bt:condition-wait (workflow-engine-shutdown-condvar engine)
                               (workflow-engine-shutdown-lock engine)
                               :timeout 0.5)))
      (error (e)
        (log-message "Scheduler error: ~A" e)))))

(defun fire-due-timers (engine)
  (let ((timers (db-poll-due-timers (workflow-engine-db engine))))
    (dolist (timer timers)
      (destructuring-bind (timer-id run-id event-id) timer
        (declare (ignore event-id))
        (let ((ctx (bt:with-lock-held ((workflow-engine-lock engine))
                     (gethash run-id (workflow-engine-contexts engine)))))
          (when ctx
            (with-transaction ((workflow-engine-db engine))
              (db-fire-timer (workflow-engine-db engine) timer-id)
              (db-append-event (workflow-engine-db engine)
                               run-id (next-event-id ctx)
                               "TIMER_FIRED" nil))
            ;; Clear signal wait state if this timer was for a workflow-receive timeout
            (setf (workflow-context-waiting-for-signal ctx) nil)
            (wake-workflow ctx :timer-fired nil)))))))

(defun check-execution-timeouts (engine)
  "Check for workflows that have exceeded their execution timeout."
  (let ((now (get-universal-time))
        (timed-out nil))
    ;; Collect timed-out runs (can't modify hash table during maphash)
    (bt:with-lock-held ((workflow-engine-lock engine))
      (maphash
       (lambda (run-id ctx)
         (let ((deadline (workflow-context-deadline ctx)))
           (when (and deadline (> now deadline))
             (push (cons run-id ctx) timed-out))))
       (workflow-engine-contexts engine)))
    ;; Process them
    (dolist (entry timed-out)
      (destructuring-bind (run-id . ctx) entry
        (log-message "Workflow run ~A exceeded execution timeout" run-id)
        (handler-case
            (progn
              (with-transaction ((workflow-engine-db engine))
                (db-append-event (workflow-engine-db engine) run-id
                                 (next-event-id ctx) "WORKFLOW_TIMED_OUT"
                                 (list :reason "Execution timeout exceeded"))
                (db-timeout-workflow-run (workflow-engine-db engine) run-id))
              ;; Remove from contexts
              (bt:with-lock-held ((workflow-engine-lock engine))
                (remhash run-id (workflow-engine-contexts engine)))
              ;; Wake the workflow thread with an error so it exits
              (wake-workflow-error
               ctx
               (make-condition 'workflow-execution-timeout
                               :workflow-id (workflow-context-workflow-id ctx)
                               :run-id run-id)))
          (error (e)
            (log-message "Error handling timeout for ~A: ~A" run-id e)))))))

;;; ─── Activity Worker ───────────────────────────────────────────────────────

(defun activity-worker-loop (engine)
  (loop while (workflow-engine-running-p engine) do
    (handler-case
        (let ((did-work nil))
          (dolist (tq (workflow-engine-task-queues engine))
            (let ((tasks (db-poll-pending-activities
                          (workflow-engine-db engine) tq :limit 1)))
              (dolist (task tasks)
                (destructuring-bind (task-id run-id event-id activity-type input attempt) task
                  (declare (ignore event-id))
                  (when (db-claim-activity-task (workflow-engine-db engine) task-id)
                    (setf did-work t)
                    (execute-activity-task engine task-id run-id
                                           activity-type input attempt))))))
          (unless did-work
            (sleep 0.25)))
      (error (e)
        (log-message "Activity worker error: ~A" e)
        (sleep 1)))))

(defun execute-activity-task (engine task-id run-id activity-type input attempt)
  (let* ((adef (handler-case (find-activity activity-type)
                 (unknown-activity-error ()
                   (log-message "Unknown activity ~A" activity-type)
                   (return-from execute-activity-task))))
         (activity-name (activity-def-name adef))
         (policy (activity-def-retry-policy adef))
         (deserialized-input (deserialize input)))
    (handler-case
        (let ((result (apply (activity-def-function adef)
                             (if (listp deserialized-input)
                                 deserialized-input
                                 (list deserialized-input)))))
          ;; Success
          (let ((ctx (bt:with-lock-held ((workflow-engine-lock engine))
                       (gethash run-id (workflow-engine-contexts engine)))))
            (with-transaction ((workflow-engine-db engine))
              (db-complete-activity-task (workflow-engine-db engine) task-id result)
              (when ctx
                (db-append-event (workflow-engine-db engine) run-id
                                 (next-event-id ctx) "ACTIVITY_COMPLETED"
                                 (list :name activity-type :result result))))
            (when ctx
              (wake-workflow ctx :activity-completed result))))
      (error (e)
        (let* ((error-msg (format nil "~A" e))
               (non-retryable-p
                 (some (lambda (etype) (typep e etype))
                       (retry-policy-non-retryable-error-types policy)))
               (outcome
                 (if non-retryable-p
                     (progn
                       (with-db-lock ((workflow-engine-db engine))
                         (sqlite:execute-non-query
                          (db-handle (workflow-engine-db engine))
                          "UPDATE activity_tasks SET status='FAILED', error_message=?, completed_at=? WHERE id=?"
                          error-msg (now-iso8601) task-id))
                       :exhausted)
                     (with-db-lock ((workflow-engine-db engine))
                       (db-fail-activity-task
                        (workflow-engine-db engine) task-id error-msg attempt
                        (retry-policy-max-attempts policy)
                        (retry-policy-initial-interval policy)
                        (retry-policy-backoff-coefficient policy)
                        (retry-policy-max-interval policy))))))
          (when (eq outcome :exhausted)
            (let ((ctx (bt:with-lock-held ((workflow-engine-lock engine))
                         (gethash run-id (workflow-engine-contexts engine)))))
              (when ctx
                (with-transaction ((workflow-engine-db engine))
                  (db-append-event (workflow-engine-db engine) run-id
                                   (next-event-id ctx) "ACTIVITY_FAILED"
                                   (list :name activity-type
                                         :attempts attempt
                                         :error-message error-msg)))
                (wake-workflow-error
                 ctx
                 (make-condition 'activity-failure
                                 :activity-name activity-name
                                 :attempts attempt
                                 :last-error error-msg))))))))))

;;; ─── Signal Delivery ───────────────────────────────────────────────────────

(defun signal-workflow (engine workflow-id signal-name &key payload)
  "Send a signal to the currently RUNNING run of workflow-id.
   Records SIGNAL_RECEIVED. If the workflow is blocked in workflow-receive
   for this signal, also consumes it and wakes the workflow."
  (let ((run-info (db-find-running-run (workflow-engine-db engine) workflow-id)))
    (unless run-info
      (error 'no-running-workflow-error :workflow-id workflow-id))
    (let* ((run-id (getf run-info :run-id))
           (ctx (bt:with-lock-held ((workflow-engine-lock engine))
                  (gethash run-id (workflow-engine-contexts engine)))))
      ;; Store signal and record SIGNAL_RECEIVED atomically.
      ;; SIGNAL_RECEIVED is an external event (not a workflow command), so it
      ;; uses a negative event_id to avoid colliding with the workflow's
      ;; command sequence. It is informational -- replay does not check it.
      (with-transaction ((workflow-engine-db engine))
        (db-create-signal (workflow-engine-db engine) run-id signal-name payload)
        (let ((ext-event-id (next-external-event-id engine run-id)))
          (db-append-event (workflow-engine-db engine) run-id ext-event-id
                           "SIGNAL_RECEIVED"
                           (list :signal-name signal-name :payload payload))))
      ;; If the workflow is actively waiting for this signal, consume and wake
      (when (and ctx
                 (equal (workflow-context-waiting-for-signal ctx) signal-name))
        (let ((sig (with-db-lock ((workflow-engine-db engine))
                     (db-find-unconsumed-signal (workflow-engine-db engine)
                                                run-id signal-name))))
          (when sig
            (with-transaction ((workflow-engine-db engine))
              (db-consume-signal (workflow-engine-db engine) (getf sig :id))
              (db-append-event (workflow-engine-db engine)
                               run-id (next-event-id ctx)
                               "SIGNAL_CONSUMED"
                               (list :signal-name signal-name :payload payload)))
            (setf (workflow-context-waiting-for-signal ctx) nil)
            (wake-workflow ctx :signal-consumed payload)))))))

;;; ─── Query ─────────────────────────────────────────────────────────────────

(defun query-workflow (engine workflow-id query-name &key run-id)
  "Query a workflow's state.
   Without :run-id, queries the currently RUNNING run (in-memory state).
   With :run-id, replays the run from history to reconstruct state (synchronous, slow)."
  (if run-id
      (query-historical-run engine run-id query-name)
      (query-live-run engine workflow-id query-name)))

(defun query-historical-run (engine run-id query-name)
  "Query a completed run by replaying it read-only."
  (let ((run (db-get-workflow-run (workflow-engine-db engine) run-id)))
    (unless run
      (error "Run ~A not found" run-id))
    (let* ((workflow-type (getf run :workflow-type))
           (handler (find-query-handler workflow-type query-name)))
      (unless handler
        (error "No query handler ~A for ~A" query-name workflow-type))
      (let* ((wdef (find-workflow workflow-type))
             (history (db-load-event-history (workflow-engine-db engine) run-id))
             (input (getf run :input))
             (result-lock (bt:make-lock "hist-query"))
             (result-cv (bt:make-condition-variable :name "hist-query-cv"))
             (query-result nil)
             (query-done nil)
             (query-error nil))
        ;; Replay in a temporary thread
        (bt:make-thread
         (lambda ()
           (let* ((ctx (make-workflow-context
                         :engine engine
                         :run-id run-id
                         :workflow-id (getf run :workflow-id)
                         :workflow-type workflow-type
                         :task-queue (getf run :task-queue)
                         :event-counter 0
                         :history history
                         :history-length (length history)))
                  (*workflow-context* ctx))
             (handler-case
                 (progn
                   (apply (workflow-def-function wdef)
                          (if (listp input) input (list input)))
                   ;; Replay finished -- run query against reconstructed state
                   (bt:with-lock-held (result-lock)
                     (setf query-result (funcall handler)
                           query-done t)
                     (bt:condition-notify result-cv)))
               (error (e)
                 (bt:with-lock-held (result-lock)
                   (setf query-error e
                         query-done t)
                   (bt:condition-notify result-cv))))))
         :name (format nil "hist-query-~A" run-id))
        ;; Wait for replay
        (bt:with-lock-held (result-lock)
          (loop until query-done
                do (bt:condition-wait result-cv result-lock :timeout 30))
          (when query-error (error query-error))
          query-result)))))

(defun query-live-run (engine workflow-id query-name)
  (let ((run-info (db-find-running-run (workflow-engine-db engine) workflow-id)))
    (unless run-info
      (error 'no-running-workflow-error :workflow-id workflow-id))
    (let* ((run-id (getf run-info :run-id))
           (ctx (bt:with-lock-held ((workflow-engine-lock engine))
                  (gethash run-id (workflow-engine-contexts engine)))))
      (unless ctx
        (error "Run ~A has no active context" run-id))
      (let ((handler (find-query-handler (workflow-context-workflow-type ctx) query-name)))
        (unless handler
          (error "No query handler ~A for ~A" query-name (workflow-context-workflow-type ctx)))
        ;; Deliver to workflow thread
        (bt:with-lock-held ((workflow-context-query-lock ctx))
          (setf (workflow-context-pending-query ctx) handler
                (workflow-context-query-ready ctx) nil))
        ;; Wake workflow if blocked
        (bt:with-lock-held ((workflow-context-lock ctx))
          (bt:condition-notify (workflow-context-condvar ctx)))
        ;; Wait for result
        (bt:with-lock-held ((workflow-context-query-lock ctx))
          (loop until (workflow-context-query-ready ctx)
                do (bt:condition-wait (workflow-context-query-condvar ctx)
                                      (workflow-context-query-lock ctx)
                                      :timeout 5))
          (workflow-context-query-result ctx))))))

;;; ─── Status & History ──────────────────────────────────────────────────────

(defun get-workflow-status (engine workflow-id &key run-id)
  (if run-id
      (let ((run (db-get-workflow-run (workflow-engine-db engine) run-id)))
        (when run (getf run :status)))
      (let ((run-info (db-find-running-run (workflow-engine-db engine) workflow-id)))
        (if run-info
            "RUNNING"
            (let ((runs (db-list-workflow-runs (workflow-engine-db engine)
                                               :workflow-id workflow-id :limit 1)))
              (when runs (fourth (first runs))))))))

(defun get-workflow-history (engine workflow-id &key run-id include-external)
  "Get event history. With INCLUDE-EXTERNAL, also returns SIGNAL_RECEIVED etc."
  (let ((rid (or run-id
                 (let ((info (db-find-running-run (workflow-engine-db engine) workflow-id)))
                   (if info
                       (getf info :run-id)
                       (let ((runs (db-list-workflow-runs (workflow-engine-db engine)
                                                          :workflow-id workflow-id :limit 1)))
                         (when runs (first (first runs)))))))))
    (when rid
      (db-load-event-history (workflow-engine-db engine) rid
                              :include-external include-external))))

;;; ─── Logging ───────────────────────────────────────────────────────────────

(defun log-message (fmt &rest args)
  (format *error-output* "~&[cl-workflow] ~?~%" fmt args)
  (force-output *error-output*))
