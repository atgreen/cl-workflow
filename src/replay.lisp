;;; replay.lisp -- Workflow context, replay engine, and workflow commands
;;;
;;; SPDX-License-Identifier: MIT
;;;
;;; Copyright (C) 2026 Anthony Green

(in-package #:cl-flow)

;;; ─── Workflow Context ──────────────────────────────────────────────────────
;;;
;;; Each workflow run thread has a *workflow-context* bound to its context.
;;; The context tracks the event counter (position in history), the loaded
;;; event history, and a condition variable for blocking.

(defstruct workflow-context
  "Per-run context bound on the workflow thread."
  (engine nil)                              ; back-reference to engine
  (run-id nil :type string)
  (workflow-id nil :type string)
  (workflow-type nil)
  (task-queue nil :type string)
  (event-counter 0 :type fixnum)            ; next event-id to issue
  (history nil :type list)                   ; loaded events from DB
  (history-length 0 :type fixnum)           ; length of history
  (lock (bt:make-lock "wf-ctx"))
  (condvar (bt:make-condition-variable :name "wf-cv"))
  (pending-result nil)                       ; result slot set by engine
  (pending-result-type nil)                  ; :activity-completed, :timer-fired, etc.
  (pending-error nil)                        ; error slot for failures
  ;; Query support
  (query-lock (bt:make-lock "wf-query"))
  (query-condvar (bt:make-condition-variable :name "wf-qcv"))
  (pending-query nil)                        ; query handler to run
  (query-result nil)                         ; result of query
  (query-ready nil)                          ; flag: query result available
  ;; State
  (at-safe-point nil)                        ; between commands
  (waiting-for-signal nil)                   ; signal-name string if blocked in workflow-receive
  ;; User-visible workflow state for query handlers
  (state (make-hash-table :test 'equal))     ; workflow sets via (setf (workflow-state key) val)
  ;; Execution timeout
  (deadline nil))                            ; universal-time when this run must finish, or nil

(defvar *workflow-context* nil
  "The current workflow context. Bound on workflow run threads.")

(defun ensure-in-workflow (command-name)
  "Signal an error if not running inside a workflow."
  (unless *workflow-context*
    (error 'not-in-workflow-error :command command-name)))

(defun workflow-state (key)
  "Get a value from the workflow's queryable state.
   Callable from both workflow code and query handlers."
  (gethash key (workflow-context-state *workflow-context*)))

(defun (setf workflow-state) (value key)
  "Set a value in the workflow's queryable state.
   Callable from workflow code (not from query handlers)."
  (setf (gethash key (workflow-context-state *workflow-context*)) value))

;;; ─── Replay Logic ──────────────────────────────────────────────────────────

(defun next-event-id (ctx)
  "Increment and return the next event-id for this run."
  (incf (workflow-context-event-counter ctx)))

(defun replay-event (ctx expected-event-id)
  "Check if event at EXPECTED-EVENT-ID exists in the loaded history.
   Returns the event plist if replaying, NIL if at the frontier.
   Event IDs are 0-based: WORKFLOW_STARTED=0, first command=1, etc."
  (let ((idx expected-event-id))
    (when (< idx (workflow-context-history-length ctx))
      (nth idx (workflow-context-history ctx)))))

(defun check-replay-match (ctx event-id expected-type &optional expected-name)
  "On replay, verify the recorded event matches what the workflow issued.
   Raises non-determinism-error on mismatch."
  (let ((recorded (replay-event ctx event-id)))
    (when recorded
      (let ((recorded-type (getf recorded :event-type)))
        (unless (string= recorded-type expected-type)
          (error 'non-determinism-error
                 :run-id (workflow-context-run-id ctx)
                 :event-id event-id
                 :expected expected-type
                 :actual recorded-type))
        (when expected-name
          (let ((recorded-name (getf (getf recorded :attributes) :name)))
            (when (and recorded-name (not (equal recorded-name expected-name)))
              (error 'non-determinism-error
                     :run-id (workflow-context-run-id ctx)
                     :event-id event-id
                     :expected (format nil "~A ~A" expected-type expected-name)
                     :actual (format nil "~A ~A" recorded-type recorded-name))))))
      recorded)))

;;; ─── Safe Point (Query Processing) ─────────────────────────────────────────

(defun process-pending-queries (ctx)
  "Check for and process a pending query at a safe point."
  (setf (workflow-context-at-safe-point ctx) t)
  (bt:with-lock-held ((workflow-context-query-lock ctx))
    (when (workflow-context-pending-query ctx)
      (let ((handler (workflow-context-pending-query ctx)))
        (setf (workflow-context-query-result ctx)
              (handler-case (funcall handler)
                (error (e) (format nil "Query error: ~A" e))))
        (setf (workflow-context-pending-query ctx) nil)
        (setf (workflow-context-query-ready ctx) t)
        (bt:condition-notify (workflow-context-query-condvar ctx)))))
  (setf (workflow-context-at-safe-point ctx) nil))

;;; ─── Block/Wake Primitives ─────────────────────────────────────────────────

(defun block-workflow (ctx)
  "Block the workflow thread until the engine wakes it."
  (process-pending-queries ctx)
  (bt:with-lock-held ((workflow-context-lock ctx))
    (loop until (or (workflow-context-pending-result-type ctx)
                    (workflow-context-pending-error ctx))
          do (bt:condition-wait (workflow-context-condvar ctx)
                                (workflow-context-lock ctx))
             ;; Check for queries while waiting
             (process-pending-queries ctx)))
  ;; Check for error first
  (when (workflow-context-pending-error ctx)
    (let ((err (workflow-context-pending-error ctx)))
      (setf (workflow-context-pending-error ctx) nil)
      (error err)))
  ;; Return result
  (let ((result (workflow-context-pending-result ctx)))
    (setf (workflow-context-pending-result ctx) nil)
    (setf (workflow-context-pending-result-type ctx) nil)
    result))

(defun wake-workflow (ctx result-type result)
  "Wake a blocked workflow thread with a result."
  (bt:with-lock-held ((workflow-context-lock ctx))
    (setf (workflow-context-pending-result-type ctx) result-type)
    (setf (workflow-context-pending-result ctx) result)
    (bt:condition-notify (workflow-context-condvar ctx))))

(defun wake-workflow-error (ctx error)
  "Wake a blocked workflow thread with an error."
  (bt:with-lock-held ((workflow-context-lock ctx))
    (setf (workflow-context-pending-error ctx) error)
    (bt:condition-notify (workflow-context-condvar ctx))))

;;; ─── Workflow Commands ─────────────────────────────────────────────────────

(defun execute-activity (name &key input task-queue)
  "Execute an activity. Blocks until the activity completes or fails.
   On replay, returns the persisted result."
  (ensure-in-workflow 'execute-activity)
  (let* ((ctx *workflow-context*)
         (engine (workflow-context-engine ctx))
         (activity-def (find-activity name))
         (event-id (next-event-id ctx))
         (tq (or task-queue (workflow-context-task-queue ctx)))
         (activity-name-str (symbol-name name)))

    (tagbody
       ;; Check replay: look for ACTIVITY_SCHEDULED at this position
       (let ((scheduled-event (check-replay-match ctx event-id "ACTIVITY_SCHEDULED" activity-name-str)))
         (when scheduled-event
           ;; We're replaying. The next event should be ACTIVITY_COMPLETED or ACTIVITY_FAILED.
           (let* ((result-event-id (next-event-id ctx))
                  (result-event (replay-event ctx result-event-id)))
             (unless result-event
               ;; History ends here -- we need to actually run the activity from this point
               ;; Decrement counter since we'll re-issue this event
               (decf (workflow-context-event-counter ctx))
               (go go-to-live-execution))
             (let ((result-type (getf result-event :event-type)))
               (cond
                 ((string= result-type "ACTIVITY_COMPLETED")
                  (process-pending-queries ctx)
                  (return-from execute-activity
                    (getf (getf result-event :attributes) :result)))
                 ((string= result-type "ACTIVITY_FAILED")
                  (let ((attrs (getf result-event :attributes)))
                    (error 'activity-failure
                           :activity-name name
                           :attempts (getf attrs :attempts)
                           :last-error (getf attrs :error-message))))
                 (t
                  (error 'non-determinism-error
                         :run-id (workflow-context-run-id ctx)
                         :event-id result-event-id
                         :expected "ACTIVITY_COMPLETED or ACTIVITY_FAILED"
                         :actual result-type)))))))

     go-to-live-execution
       ;; Live execution: schedule the activity
       (engine-schedule-activity engine ctx event-id name activity-name-str tq input activity-def)
       ;; Block until the engine completes/fails it
       (let ((result (block-workflow ctx)))
         (process-pending-queries ctx)
         (return-from execute-activity result)))))

(defun workflow-sleep (seconds)
  "Durable sleep. On replay, returns immediately if the timer has already fired."
  (ensure-in-workflow 'workflow-sleep)
  (let* ((ctx *workflow-context*)
         (engine (workflow-context-engine ctx))
         (event-id (next-event-id ctx)))

    ;; Check replay
    (let ((timer-event (check-replay-match ctx event-id "TIMER_STARTED")))
      (when timer-event
        ;; Look for TIMER_FIRED
        (let* ((fire-event-id (next-event-id ctx))
               (fire-event (replay-event ctx fire-event-id)))
          (when (and fire-event (string= (getf fire-event :event-type) "TIMER_FIRED"))
            (process-pending-queries ctx)
            (return-from workflow-sleep nil))
          ;; Timer was started but not yet fired -- need to wait
          (decf (workflow-context-event-counter ctx))
          (engine-wait-timer engine ctx event-id seconds t)
          (process-pending-queries ctx)
          (return-from workflow-sleep nil))))

    ;; Live execution
    (engine-wait-timer engine ctx event-id seconds nil)
    (process-pending-queries ctx)
    nil))

(defun workflow-receive (signal-name &key timeout)
  "Wait for a signal. Returns the payload, or NIL if timeout expires.
   On replay, returns the persisted outcome."
  (ensure-in-workflow 'workflow-receive)
  (let* ((ctx *workflow-context*)
         (engine (workflow-context-engine ctx))
         (event-id (next-event-id ctx)))

    ;; On replay, the event should be TIMER_STARTED (if timeout) or directly check
    ;; for SIGNAL_CONSUMED
    (let ((event (replay-event ctx event-id)))
      (when event
        (let ((etype (getf event :event-type)))
          (cond
            ((string= etype "TIMER_STARTED")
             ;; Look at the next event for outcome
             (let* ((outcome-id (next-event-id ctx))
                    (outcome (replay-event ctx outcome-id)))
               (when outcome
                 (let ((otype (getf outcome :event-type)))
                   (cond
                     ((string= otype "SIGNAL_CONSUMED")
                      (process-pending-queries ctx)
                      (return-from workflow-receive
                        (getf (getf outcome :attributes) :payload)))
                     ((string= otype "TIMER_FIRED")
                      (process-pending-queries ctx)
                      (return-from workflow-receive nil))
                     (t
                      (error 'non-determinism-error
                             :run-id (workflow-context-run-id ctx)
                             :event-id outcome-id
                             :expected "SIGNAL_CONSUMED or TIMER_FIRED"
                             :actual otype)))))
               ;; Outcome not yet recorded, fall through to live
               (decf (workflow-context-event-counter ctx))))
            ((string= etype "SIGNAL_CONSUMED")
             (process-pending-queries ctx)
             (return-from workflow-receive
               (getf (getf event :attributes) :payload)))
            (t
             (error 'non-determinism-error
                    :run-id (workflow-context-run-id ctx)
                    :event-id event-id
                    :expected "TIMER_STARTED or SIGNAL_CONSUMED"
                    :actual etype))))))

    ;; Live execution
    (engine-wait-signal engine ctx event-id signal-name timeout)
    (let ((result (block-workflow ctx)))
      (process-pending-queries ctx)
      result)))

(defun workflow-now ()
  "Return current time, deterministic on replay."
  (ensure-in-workflow 'workflow-now)
  (let* ((ctx *workflow-context*)
         (event-id (next-event-id ctx)))
    (let ((event (check-replay-match ctx event-id "SIDE_EFFECT_RECORDED")))
      (when event
        (return-from workflow-now
          (getf (getf event :attributes) :value))))
    ;; Live: record current time
    (let ((now (get-universal-time)))
      (with-transaction ((engine-db (workflow-context-engine ctx)))
        (db-append-event (engine-db (workflow-context-engine ctx))
                         (workflow-context-run-id ctx)
                         event-id
                         "SIDE_EFFECT_RECORDED"
                         (list :name "workflow-now" :value now)))
      now)))

(defun workflow-random (&optional (limit 1.0))
  "Return a random number, deterministic on replay."
  (ensure-in-workflow 'workflow-random)
  (let* ((ctx *workflow-context*)
         (event-id (next-event-id ctx)))
    (let ((event (check-replay-match ctx event-id "SIDE_EFFECT_RECORDED")))
      (when event
        (return-from workflow-random
          (getf (getf event :attributes) :value))))
    ;; Live: record random value
    (let ((val (random limit)))
      (with-transaction ((engine-db (workflow-context-engine ctx)))
        (db-append-event (engine-db (workflow-context-engine ctx))
                         (workflow-context-run-id ctx)
                         event-id
                         "SIDE_EFFECT_RECORDED"
                         (list :name "workflow-random" :value val)))
      val)))

(defun workflow-side-effect (thunk)
  "Execute THUNK and record its return value. Deterministic on replay."
  (ensure-in-workflow 'workflow-side-effect)
  (let* ((ctx *workflow-context*)
         (event-id (next-event-id ctx)))
    (let ((event (check-replay-match ctx event-id "SIDE_EFFECT_RECORDED")))
      (when event
        (return-from workflow-side-effect
          (getf (getf event :attributes) :value))))
    ;; Live: run thunk and record
    (let ((val (funcall thunk)))
      (with-transaction ((engine-db (workflow-context-engine ctx)))
        (db-append-event (engine-db (workflow-context-engine ctx))
                         (workflow-context-run-id ctx)
                         event-id
                         "SIDE_EFFECT_RECORDED"
                         (list :name "workflow-side-effect" :value val)))
      val)))
