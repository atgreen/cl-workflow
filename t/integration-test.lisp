;;; integration-test.lisp -- Integration tests for cl-workflow
;;;
;;; Run with:
;;;   sbcl --eval '(asdf:load-system :cl-workflow)' \
;;;        --load t/integration-test.lisp \
;;;        --eval '(cl-workflow-test:run-all-tests)' \
;;;        --quit

(defpackage #:cl-workflow-test
  (:use #:cl #:cl-workflow)
  (:export #:run-all-tests))

(in-package #:cl-workflow-test)

(defvar *test-results* nil)
(defvar *test-count* 0)
(defvar *pass-count* 0)

(defmacro deftest (name &body body)
  `(defun ,name ()
     (incf *test-count*)
     (handler-case
         (progn ,@body
                (incf *pass-count*)
                (format t "  PASS ~A~%" ',name)
                (push (cons ',name :pass) *test-results*))
       (error (e)
         (format t "  FAIL ~A: ~A~%" ',name e)
         (push (cons ',name e) *test-results*)))))

(defmacro assert-equal (expected actual &optional message)
  `(let ((exp ,expected)
         (act ,actual))
     (unless (equal exp act)
       (error "~@[~A: ~]Expected ~S, got ~S" ,message exp act))))

(defmacro assert-true (expr &optional message)
  `(unless ,expr
     (error "~@[~A: ~]Expected true, got NIL" ,message)))

(defun cleanup-test-db (path)
  (when (probe-file path)
    (delete-file path))
  ;; Also clean up WAL and SHM files
  (let ((wal (format nil "~A-wal" path))
        (shm (format nil "~A-shm" path)))
    (when (probe-file wal) (delete-file wal))
    (when (probe-file shm) (delete-file shm))))

;;; ─── Test Activities & Workflows ───────────────────────────────────────────

(defvar *activity-call-log* nil
  "Track activity invocations for testing.")

(cl-workflow:defactivity test-add ((a number) (b number))
  :retry-policy (:max-attempts 1)
  (push (list 'test-add a b) *activity-call-log*)
  (+ a b))

(cl-workflow:defactivity test-greet ((name string))
  :retry-policy (:max-attempts 1)
  (push (list 'test-greet name) *activity-call-log*)
  (format nil "Hello, ~A!" name))

(defvar *fail-count* 0)

(cl-workflow:defactivity test-flaky ((msg string))
  :retry-policy (:max-attempts 3 :initial-interval 0.1 :backoff-coefficient 1.0)
  (push (list 'test-flaky msg *fail-count*) *activity-call-log*)
  (when (< *fail-count* 2)
    (incf *fail-count*)
    (error "Transient failure #~D" *fail-count*))
  (format nil "Eventually: ~A" msg))

(cl-workflow:defworkflow simple-math ((a number) (b number))
  "Add two numbers via an activity."
  (cl-workflow:execute-activity 'test-add :input (list a b)))

(cl-workflow:defworkflow multi-step ((name string))
  "Multi-step workflow: greet then add."
  (let ((greeting (cl-workflow:execute-activity 'test-greet :input (list name)))
        (sum (cl-workflow:execute-activity 'test-add :input (list 10 20))))
    (list greeting sum)))

(cl-workflow:defworkflow signal-workflow-test ((name string))
  "Workflow that waits for a signal."
  (let ((greeting (cl-workflow:execute-activity 'test-greet :input (list name))))
    (let ((sig (cl-workflow:workflow-receive "my-signal" :timeout 10)))
      (list greeting sig))))

(cl-workflow:defworkflow side-effect-workflow ()
  "Workflow using side-effect helpers."
  (let ((t1 (cl-workflow:workflow-now))
        (r1 (cl-workflow:workflow-random 100))
        (se (cl-workflow:workflow-side-effect (lambda () "computed-value"))))
    (list t1 r1 se)))

(cl-workflow:defworkflow timer-workflow ()
  "Workflow with a short timer."
  (cl-workflow:execute-activity 'test-add :input '(1 1))
  (cl-workflow:workflow-sleep 1)
  (cl-workflow:execute-activity 'test-add :input '(2 2))
  :done)

(cl-workflow:defworkflow retry-workflow ((msg string))
  "Workflow that calls a flaky activity."
  (cl-workflow:execute-activity 'test-flaky :input (list msg)))

;;; ─── Tests ─────────────────────────────────────────────────────────────────

(deftest test-simple-workflow
  "A simple workflow that runs one activity."
  (let ((db-path "/tmp/cl-workflow-test-simple.db"))
    (cleanup-test-db db-path)
    (setf *activity-call-log* nil)
    (let ((engine (cl-workflow:make-engine :db-path db-path)))
      (unwind-protect
           (let ((run-id (cl-workflow:start-workflow
                          engine 'simple-math
                          :workflow-id "test-simple"
                          :input '(3 4))))
             (assert-true run-id "run-id returned")
             ;; Wait for completion
             (sleep 2)
             (let ((status (cl-workflow:get-workflow-status engine "test-simple")))
               (assert-equal "COMPLETED" status "workflow completed"))
             ;; Check history
             (let ((history (cl-workflow:get-workflow-history engine "test-simple")))
               (assert-true (> (length history) 0) "has events"))
             ;; Check activity was called
             (assert-true (find 'test-add *activity-call-log* :key #'car)
                          "activity was executed"))
        (cl-workflow:stop-engine engine)
        (cleanup-test-db db-path)))))

(deftest test-multi-step-workflow
  "A workflow with two sequential activities."
  (let ((db-path "/tmp/cl-workflow-test-multi.db"))
    (cleanup-test-db db-path)
    (setf *activity-call-log* nil)
    (let ((engine (cl-workflow:make-engine :db-path db-path)))
      (unwind-protect
           (progn
             (cl-workflow:start-workflow engine 'multi-step
                                         :workflow-id "test-multi"
                                         :input '("World"))
             (sleep 3)
             (let ((status (cl-workflow:get-workflow-status engine "test-multi")))
               (assert-equal "COMPLETED" status "workflow completed"))
             ;; Both activities should have been called
             (assert-true (find 'test-greet *activity-call-log* :key #'car)
                          "greet called")
             (assert-true (find 'test-add *activity-call-log* :key #'car)
                          "add called"))
        (cl-workflow:stop-engine engine)
        (cleanup-test-db db-path)))))

(deftest test-signal-delivery
  "A workflow that waits for an external signal."
  (let ((db-path "/tmp/cl-workflow-test-signal.db"))
    (cleanup-test-db db-path)
    (setf *activity-call-log* nil)
    (let ((engine (cl-workflow:make-engine :db-path db-path)))
      (unwind-protect
           (progn
             (cl-workflow:start-workflow engine 'signal-workflow-test
                                         :workflow-id "test-signal"
                                         :input '("Signals"))
             ;; Wait for the workflow to reach the signal-receive point
             (sleep 2)
             ;; It should still be running (waiting for signal)
             (let ((status (cl-workflow:get-workflow-status engine "test-signal")))
               (assert-equal "RUNNING" status "still running waiting for signal"))
             ;; Send the signal
             (cl-workflow:signal-workflow engine "test-signal" "my-signal"
                                          :payload '(:data "signal-payload"))
             (sleep 2)
             ;; Now it should be completed
             (let ((status (cl-workflow:get-workflow-status engine "test-signal")))
               (assert-equal "COMPLETED" status "completed after signal")))
        (cl-workflow:stop-engine engine)
        (cleanup-test-db db-path)))))

(deftest test-durable-timer
  "A workflow with a 1-second sleep."
  (let ((db-path "/tmp/cl-workflow-test-timer.db"))
    (cleanup-test-db db-path)
    (setf *activity-call-log* nil)
    (let ((engine (cl-workflow:make-engine :db-path db-path)))
      (unwind-protect
           (progn
             (cl-workflow:start-workflow engine 'timer-workflow
                                         :workflow-id "test-timer"
                                         :input nil)
             (sleep 4)
             (let ((status (cl-workflow:get-workflow-status engine "test-timer")))
               (assert-equal "COMPLETED" status "completed after timer"))
             ;; Both activities should have run
             (let ((add-calls (remove-if-not (lambda (x) (eq (car x) 'test-add))
                                             *activity-call-log*)))
               (assert-equal 2 (length add-calls) "two add activities")))
        (cl-workflow:stop-engine engine)
        (cleanup-test-db db-path)))))

(deftest test-side-effects
  "Workflow-now, workflow-random, workflow-side-effect."
  (let ((db-path "/tmp/cl-workflow-test-side.db"))
    (cleanup-test-db db-path)
    (let ((engine (cl-workflow:make-engine :db-path db-path)))
      (unwind-protect
           (progn
             (cl-workflow:start-workflow engine 'side-effect-workflow
                                         :workflow-id "test-side"
                                         :input nil)
             (sleep 2)
             (let ((status (cl-workflow:get-workflow-status engine "test-side")))
               (assert-equal "COMPLETED" status "completed"))
             ;; Check events were recorded
             (let ((history (cl-workflow:get-workflow-history engine "test-side")))
               ;; Should have WORKFLOW_STARTED + 3 SIDE_EFFECT_RECORDED + WORKFLOW_COMPLETED
               (let ((side-effects (remove-if-not
                                    (lambda (ev) (string= (getf ev :event-type) "SIDE_EFFECT_RECORDED"))
                                    history)))
                 (assert-equal 3 (length side-effects) "three side effects recorded"))))
        (cl-workflow:stop-engine engine)
        (cleanup-test-db db-path)))))

(deftest test-activity-retry
  "Activity that fails twice then succeeds."
  (let ((db-path "/tmp/cl-workflow-test-retry.db"))
    (cleanup-test-db db-path)
    (setf *activity-call-log* nil)
    (setf *fail-count* 0)
    (let ((engine (cl-workflow:make-engine :db-path db-path)))
      (unwind-protect
           (progn
             (cl-workflow:start-workflow engine 'retry-workflow
                                         :workflow-id "test-retry"
                                         :input '("retry-msg"))
             ;; Retries with 0.1s interval, need to wait a bit
             (sleep 5)
             (let ((status (cl-workflow:get-workflow-status engine "test-retry")))
               (assert-equal "COMPLETED" status "completed after retries"))
             ;; The flaky activity should have been called 3 times
             (let ((flaky-calls (remove-if-not (lambda (x) (eq (car x) 'test-flaky))
                                               *activity-call-log*)))
               (assert-true (>= (length flaky-calls) 3) "at least 3 attempts")))
        (cl-workflow:stop-engine engine)
        (cleanup-test-db db-path)))))

(deftest test-event-history
  "Verify event history structure."
  (let ((db-path "/tmp/cl-workflow-test-history.db"))
    (cleanup-test-db db-path)
    (let ((engine (cl-workflow:make-engine :db-path db-path)))
      (unwind-protect
           (progn
             (cl-workflow:start-workflow engine 'simple-math
                                         :workflow-id "test-history"
                                         :input '(10 20))
             (sleep 2)
             (let ((history (cl-workflow:get-workflow-history engine "test-history")))
               (assert-true (>= (length history) 4) "at least 4 events")
               ;; First event should be WORKFLOW_STARTED
               (assert-equal "WORKFLOW_STARTED" (getf (first history) :event-type)
                             "starts with WORKFLOW_STARTED")
               ;; Last event should be WORKFLOW_COMPLETED
               (assert-equal "WORKFLOW_COMPLETED" (getf (car (last history)) :event-type)
                             "ends with WORKFLOW_COMPLETED")))
        (cl-workflow:stop-engine engine)
        (cleanup-test-db db-path)))))

;;; ─── Regression: Replay After Restart ────────────────────────────────────

(cl-workflow:defworkflow replay-test-workflow ((x number))
  "Workflow for testing replay: two activities."
  (let ((a (cl-workflow:execute-activity 'test-add :input (list x 10)))
        (b (cl-workflow:execute-activity 'test-add :input (list x 20))))
    (list a b)))

(deftest test-replay-after-restart
  "Simulate restart: run a workflow, stop engine, start new engine, verify state."
  (let ((db-path "/tmp/cl-workflow-test-replay.db"))
    (cleanup-test-db db-path)
    (setf *activity-call-log* nil)
    ;; Run workflow to completion
    (let ((engine (cl-workflow:make-engine :db-path db-path)))
      (cl-workflow:start-workflow engine 'replay-test-workflow
                                   :workflow-id "test-replay"
                                   :input '(5))
      (sleep 3)
      (assert-equal "COMPLETED"
                    (cl-workflow:get-workflow-status engine "test-replay")
                    "completed in first engine")
      (cl-workflow:stop-engine engine))
    ;; Start a new engine on the same DB -- it should see the completed run
    (let ((engine (cl-workflow:make-engine :db-path db-path)))
      (unwind-protect
           (progn
             (sleep 1)
             (let ((status (cl-workflow:get-workflow-status engine "test-replay")))
               (assert-equal "COMPLETED" status "still completed after restart"))
             ;; History should be intact
             (let ((history (cl-workflow:get-workflow-history engine "test-replay")))
               (assert-true (>= (length history) 5) "full history preserved")
               (assert-equal "WORKFLOW_STARTED" (getf (first history) :event-type))
               (assert-equal "WORKFLOW_COMPLETED" (getf (car (last history)) :event-type))))
        (cl-workflow:stop-engine engine)
        (cleanup-test-db db-path)))))

;;; ─── Regression: Signal Before Receive ──────────────────────────────────

(cl-workflow:defworkflow signal-before-receive-wf ()
  "Workflow that sleeps before receiving, giving time for signal to arrive first."
  (cl-workflow:workflow-sleep 2)
  (cl-workflow:workflow-receive "early-signal" :timeout 5))

(deftest test-signal-before-receive
  "Send a signal before the workflow calls workflow-receive."
  (let ((db-path "/tmp/cl-workflow-test-sig-before.db"))
    (cleanup-test-db db-path)
    (let ((engine (cl-workflow:make-engine :db-path db-path)))
      (unwind-protect
           (progn
             (cl-workflow:start-workflow engine 'signal-before-receive-wf
                                         :workflow-id "test-sig-before"
                                         :input nil)
             ;; Send signal immediately (before workflow reaches workflow-receive)
             (sleep 0.5)
             (cl-workflow:signal-workflow engine "test-sig-before" "early-signal"
                                          :payload '(:early t))
             ;; Wait for workflow to complete (sleep 2 + receive)
             (sleep 5)
             (let ((status (cl-workflow:get-workflow-status engine "test-sig-before")))
               (assert-equal "COMPLETED" status "completed with early signal")))
        (cl-workflow:stop-engine engine)
        (cleanup-test-db db-path)))))

;;; ─── Regression: Query Live State ──────────────────────────────────────────

(cl-workflow:defworkflow queryable-wf ()
  "Workflow that sets state and waits for a signal."
  (setf (cl-workflow:workflow-state :phase) :started)
  (cl-workflow:execute-activity 'test-add :input '(1 1))
  (setf (cl-workflow:workflow-state :phase) :waiting)
  (cl-workflow:workflow-receive "done" :timeout 30)
  (setf (cl-workflow:workflow-state :phase) :finished)
  :ok)

(cl-workflow:defquery queryable-wf current-phase ()
  "Return the current phase."
  (cl-workflow:workflow-state :phase))

(deftest test-query-live-state
  "Query a running workflow's state."
  (let ((db-path "/tmp/cl-workflow-test-query.db"))
    (cleanup-test-db db-path)
    (let ((engine (cl-workflow:make-engine :db-path db-path)))
      (unwind-protect
           (progn
             (cl-workflow:start-workflow engine 'queryable-wf
                                         :workflow-id "test-query"
                                         :input nil)
             ;; Wait for activity + state update
             (sleep 3)
             ;; Query the phase -- should be :waiting (blocked on signal)
             (let ((phase (cl-workflow:query-workflow engine "test-query" 'current-phase)))
               (assert-equal :waiting phase "query returns :waiting"))
             ;; Send signal to complete
             (cl-workflow:signal-workflow engine "test-query" "done" :payload t)
             (sleep 2)
             (let ((status (cl-workflow:get-workflow-status engine "test-query")))
               (assert-equal "COMPLETED" status "completed after signal")))
        (cl-workflow:stop-engine engine)
        (cleanup-test-db db-path)))))

;;; ─── Regression: Backoff Timing ────────────────────────────────────────────

(defvar *backoff-timestamps* nil)

(cl-workflow:defactivity timed-flaky ((msg string))
  :retry-policy (:max-attempts 3 :initial-interval 2 :backoff-coefficient 1.0)
  (push (get-internal-real-time) *backoff-timestamps*)
  (when (< (length *backoff-timestamps*) 3)
    (error "Fail ~D" (length *backoff-timestamps*)))
  msg)

(cl-workflow:defworkflow backoff-test-wf ((msg string))
  (cl-workflow:execute-activity 'timed-flaky :input (list msg)))

(deftest test-backoff-timing
  "Verify that retry backoff actually delays execution."
  (let ((db-path "/tmp/cl-workflow-test-backoff.db"))
    (cleanup-test-db db-path)
    (setf *backoff-timestamps* nil)
    (let ((engine (cl-workflow:make-engine :db-path db-path)))
      (unwind-protect
           (progn
             (cl-workflow:start-workflow engine 'backoff-test-wf
                                         :workflow-id "test-backoff"
                                         :input '("backoff"))
             (sleep 10)
             (let ((status (cl-workflow:get-workflow-status engine "test-backoff")))
               (assert-equal "COMPLETED" status "completed after retries"))
             ;; Check that there was meaningful delay between attempts
             (assert-true (>= (length *backoff-timestamps*) 3) "at least 3 attempts")
             (when (>= (length *backoff-timestamps*) 2)
               (let* ((sorted (sort (copy-list *backoff-timestamps*) #'<))
                      (gap (/ (- (second sorted) (first sorted))
                              internal-time-units-per-second)))
                 ;; With initial-interval=2, gap should be >= 1.5s (allowing some slack)
                 (assert-true (>= gap 1.0)
                              (format nil "retry gap ~,1Fs >= 1.0s" gap)))))
        (cl-workflow:stop-engine engine)
        (cleanup-test-db db-path)))))

;;; ─── Regression: SIGNAL_RECEIVED in History ────────────────────────────────

(deftest test-signal-received-event
  "Verify that signal-workflow records a SIGNAL_RECEIVED event in history."
  (let ((db-path "/tmp/cl-workflow-test-sigrec.db"))
    (cleanup-test-db db-path)
    (let ((engine (cl-workflow:make-engine :db-path db-path)))
      (unwind-protect
           (progn
             (cl-workflow:start-workflow engine 'signal-workflow-test
                                         :workflow-id "test-sigrec"
                                         :input '("SigRec"))
             (sleep 2)
             (cl-workflow:signal-workflow engine "test-sigrec" "my-signal"
                                          :payload '(:test t))
             (sleep 2)
             ;; SIGNAL_RECEIVED should appear in full history (include-external)
             (let* ((full-history (cl-workflow:get-workflow-history
                                  engine "test-sigrec" :include-external t))
                    (received (find "SIGNAL_RECEIVED" full-history
                                    :key (lambda (ev) (getf ev :event-type))
                                    :test #'string=)))
               (assert-true received "SIGNAL_RECEIVED event exists in full history"))
             ;; But NOT in replay history (default), to avoid corrupting replay
             (let* ((replay-history (cl-workflow:get-workflow-history engine "test-sigrec"))
                    (received (find "SIGNAL_RECEIVED" replay-history
                                    :key (lambda (ev) (getf ev :event-type))
                                    :test #'string=)))
               (assert-true (null received) "SIGNAL_RECEIVED excluded from replay history")))
        (cl-workflow:stop-engine engine)
        (cleanup-test-db db-path)))))

;;; ─── Regression: Historical Query ──────────────────────────────────────────

(cl-workflow:defworkflow historical-query-wf ((val number))
  "Simple workflow that sets state then completes."
  (setf (cl-workflow:workflow-state :final-value) val)
  (cl-workflow:execute-activity 'test-add :input (list val val))
  (setf (cl-workflow:workflow-state :final-value) (* val 2))
  (* val 2))

(cl-workflow:defquery historical-query-wf get-final-value ()
  (cl-workflow:workflow-state :final-value))

(deftest test-historical-query
  "Query a completed run by replaying it."
  (let ((db-path "/tmp/cl-workflow-test-histq.db"))
    (cleanup-test-db db-path)
    (let ((engine (cl-workflow:make-engine :db-path db-path))
          (run-id nil))
      (unwind-protect
           (progn
             (setf run-id (cl-workflow:start-workflow engine 'historical-query-wf
                                                      :workflow-id "test-histq"
                                                      :input '(21)))
             (sleep 3)
             (assert-equal "COMPLETED"
                           (cl-workflow:get-workflow-status engine "test-histq")
                           "workflow completed")
             ;; Now query the completed run by run-id
             (let ((result (cl-workflow:query-workflow engine "test-histq" 'get-final-value
                                                       :run-id run-id)))
               (assert-equal 42 result "historical query returns 42")))
        (cl-workflow:stop-engine engine)
        (cleanup-test-db db-path)))))

;;; ─── Regression: Execution Timeout ─────────────────────────────────────────

(cl-workflow:defworkflow timeout-wf ()
  "Workflow that hangs forever (waits for a signal that never comes)."
  (cl-workflow:workflow-receive "never-arrives" :timeout 3600)
  :should-not-reach)

(deftest test-execution-timeout
  "Workflow with a short execution timeout transitions to TIMED_OUT."
  (let ((db-path "/tmp/cl-workflow-test-timeout.db"))
    (cleanup-test-db db-path)
    (let ((engine (cl-workflow:make-engine :db-path db-path)))
      (unwind-protect
           (progn
             (cl-workflow:start-workflow engine 'timeout-wf
                                         :workflow-id "test-timeout"
                                         :input nil
                                         :execution-timeout 2)
             ;; Wait for timeout to fire (2s + scheduler poll interval)
             (sleep 5)
             (let ((status (cl-workflow:get-workflow-status engine "test-timeout")))
               (assert-equal "TIMED_OUT" status "workflow timed out")))
        (cl-workflow:stop-engine engine)
        (cleanup-test-db db-path)))))

;;; ─── Runner ────────────────────────────────────────────────────────────────

(defun run-all-tests ()
  (setf *test-results* nil
        *test-count* 0
        *pass-count* 0)
  (format t "~%Running cl-workflow integration tests...~%~%")
  (test-simple-workflow)
  (test-multi-step-workflow)
  (test-signal-delivery)
  (test-durable-timer)
  (test-side-effects)
  (test-activity-retry)
  (test-event-history)
  ;; Regression tests
  (test-replay-after-restart)
  (test-signal-before-receive)
  (test-query-live-state)
  (test-backoff-timing)
  (test-signal-received-event)
  (test-historical-query)
  (test-execution-timeout)
  (format t "~%~D/~D tests passed.~%" *pass-count* *test-count*)
  (if (= *pass-count* *test-count*)
      (format t "All tests passed!~%")
      (progn
        (format t "FAILURES:~%")
        (dolist (r *test-results*)
          (unless (eq (cdr r) :pass)
            (format t "  ~A: ~A~%" (car r) (cdr r))))))
  (values *pass-count* *test-count*))
