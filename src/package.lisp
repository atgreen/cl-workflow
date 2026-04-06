;;; package.lisp
;;;
;;; SPDX-License-Identifier: MIT
;;;
;;; Copyright (C) 2026 Anthony Green

(defpackage #:cl-workflow
  (:use #:cl)
  (:documentation "A Common Lisp-native durable workflow engine.")
  (:export
   ;; Engine
   #:make-engine
   #:stop-engine
   #:start-workflow
   #:signal-workflow
   #:query-workflow
   #:get-workflow-status
   #:get-workflow-history

   ;; Macros
   #:defworkflow
   #:defactivity
   #:defquery

   ;; Workflow commands (callable inside defworkflow only)
   #:execute-activity
   #:workflow-sleep
   #:workflow-receive
   #:workflow-now
   #:workflow-random
   #:workflow-side-effect
   #:workflow-state

   ;; Conditions
   #:activity-failure
   #:activity-failure-activity-name
   #:activity-failure-attempts
   #:activity-failure-last-error
   #:non-determinism-error
   #:not-in-workflow-error
   #:unknown-activity-error
   #:no-running-workflow-error
   #:workflow-execution-timeout))

(in-package #:cl-workflow)
