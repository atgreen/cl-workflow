;;; conditions.lisp -- Error and condition types
;;;
;;; SPDX-License-Identifier: MIT
;;;
;;; Copyright (C) 2026 Anthony Green

(in-package #:cl-workflow)

(define-condition cl-workflow-error (error)
  ()
  (:documentation "Base condition for all cl-workflow errors."))

(define-condition activity-failure (cl-workflow-error)
  ((activity-name :initarg :activity-name :reader activity-failure-activity-name)
   (attempts :initarg :attempts :reader activity-failure-attempts)
   (last-error :initarg :last-error :reader activity-failure-last-error))
  (:report (lambda (c s)
             (format s "Activity ~A failed after ~D attempt~:P: ~A"
                     (activity-failure-activity-name c)
                     (activity-failure-attempts c)
                     (activity-failure-last-error c)))))

(define-condition non-determinism-error (cl-workflow-error)
  ((run-id :initarg :run-id :reader non-determinism-error-run-id)
   (event-id :initarg :event-id :reader non-determinism-error-event-id)
   (expected :initarg :expected :reader non-determinism-error-expected)
   (actual :initarg :actual :reader non-determinism-error-actual))
  (:report (lambda (c s)
             (format s "Non-determinism detected in run ~A at event ~D: expected ~A, got ~A"
                     (non-determinism-error-run-id c)
                     (non-determinism-error-event-id c)
                     (non-determinism-error-expected c)
                     (non-determinism-error-actual c)))))

(define-condition not-in-workflow-error (cl-workflow-error)
  ((command :initarg :command :reader not-in-workflow-error-command))
  (:report (lambda (c s)
             (format s "~A called outside of a workflow context"
                     (not-in-workflow-error-command c)))))

(define-condition unknown-activity-error (cl-workflow-error)
  ((activity-name :initarg :activity-name :reader unknown-activity-error-activity-name))
  (:report (lambda (c s)
             (format s "Unknown activity: ~A" (unknown-activity-error-activity-name c)))))

(define-condition no-running-workflow-error (cl-workflow-error)
  ((workflow-id :initarg :workflow-id :reader no-running-workflow-error-workflow-id))
  (:report (lambda (c s)
             (format s "No running workflow for workflow-id ~A"
                     (no-running-workflow-error-workflow-id c)))))

(define-condition workflow-execution-timeout (cl-workflow-error)
  ((workflow-id :initarg :workflow-id :reader workflow-execution-timeout-workflow-id)
   (run-id :initarg :run-id :reader workflow-execution-timeout-run-id))
  (:report (lambda (c s)
             (format s "Workflow ~A (run ~A) exceeded execution timeout"
                     (workflow-execution-timeout-workflow-id c)
                     (workflow-execution-timeout-run-id c)))))
