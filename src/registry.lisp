;;; registry.lisp -- Workflow, activity, and query registries + macros
;;;
;;; SPDX-License-Identifier: MIT
;;;
;;; Copyright (C) 2026 Anthony Green

(in-package #:cl-workflow)

;;; ─── Retry Policy ──────────────────────────────────────────────────────────

(defstruct retry-policy
  (max-attempts 1 :type fixnum)
  (initial-interval 1 :type number)
  (backoff-coefficient 2.0 :type number)
  (max-interval 60 :type number)
  (non-retryable-error-types nil :type list))

(defparameter +default-retry-policy+
  (make-retry-policy))

;;; ─── Activity Registration ─────────────────────────────────────────────────

(defstruct activity-def
  (name nil :type symbol)
  (function nil :type function)
  (retry-policy +default-retry-policy+ :type retry-policy)
  (timeout nil :type (or null number)))

(defvar *activity-registry* (make-hash-table :test 'equal)
  "Map from activity name (string, upcased) to activity-def.")

(defun activity-key (name)
  "Normalize an activity name to a lookup key (uppercase string)."
  (etypecase name
    (symbol (symbol-name name))
    (string (string-upcase name))))

(defun register-activity (name function &key retry-policy timeout (allow-redefine nil))
  (let ((key (activity-key name)))
    (when (and (gethash key *activity-registry*) (not allow-redefine))
      (cerror "Redefine activity ~A" "Activity ~A is already registered" name))
    (setf (gethash key *activity-registry*)
          (make-activity-def :name name
                             :function function
                             :retry-policy (or retry-policy +default-retry-policy+)
                             :timeout timeout))))

(defun find-activity (name)
  (or (gethash (activity-key name) *activity-registry*)
      (error 'unknown-activity-error :activity-name name)))

;;; ─── Workflow Registration ─────────────────────────────────────────────────

(defstruct workflow-def
  (name nil :type symbol)
  (function nil :type function))

(defvar *workflow-registry* (make-hash-table :test 'equal)
  "Map from workflow name (string, upcased) to workflow-def.")

(defun workflow-key (name)
  (etypecase name
    (symbol (symbol-name name))
    (string (string-upcase name))))

(defun register-workflow (name function &key (allow-redefine nil))
  (let ((key (workflow-key name)))
    (when (and (gethash key *workflow-registry*) (not allow-redefine))
      (cerror "Redefine workflow ~A" "Workflow ~A is already registered" name))
    (setf (gethash key *workflow-registry*)
          (make-workflow-def :name name :function function))))

(defun find-workflow (name)
  (or (gethash (workflow-key name) *workflow-registry*)
      (error "Unknown workflow: ~A" name)))

;;; ─── Query Handler Registration ────────────────────────────────────────────

(defvar *query-registry* (make-hash-table :test 'equal)
  "Map from (workflow-name-str . query-name-str) to handler function.")

(defun register-query-handler (workflow-name query-name function)
  (setf (gethash (cons (workflow-key workflow-name) (workflow-key query-name))
                 *query-registry*)
        function))

(defun find-query-handler (workflow-name query-name)
  (gethash (cons (workflow-key workflow-name) (workflow-key query-name))
           *query-registry*))

;;; ─── Macros ────────────────────────────────────────────────────────────────

(defun parse-retry-policy-plist (plist)
  "Parse a retry-policy keyword plist into a make-retry-policy form."
  (let ((args '()))
    (loop for (key val) on plist by #'cddr
          do (case key
               (:max-attempts (push `(:max-attempts ,val) args))
               (:initial-interval (push `(:initial-interval ,val) args))
               (:backoff-coefficient (push `(:backoff-coefficient ,val) args))
               (:max-interval (push `(:max-interval ,val) args))
               (:non-retryable-errors
                (push `(:non-retryable-error-types (list ,@(mapcar (lambda (e) `(quote ,e)) val)))
                      args))))
    (let ((flat (reduce #'append (reverse args))))
      `(make-retry-policy ,@flat))))

(defmacro defactivity (name lambda-list &body body)
  "Define and register an activity.

Syntax:
  (defactivity name ((param type) ...)
    [:retry-policy (:max-attempts N :initial-interval N ...)]
    [:timeout N]
    body...)
"
  (let ((retry-policy-form nil)
        (timeout-form nil)
        (doc nil)
        (real-body body))
    ;; Extract optional docstring
    (when (stringp (car real-body))
      (setf doc (car real-body))
      (setf real-body (cdr real-body)))
    ;; Parse keyword options before the body
    (loop while (and (keywordp (car real-body))
                     (cdr real-body))
          do (case (car real-body)
               (:retry-policy
                (setf retry-policy-form (parse-retry-policy-plist (cadr real-body)))
                (setf real-body (cddr real-body)))
               (:timeout
                (setf timeout-form (cadr real-body))
                (setf real-body (cddr real-body)))
               (otherwise (return))))
    (let ((param-names (mapcar #'car lambda-list)))
      `(progn
         (defun ,name ,param-names
           ,@(when doc (list doc))
           ,@real-body)
         (register-activity ',name #',name
                            :retry-policy ,retry-policy-form
                            :timeout ,timeout-form
                            :allow-redefine t)
         ',name))))

(defmacro defworkflow (name lambda-list &body body)
  "Define and register a workflow.

Syntax:
  (defworkflow name ((param type) ...)
    [docstring]
    body...)
"
  (let ((param-names (mapcar #'car lambda-list))
        (doc (when (stringp (car body)) (car body)))
        (real-body (if (stringp (car body)) (cdr body) body)))
    `(progn
       (defun ,name ,param-names
         ,@(when doc (list doc))
         ,@real-body)
       (register-workflow ',name #',name :allow-redefine t)
       ',name)))

(defmacro defquery (workflow-name query-name lambda-list &body body)
  "Define and register a query handler for a workflow type.

Syntax:
  (defquery workflow-name query-name ()
    [docstring]
    body...)
"
  (declare (ignore lambda-list))
  (let ((doc (when (stringp (car body)) (car body)))
        (real-body (if (stringp (car body)) (cdr body) body)))
    `(progn
       (register-query-handler ',workflow-name ',query-name
                               (lambda ()
                                 ,@(when doc (list doc))
                                 ,@real-body))
       ',query-name)))
