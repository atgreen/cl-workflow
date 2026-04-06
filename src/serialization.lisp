;;; serialization.lisp -- Serialization via cl-conspack
;;;
;;; SPDX-License-Identifier: MIT
;;;
;;; Copyright (C) 2026 Anthony Green

(in-package #:cl-workflow)

(defun serialize (value)
  "Serialize VALUE to a byte vector using conspack."
  (cpk:encode value))

(defun deserialize (bytes)
  "Deserialize BYTES (a byte vector) back to a Lisp value."
  (when bytes
    (cpk:decode bytes)))
