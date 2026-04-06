;;; cl-workflow.asd
;;;
;;; SPDX-License-Identifier: MIT
;;;
;;; Copyright (C) 2026 Anthony Green

(asdf:defsystem #:cl-workflow
  :description "A Common Lisp-native durable workflow engine."
  :author      "Anthony Green"
  :license     "MIT"
  :version     "0.1.0"
  :depends-on (:bordeaux-threads
               :cl-conspack
               :local-time
               :sqlite)
  :serial t
  :components ((:file "src/package")
               (:file "src/conditions")
               (:file "src/serialization")
               (:file "src/persistence")
               (:file "src/registry")
               (:file "src/replay")
               (:file "src/engine")))
