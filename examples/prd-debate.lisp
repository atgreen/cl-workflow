;;; prd-debate.lisp -- Collaborative PRD development workflow
;;;
;;; Use case: User describes what they want to build. Two LLM agents
;;; (Claude and Codex) independently draft a PRD, ask the user
;;; clarifying questions, then argue with each other in rounds
;;; until they reach consensus on a final PRD.
;;;
;;; This is a sketch, not runnable code. It demonstrates how the
;;; cl-workflow API would be used for a real agentic workflow.

(in-package #:cl-workflow)

;;; ─── Activities ─────────────────────────────────────────────────────────────

(defactivity claude-complete ((messages list))
  "Call Claude API."
  :retry-policy (:max-attempts 3 :initial-interval 2 :backoff-coefficient 2.0)
  :timeout 120
  (cl-completions:create :model "claude-sonnet-4-20250514"
                         :messages messages
                         :max-tokens 8192))

(defactivity codex-complete ((messages list))
  "Call Codex/OpenAI API."
  :retry-policy (:max-attempts 3 :initial-interval 2 :backoff-coefficient 2.0)
  :timeout 120
  (cl-completions:create :provider :openai
                         :model "o3"
                         :messages messages
                         :max-tokens 8192))

(defactivity notify-user ((workflow-id string) (message string))
  "Send a notification to the user (e.g., Slack, email, terminal)."
  :retry-policy (:max-attempts 2 :initial-interval 1)
  :timeout 30
  (send-notification workflow-id message))

;;; ─── Query Handlers ─────────────────────────────────────────────────────────

;; Let external callers inspect the current state of the debate.
;; These run against the workflow thread's in-memory state at safe points.

(defquery prd-debate phase ()
  "Which phase is the workflow in?"
  *current-phase*)

(defquery prd-debate current-drafts ()
  "Return the latest drafts from both agents."
  (list :claude *claude-draft* :codex *codex-draft*))

(defquery prd-debate debate-log ()
  "Return the full debate history."
  *debate-rounds*)

;;; ─── Workflow ───────────────────────────────────────────────────────────────

(defworkflow prd-debate ((project-description string)
                         (max-debate-rounds integer))
  "Collaborative PRD development with two LLM agents and human-in-the-loop."

  ;; Workflow-local state (survives across commands via replay)
  (let ((*current-phase* :drafting)
        (*claude-draft* nil)
        (*codex-draft* nil)
        (*debate-rounds* '())
        (user-answers '())
        (system-prompt
          (format nil "You are helping write a PRD for the following project:~%~%~A~%~%"
                  project-description)))

    ;; ── Phase 1: Independent Drafts ──────────────────────────────────────────
    ;; Both agents draft a PRD and generate clarifying questions.
    ;; Activities are sequential here (execute-activity blocks), but a future
    ;; version could add parallel activity execution.

    (let ((claude-result
            (execute-activity
             'claude-complete
             :input (list (list
                           (msg :system (concatenate 'string system-prompt
                                  "Write a PRD for this project. Include:
1. A draft PRD with problem, goals, scope, acceptance criteria
2. A list of 3-5 clarifying questions for the user

Respond in JSON: {\"prd\": \"...\", \"questions\": [\"...\"]}"))
                           (msg :user project-description)))))
          (codex-result
            (execute-activity
             'codex-complete
             :input (list (list
                           (msg :system (concatenate 'string system-prompt
                                  "Write a PRD for this project. Include:
1. A draft PRD with problem, goals, scope, acceptance criteria
2. A list of 3-5 clarifying questions for the user

Respond in JSON: {\"prd\": \"...\", \"questions\": [\"...\"]}"))
                           (msg :user project-description))))))

      (setf *claude-draft* (json-field claude-result "prd"))
      (setf *codex-draft* (json-field codex-result "prd"))

      ;; ── Phase 2: User Q&A ─────────────────────────────────────────────────
      ;; Collect questions from both agents, deduplicate, ask the user.

      (setf *current-phase* :questions)

      (let* ((claude-questions (json-field claude-result "questions"))
             (codex-questions (json-field codex-result "questions"))
             (all-questions (deduplicate-questions claude-questions codex-questions)))

        ;; Notify the user and wait for answers via signal
        (execute-activity
         'notify-user
         :input (list (workflow-id) ; hypothetical accessor
                      (format nil "Your PRD drafts are ready. ~D questions need answers.~%~%~{~D. ~A~%~}"
                              (length all-questions)
                              (loop for q in all-questions
                                    for i from 1
                                    collect i collect q))))

        ;; Block until the user sends their answers as a signal.
        ;; In practice, a CLI or web UI would call signal-workflow.
        (let ((answers (workflow-receive "user-answers" :timeout 86400)))
          (unless answers
            ;; User didn't respond within 24h -- proceed with what we have
            (setf answers '()))
          (setf user-answers answers))

        ;; ── Phase 3: Revision with User Answers ─────────────────────────────
        ;; Both agents revise their drafts incorporating the user's answers.

        (setf *current-phase* :revising)

        (let ((context (format nil "User's answers to clarifying questions:~%~A"
                               (format-answers all-questions user-answers))))

          (setf *claude-draft*
                (execute-activity
                 'claude-complete
                 :input (list (list
                               (msg :system system-prompt)
                               (msg :user project-description)
                               (msg :assistant *claude-draft*)
                               (msg :user (concatenate 'string context
                                            "~%~%Revise your PRD based on these answers. Output only the revised PRD."))))))

          (setf *codex-draft*
                (execute-activity
                 'codex-complete
                 :input (list (list
                               (msg :system system-prompt)
                               (msg :user project-description)
                               (msg :assistant *codex-draft*)
                               (msg :user (concatenate 'string context
                                            "~%~%Revise your PRD based on these answers. Output only the revised PRD.")))))))

        ;; ── Phase 4: Debate ──────────────────────────────────────────────────
        ;; The two agents argue over differences until they converge or
        ;; exhaust the round limit.

        (setf *current-phase* :debating)

        (dotimes (round max-debate-rounds)

          ;; Claude critiques Codex's draft
          (let ((claude-critique
                  (execute-activity
                   'claude-complete
                   :input (list (list
                                 (msg :system (concatenate 'string system-prompt
                                        "You are reviewing a PRD written by another agent. Your own draft is provided for reference.
Compare the two critically. List:
1. Specific contradictions, gaps, or ambiguities in their draft
2. Things their draft does better than yours
3. A proposed merged PRD that takes the best of both

Be direct and specific. No pleasantries."))
                                 (msg :user (format nil "YOUR DRAFT:~%~A~%~%THEIR DRAFT:~%~A"
                                                    *claude-draft* *codex-draft*)))))))

                ;; Codex critiques Claude's draft
                (codex-critique
                  (execute-activity
                   'codex-complete
                   :input (list (list
                                 (msg :system (concatenate 'string system-prompt
                                        "You are reviewing a PRD written by another agent. Your own draft is provided for reference.
Compare the two critically. List:
1. Specific contradictions, gaps, or ambiguities in their draft
2. Things their draft does better than yours
3. A proposed merged PRD that takes the best of both

Be direct and specific. No pleasantries."))
                                 (msg :user (format nil "YOUR DRAFT:~%~A~%~%THEIR DRAFT:~%~A"
                                                    *codex-draft* *claude-draft*)))))))

            (push (list :round (1+ round)
                        :claude-critique claude-critique
                        :codex-critique codex-critique)
                  *debate-rounds*)

            ;; Now each agent sees the other's critique and produces a revised draft.
            ;; The prompt explicitly asks for convergence.
            (setf *claude-draft*
                  (execute-activity
                   'claude-complete
                   :input (list (list
                                 (msg :system (concatenate 'string system-prompt
                                        "You are converging on a final PRD. You have seen the other agent's critique of your work.
Produce a revised PRD that addresses valid criticisms. Do not capitulate on points where you are right -- argue back in a brief note, then output the revised PRD.

Format: {\"rebuttal\": \"...\", \"prd\": \"...\", \"converged\": true/false}"))
                                 (msg :user (format nil "Your current draft:~%~A~%~%Their critique:~%~A~%~%Your critique of them:~%~A"
                                                    *claude-draft* codex-critique claude-critique))))))

            (setf *codex-draft*
                  (execute-activity
                   'codex-complete
                   :input (list (list
                                 (msg :system (concatenate 'string system-prompt
                                        "You are converging on a final PRD. You have seen the other agent's critique of your work.
Produce a revised PRD that addresses valid criticisms. Do not capitulate on points where you are right -- argue back in a brief note, then output the revised PRD.

Format: {\"rebuttal\": \"...\", \"prd\": \"...\", \"converged\": true/false}"))
                                 (msg :user (format nil "Your current draft:~%~A~%~%Their critique:~%~A~%~%Your critique of them:~%~A"
                                                    *codex-draft* claude-critique codex-critique))))))

            ;; Check for convergence
            (when (and (json-field *claude-draft* "converged")
                       (json-field *codex-draft* "converged"))
              (return))))

        ;; ── Phase 5: Final Merge ─────────────────────────────────────────────
        ;; One final pass to produce a single document from the two
        ;; (hopefully convergent) drafts.

        (setf *current-phase* :merging)

        (let ((final-prd
                (execute-activity
                 'claude-complete
                 :input (list (list
                               (msg :system "You are producing the final, authoritative PRD from two agent drafts that have been through multiple rounds of debate and revision. Merge them into a single coherent document. Preserve all acceptance criteria and specific technical decisions. Remove any debate artifacts or meta-commentary. Output only the final PRD in markdown.")
                               (msg :user (format nil "DRAFT A:~%~A~%~%DRAFT B:~%~A~%~%Debate history:~%~A"
                                                  (json-field *claude-draft* "prd")
                                                  (json-field *codex-draft* "prd")
                                                  (format-debate-log *debate-rounds*))))))))

          ;; Notify user that the PRD is ready
          (execute-activity
           'notify-user
           :input (list (workflow-id)
                        "Your PRD is ready. Query 'final-prd' to retrieve it."))

          ;; ── Phase 6: User Review Loop ────────────────────────────────────────
          ;; The user can send revision requests via signals. Each one triggers
          ;; another revision pass. Send signal "approve" to finalize.

          (setf *current-phase* :review)

          (loop
            (let ((feedback (workflow-receive "user-feedback" :timeout 604800)))
              (cond
                ((null feedback)
                 ;; No response in 7 days, finalize as-is
                 (return final-prd))

                ((string-equal (getf feedback :action) "approve")
                 (return final-prd))

                (t
                 ;; Revision request
                 (setf final-prd
                       (execute-activity
                        'claude-complete
                        :input (list (list
                                      (msg :system "Revise the PRD based on user feedback. Output only the revised PRD in markdown.")
                                      (msg :user (format nil "CURRENT PRD:~%~A~%~%USER FEEDBACK:~%~A"
                                                         final-prd (getf feedback :text)))))))
                 (execute-activity
                  'notify-user
                  :input (list (workflow-id)
                               "Revised PRD is ready for review.")))))))))))


;;; ─── Helper Functions (deterministic, safe inside workflows) ────────────────

(defun msg (role content)
  (list :role (string-downcase (symbol-name role)) :content content))

(defun json-field (response field)
  "Extract a field from a JSON response string."
  ;; Implementation detail -- parse JSON, extract field
  (declare (ignore response field))
  nil)

(defun deduplicate-questions (qs-a qs-b)
  "Merge and deduplicate two question lists. Deterministic (no I/O)."
  (remove-duplicates (append qs-a qs-b) :test #'string-equal))

(defun format-answers (questions answers)
  "Format Q&A pairs for inclusion in a prompt."
  (with-output-to-string (s)
    (loop for q in questions
          for a in answers
          for i from 1
          do (format s "Q~D: ~A~%A~D: ~A~%~%" i q i (or a "[no answer]")))))

(defun format-debate-log (rounds)
  "Format debate rounds for context."
  (with-output-to-string (s)
    (dolist (round (reverse rounds))
      (format s "=== Round ~D ===~%Claude: ~A~%Codex: ~A~%~%"
              (getf round :round)
              (getf round :claude-critique)
              (getf round :codex-critique)))))


;;; ─── Usage ──────────────────────────────────────────────────────────────────
;;;
;;; ;; Start the workflow
;;; (start-workflow *engine*
;;;                 'prd-debate
;;;                 :workflow-id "prd-my-project"
;;;                 :task-queue "default"
;;;                 :input '("I want to build a Common Lisp durable
;;;                           workflow manager..."
;;;                          5))  ; max 5 debate rounds
;;;
;;; ;; Answer clarifying questions (from CLI, web UI, or another workflow)
;;; (signal-workflow *engine* "prd-my-project" "user-answers"
;;;                  :payload '("SQLite for persistence"
;;;                             "Embedded mode first"
;;;                             "Agentic AI is the primary use case"))
;;;
;;; ;; Check the debate progress
;;; (query-workflow *engine* "prd-my-project" 'phase)
;;; ;; => :DEBATING
;;;
;;; (query-workflow *engine* "prd-my-project" 'debate-log)
;;; ;; => ((:ROUND 1 :CLAUDE-CRITIQUE "..." :CODEX-CRITIQUE "...") ...)
;;;
;;; ;; After review notification, send feedback or approve
;;; (signal-workflow *engine* "prd-my-project" "user-feedback"
;;;                  :payload '(:action "approve"))
;;;
;;; ;; Or request revisions
;;; (signal-workflow *engine* "prd-my-project" "user-feedback"
;;;                  :payload '(:action "revise"
;;;                             :text "Add acceptance criteria for every requirement"))
