;;;  -*- lexical-binding: t -*-

(require 'comint)

(defun sqlcmd (host user password &optional database no-select no-display)
  (interactive
   (list (read-from-minibuffer "Host: " (swb--get-default-host))
         (read-from-minibuffer "User: " (swb--get-default-user))
         (read-passwd "Password: " nil (swb--get-default-password))
         (read-from-minibuffer "Database: " (swb--get-default-database))))
  (let* ((name (concat "sqlcmd-" (md5 (concat host user database))))
         (buffer (get-buffer-create name)))
    (unless no-display
      (if no-select
          (display-buffer buffer)
        (pop-to-buffer buffer)))
    (with-current-buffer buffer
      (unless (comint-check-proc buffer)
        (let* ((extra (when database
                        (list "-d" database))))
          (apply 'make-comint-in-buffer
                 name
                 buffer
                 "/opt/mssql-tools/bin/sqlcmd"
                 nil
                 "-S"
                 host
                 "-U"
                 user
                 "-P"
                 password
                 "-s" "|"
                 extra)
          (sqlcmd-mode)))
      buffer)))

(defvar sqlcmd-suppressed-output-sink-function nil)

(defun sqlcmd-maybe-suppress-output (output)
  (if (and sqlcmd-suppressed-output-sink-function
           (let ((m (string-match-p comint-prompt-regexp output)))
             (or (not m)
                 (not (= 0 m)))))
      (progn
        (when (functionp sqlcmd-suppressed-output-sink-function)
          (funcall sqlcmd-suppressed-output-sink-function output))
        "")
    output))

(define-derived-mode sqlcmd-mode comint-mode "sql-cmd"
  "Major mode for the sqlcmd comint buffer."
  (setq comint-prompt-regexp (rx (1+ digit) (any "~" ">")))
  (setq comint-process-echoes nil)
  (add-hook 'comint-preoutput-filter-functions
            'sqlcmd-maybe-suppress-output
            nil t))

(provide 'sqlcmd)
