(defvar org-babel-default-header-args:workbench
  `((:with-header . "no"))
  "Default arguments for evaluating a swb block.")

(defun org-babel-execute:workbench (body params)
  (message "%s" params)
  (let ((query (org-babel-expand-body:sql body params))
        (connection swb-connection)
        (with-header (equal "yes" (cdr (assq :with-header params)))))
    (swb-query-fetch-tuples connection query with-header)))

(defvar org-babel-default-header-args:swb
  `((:results . "value"))
  "Default arguments for evaluating a swb block.")

(defun org-babel-execute:swb (body params)
  (let* ((work-buffer (cdr (assoc :session params)))
         (connection (with-current-buffer work-buffer swb-connection))
         (result nil)
         (result-buffer (generate-new-buffer "*result*")))
    (with-current-buffer work-buffer
      (swb-query-format-result
       connection
       (swb--expand-columns-in-select-query (s-trim body))
       result-buffer
       (lambda (status)
         (funcall (swb--result-callback connection body) status)
         (let ((raw-result
                (buffer-substring-no-properties
                 (save-excursion
                   (goto-char (point-min))
                   (forward-line)
                   (point))
                 (save-excursion
                   (goto-char (point-max))
                   (forward-line -1)
                   (point)))))
           (setq result (org-table-to-lisp raw-result))))))
    (while (not result)
      (sit-for 0.25))
    (with-selected-window (get-buffer-window result-buffer)
      (if (< 0 (buffer-size (current-buffer)))
          (kill-buffer-and-window)
        (kill-buffer result-buffer)))
    result))

(provide 'ob-workbench)
