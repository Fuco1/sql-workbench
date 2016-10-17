(defun company-swb (command &optional arg &rest ignored)
  (interactive (list 'interactive))
  (cl-case command
    (meta (company-swb--meta arg))
    (annotation (company-swb--annotation arg))
    (interactive (company-begin-backend 'company-swb))
    (prefix (and (eq major-mode 'swb-mode)
                 (company-grab-symbol)))
    (candidates (let* ((tables (my-sql-get-tables (swb-get-query-at-point)))
                       (table-alias (save-excursion
                                      (backward-char (1+ (length arg)))
                                      (company-grab-symbol)))
                       (tables (or (--when-let (--first
                                                (equal (cadr it) table-alias)
                                                tables)
                                     (list it))
                                   tables)))
                  (--filter (string-prefix-p arg it)
                            (-mapcat (-lambda ((table alias))
                                       (--map (propertize
                                               (plist-get it :Field)
                                               'meta table)
                                              (swb-query-fetch-plist
                                               swb-connection
                                               (format "describe %s" table))))
                                     tables))))))

(defun company-swb--meta (candidate)
  (get-text-property 0 'meta candidate))

(defun company-swb--annotation (candidate)
  (format " (%s)" (get-text-property 0 'meta candidate)))

(defun my-sql-get-context (sql position)
  (with-temp-buffer
    (insert sql)
    (goto-char position)
    (when (re-search-backward (regexp-opt
                               (list
                                "from"
                                "join"
                                "select"
                                "delete"
                                "insert"
                                )) nil t)
      (let ((keyword (match-string 0)))
        (pcase keyword
          ("from" 1)
          ("join" 2)
          ("select" 3)
          ("delete" 4)
          ("insert" 5))))))

(defun my-sql-get-tables (sql)
  (let ((keywords (concat
                   "[^`]\\<"
                   (regexp-opt
                    (list "where" "order" "group" "join")) "\\>")))
    (with-temp-buffer
      (insert sql)
      (goto-char (point-min))
      ;; get tables from `from'
      (-when-let (beg (re-search-forward "from" nil t))
        (-when-let (end (or (when (re-search-forward keywords nil t)
                              (match-beginning 0))
                            (point-max)))
          (let* ((tables (buffer-substring-no-properties beg end))
                 (tables (replace-regexp-in-string "[`;]" "" tables))
                 (tables (split-string tables ","))
                 (tables (-map 's-trim tables))
                 (tables (--map (split-string it " \\(as\\)?" t) tables)))
            tables))))))
