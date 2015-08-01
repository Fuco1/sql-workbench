;;; sql-workbench.el --- Send queries to a database and work with results.

;; Copyright (C) 2015 Matúš Goljer <matus.goljer@gmail.com>

;; Author: Matúš Goljer <matus.goljer@gmail.com>
;; Maintainer: Matúš Goljer <matus.goljer@gmail.com>
;; Version: 0.0.1
;; Created: 21st July 2015
;; Package-requires: ((dash "2.10.0") (s "1.5.0"))
;; Keywords: data

;; This program is free software; you can redistribute it and/or
;; modify it under the terms of the GNU General Public License
;; as published by the Free Software Foundation; either version 3
;; of the License, or (at your option) any later version.

;; This program is distributed in the hope that it will be useful,
;; but WITHOUT ANY WARRANTY; without even the implied warranty of
;; MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
;; GNU General Public License for more details.

;; You should have received a copy of the GNU General Public License
;; along with this program. If not, see <http://www.gnu.org/licenses/>.

;;; Commentary:

;; TODO: Add function to clone the current workbench: basically just
;; open a new buffer with the same connection.
;; TODO: Better error handling (font-lock the error)

;;; Code:

(require 'dash)
(require 's)

(require 'sql)
(require 'org)
(require 'org-table)

(require 'swb-connection-mysql)


;;; Workbench mode
(defvar swb-connection nil
  "Connection to the server for this workbench.")

;; TOOD: make this into a ring.
(defvar swb-query nil
  "Last executed query for this workbench.")

(defvar swb-result-buffer nil
  "Result buffer for this workbench.")

(defun swb--read-connection (connection-constructor)
  "Read connection data.

CONNECTION-CONSTRUCTOR is a constructor to create temporary
connection when we query for the list of database."
  (let* ((host (read-from-minibuffer "Host: " (when (swb-connection-p swb-connection) (swb-get-host swb-connection))))
         (port (read-from-minibuffer "Port: " (when (swb-connection-p swb-connection) (number-to-string (swb-get-port swb-connection)))))
         (user (read-from-minibuffer "User: " (when (swb-connection-p swb-connection) (swb-get-user swb-connection))))
         (password (read-passwd "Password: "))
         (database (completing-read "Database: "
                                    (swb-get-databases
                                     (funcall connection-constructor "temp" :host host :port (string-to-number port) :user user :password password))
                                    nil t nil nil (when (swb-connection-p swb-connection) (swb-get-database swb-connection)))))
    (list host (string-to-number port) user password database)))

;; TODO: Add reconnect.  Should take parameters from the
;; file-local-parameters or ask for details.  Basically it's the same
;; as this function but without creating new window
(defun swb-new-workbench-mysql (host port user password database)
  "Create new mysql workbench.

HOST, PORT, USER, PASSWORD and DATABASE are connection details."
  (interactive (swb--read-connection 'swb-connection-mysql))
  (let* ((buffer-name (generate-new-buffer-name "*swb-workbench*"))
         (connection (swb-connection-mysql buffer-name :host host :port port :user user :password password :database database)))
    (with-current-buffer (get-buffer-create buffer-name)
      (swb-mode)
      (set (make-local-variable 'swb-connection) connection)
      (pop-to-buffer (current-buffer)))))

;; TODO: this might be connection-specific too, so we should probably
;; move it to the class
(defun swb-get-query-at-point ()
  "Get query at point."
  (let ((beg (save-excursion
               (condition-case err
                   (progn
                     (while (not (and (re-search-backward ";")
                                      (not (nth 4 (syntax-ppss))))))
                     (point))
                 (error (point-min)))))
        (end (save-excursion
               (condition-case err
                   (progn
                     (while (not (and (re-search-forward ";")
                                      (not (nth 4 (syntax-ppss))))))
                     (point))
                 (error (point-max))))))
    (buffer-substring-no-properties beg end)))

;; TODO: if user renames the window and there's no `workbench' in the
;; name, just append the word `result' instead.
(defun swb--get-result-buffer ()
  "Return the result buffer for this workbench."
  (if (buffer-live-p swb-result-buffer)
      swb-result-buffer
    (if (string-match-p "workbench" (buffer-name))
        (get-buffer-create (replace-regexp-in-string "workbench" "result" (buffer-name)))
      (get-buffer-create (concat "*swb-result-" (buffer-name) "*")))))

;; TODO: add something to send multiple queries (region/buffer)
;; TODO: figure out how to show progress bar (i.e. which query is being executed ATM)
;; TODO: warn before sending unsafe queries
;; TODO: add a version which replaces the SELECT clause with count(*)
;; so you can see only the number of results
(defun swb-send-current-query (&optional new-result-buffer)
  "Send the query under the cursor to the connection of current buffer.

If NEW-RESULT-BUFFER is non-nil, display the result in a separate buffer."
  (interactive "P")
  (swb-query-display-result swb-connection (swb-get-query-at-point)
                            (if new-result-buffer
                                (generate-new-buffer "*result*")
                              (swb--get-result-buffer))))

(defun swb--read-table ()
  "Completing read for a table."
  (completing-read "Table: " (swb-get-tables swb-connection) nil t nil nil (symbol-name (symbol-at-point))))

;; TODO: make this into a generic method
;; TODO: show how many lines in total are in the table (select count
;; (*) from table)
(defun swb-show-data-in-table (table)
  "Show data in TABLE.

Limits to 500 lines of output."
  (interactive (list (swb--read-table)))
  (swb-query-display-result swb-connection (format "SELECT * FROM `%s` LIMIT 500;" table)
                            (get-buffer-create (format "*data-%s*" table))))

;; TODO: make this into a generic method
;; TODO: add a version to get `show create table'
;; TODO: show index from <table> shows more detailed information about
;; keys, maybe we could merge this and the `describe table' outputs
;; into one?
(defun swb-describe-table (table)
  "Describe TABLE schema."
  (interactive (list (swb--read-table)))
  (swb-query-display-result swb-connection (format "DESCRIBE `%s`;" table)
                            (get-buffer-create (format "*schema-%s*" table))))

(defvar swb-mode-map
  (let ((map (make-sparse-keymap)))
    (set-keymap-parent map sql-mode-map)
    (define-key map (kbd "C-c C-d") 'swb-show-data-in-table)
    (define-key map (kbd "C-c C-t") 'swb-describe-table)
    (define-key map (kbd "C-c C-c") 'swb-send-current-query)
    map)
  "Keymap for swb mode.")

;; TODO: add header line (don't forget to set as buffer safe or something)
;; (setq header-line-format '(:eval (concat (swb-get-user swb-connection) "@" (swb-get-host swb-connection) ":" (number-to-string (swb-get-port swb-connection)) " -- " (swb-get-database swb-connection))))
;; TODO: store connection details to .swb files (host, port, user, database, NO PASSWORD!)
;; TODO: add command to switch to a different database on the same host
(define-derived-mode swb-mode sql-mode "SWB"
  "Mode for editing SQL queries."
  (use-local-map swb-mode-map)
  (set (make-local-variable 'swb-result-buffer) (swb--get-result-buffer)))

;;;###autoload (add-to-list 'auto-mode-alist '("\\.swb\\'" . swb-mode))

;; TODO: Add a backend for company.  It should be possible to cache
;; available tables/columns at various levels: never, between queries,
;; only invalidate on user request.


;;; Result mode

(defun swb--get-column-bounds ()
  "Return points in table which span the current column as a rectangle."
  (save-excursion
    (let ((col (org-table-current-column))
          beg end)
      (goto-char (org-table-begin))
      (unless (re-search-forward "^[ \t]*|[^-]" nil t)
        (user-error "No table data"))
      (org-table-goto-column col)
      (setq beg (point))
      (goto-char (org-table-end))
      (unless (re-search-backward "^[ \t]*|[^-]" nil t)
        (user-error "No table data"))
      (org-table-goto-column col)
      (setq end (point))
      (cons beg end))))

;; TODO: pridat podporu na zohladnenie regionu
(defun swb--get-column-data ()
  "Get data of current column."
  (-let* (((beg . end) (swb--get-column-bounds))
          (col-data (org-table-copy-region
                     (save-excursion
                       (goto-char beg)
                       (swb-result-down-cell 2)
                       (point))
                     end)))
    col-data))

;; TODO: pridat podporu na zohladnenie regionu
(defun swb-copy-column-csv ()
  "Put the values of the column into `kill-ring' as comma-separated string."
  (interactive)
  (save-excursion
    (let ((col-data (swb--get-column-data)))
      (kill-new (mapconcat 's-trim (-flatten col-data) ", "))
      (message "Copied %d rows." (length col-data)))))

(defun swb--result-get-column-names ()
  "Return all the columns in the result."
  (save-excursion
    (goto-char (point-min))
    (forward-line 1)
    (let* ((header (buffer-substring-no-properties (line-beginning-position) (line-end-position)))
           (columns (-map 's-trim (split-string header "|" t))))
      columns)))

(defun swb-result-forward-cell (&optional arg)
  "Go forward ARG cells."
  (interactive "p")
  (org-table-goto-column (+ arg (org-table-current-column)))
  (skip-syntax-forward " "))

(defun swb-result-backward-cell (&optional arg)
  "Go forward ARG cells."
  (interactive "p")
  (org-table-goto-column (- (org-table-current-column) arg))
  (skip-syntax-forward " "))

(defun swb-result-up-cell (&optional arg)
  "Go up ARG cells."
  (interactive "p")
  (let ((cc (org-table-current-column)))
    (forward-line (- arg))
    (org-table-goto-column cc)
    (skip-syntax-forward " ")))

(defun swb-result-down-cell (&optional arg)
  "Go down ARG cells."
  (interactive "p")
  (let ((cc (org-table-current-column)))
    (forward-line arg)
    (org-table-goto-column cc)
    (skip-syntax-forward " ")))

(defun swb-result-down-page ()
  "Scroll down half a page of results."
  (interactive)
  (let ((cc (org-table-current-column)))
    (scroll-up)
    (org-table-goto-column cc)))

(defun swb-result-up-page ()
  "Scroll down half a page of results."
  (interactive)
  (let ((cc (org-table-current-column)))
    (scroll-down)
    (org-table-goto-column cc)))

(defun swb-result-jump-to-column (column-name)
  "Jump to column named COLUMN-NAME."
  (interactive (list (completing-read "Column: "
                                      (swb--result-get-column-names)
                                      nil t)))
  (let ((column-number (1+ (--find-index (equal column-name it) (swb--result-get-column-names)))))
    (org-table-goto-column column-number)))

;; TODO: we should be able to edit the query which produced this
;; result and re-run it, possibly in different window
(defun swb-revert ()
  "Revert the current result buffer.

This means rerunning the query which produced it."
  (interactive)
  (swb-query-display-result swb-connection swb-query (current-buffer)))

;; TODO: sort ma blby regexp na datum, berie len timestamp <yyyy-mm-dd>... a napr ignoruje hodiny
(defun swb-sort-rows ()
  "Sort rows of the result table."
  (interactive)
  (unwind-protect
      (progn
        (read-only-mode -1)
        (call-interactively 'org-table-sort-lines))
    (read-only-mode 1)))

;; TODO: this is just copy-pasted `org-table-sum'.  Fix the bloody
;; duplicity!.
(defun swb-org-table-avg (&optional beg end nlast)
  "See `org-table-sum'."
  (interactive)
  (save-excursion
    (let (col (org-timecnt 0) diff h m s org-table-clip)
      (cond
       ((and beg end))   ; beg and end given explicitly
       ((org-region-active-p)
        (setq beg (region-beginning) end (region-end)))
       (t
        (setq col (org-table-current-column))
        (goto-char (org-table-begin))
        (unless (re-search-forward "^[ \t]*|[^-]" nil t)
          (user-error "No table data"))
        (org-table-goto-column col)
        (setq beg (point))
        (goto-char (org-table-end))
        (unless (re-search-backward "^[ \t]*|[^-]" nil t)
          (user-error "No table data"))
        (org-table-goto-column col)
        (setq end (point))))
      (let* ((items (apply 'append (org-table-copy-region beg end)))
             (items1 (cond ((not nlast) items)
                           ((>= nlast (length items)) items)
                           (t (setq items (reverse items))
                              (setcdr (nthcdr (1- nlast) items) nil)
                              (nreverse items))))
             (numbers (delq nil (mapcar 'org-table-get-number-for-summing
                                        items1)))
             (res (apply '+ numbers))
             (sres (if (= org-timecnt 0)
                       (number-to-string res)
                     (setq diff (* 3600 res)
                           h (floor (/ diff 3600)) diff (mod diff 3600)
                           m (floor (/ diff 60)) diff (mod diff 60)
                           s diff)
                     (format "%.0f:%02.0f:%02.0f" h m s))))
        (kill-new sres)
        (if (org-called-interactively-p 'interactive)
            (message "%s"
                     (substitute-command-keys
                      (format "Average of %d items: %-20f     (\\[yank] will insert result into buffer)"
                              (length numbers) (/ (float res) (length numbers))))))
        (/ res (length numbers))))))

;; TODO: pridat podporu na editovanie riadkov priamo v result sete
;; TODO: add helpers to add rows to the table (M-RET)
;; TODO: add font-locking
;; - query the server for types of columns
;;   - distinguish dates, numbers, strings, blobs (we should also shorten these somehow!), nulls
;;   - primary keys in bold
;; TODO: we should display sum and avg of current column/selected
;; region in the modeline always.  Make sure it doesn't lag too much,
;; so only run it on an idle timer, 0.5s or so should be good delay
(defvar swb-result-mode-map
  (let ((map (make-sparse-keymap)))
    (set-keymap-parent map org-mode-map)
    (define-key map "+" 'org-table-sum)
    (define-key map "%" 'swb-org-table-avg)
    (define-key map "f" 'swb-result-forward-cell)
    (define-key map "b" 'swb-result-backward-cell)
    (define-key map "p" 'swb-result-up-cell)
    (define-key map "n" 'swb-result-down-cell)
    (define-key map "g" 'swb-revert)
    (define-key map "j" 'swb-result-jump-to-column)
    (define-key map "s" 'swb-sort-rows)
    ;; TODO: add various export options: line/selection/table/column
    ;; as sql, csv, xml (??)
    ;; TODO: add function to copy the content of current cell
    (define-key map "c" 'swb-copy-column-csv)
    (define-key map "q" 'quit-window)
    (define-key map (kbd "<right>") 'swb-result-forward-cell)
    (define-key map (kbd "<left>") 'swb-result-backward-cell)
    (define-key map (kbd "<up>") 'swb-result-up-cell)
    (define-key map (kbd "<down>") 'swb-result-down-cell)
    (define-key map (kbd "<prior>") 'swb-result-up-page)
    (define-key map (kbd "<next>") 'swb-result-down-page)
    (--each (-map 'number-to-string (number-sequence 0 9))
      (define-key map it 'digit-argument))
    map)
  "Keymap for swb result mode.")

;; TODO: mode line in the result should be customized to show useful information
;; - buffer name (so we know what we're looking at, duh ;)
;; - position of the point in grid (ie active cell)
;; - if region is active, show sum and average of the selected elements
;; - how many rows were returned/affected by this query
;; Ideally make it customizable by the user, but that's only step 2
;; (with custom format string)
;; TODO: the table line with column names should be a "floating"
;; overlay which would always appear as the first line of the buffer
;; (anchored with some silly TP) so that we always see what value a
;; column is
(define-derived-mode swb-result-mode org-mode "Swb result"
  "Mode for displaying results of sql queries."
  (read-only-mode 1)
  (set (make-local-variable 'org-mode-hook) nil)
  (use-local-map swb-result-mode-map)
  (visual-line-mode -1)
  (toggle-truncate-lines 1))

(provide 'sql-workbench)
;;; sql-workbench.el ends here
