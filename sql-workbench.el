;;; sql-workbench.el --- Send queries to a database and work with results. -*- lexical-binding: t -*-

;; Copyright (C) 2015-2017 Matúš Goljer <matus.goljer@gmail.com>

;; Author: Matúš Goljer <matus.goljer@gmail.com>
;; Maintainer: Matúš Goljer <matus.goljer@gmail.com>
;; Version: 0.0.1
;; Created: 21st July 2015
;; Package-requires: ((dash "2.10.0") (s "1.5.0") (ov "1.0") (shut-up "0.3.2") (json-mode "1.6.0"))
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

;; TODO: Better error handling (font-lock the error)

;;; Code:

(require 'dash)
(require 's)
(require 'ov)
(require 'shut-up)
(require 'json-mode)

(require 'sql)
(require 'org)
(require 'org-table)
(require 'org-src)

(require 'swb-connection-mysql)


;;; Workbench mode
(defgroup sql-workbench ()
  "Workbench for SQL."
  :group 'data
  :prefix "swb-")

(defcustom swb-header-line-format '(" "
                                    (:eval
                                     (if (swb-iconnection-child-p swb-connection)
                                         (concat (swb-get-user swb-connection)
                                                 "@" (swb-get-host swb-connection)
                                                 ":" (number-to-string (swb-get-port swb-connection))
                                                 " -- " (swb-get-database swb-connection))
                                       "No connection"))
                                    "  "
                                    (:eval
                                     (when (swb-iconnection-child-p swb-connection)
                                       (-when-let (queries (swb-get-active-queries swb-connection))
                                         (concat
                                          "Active queries: "
                                          (mapconcat
                                           (lambda (x)
                                             (format "[%s]" (s-trim x)))
                                           queries " "))))))
  "The format expression for sql-workbench's header line.

Has the same format as `mode-line-format'."
  :type 'sexp
  :group 'sql-workbench)
(put 'swb-header-line-format 'risky-local-variable t)

(defcustom swb-show-data-row-page-size 500
  "How many rows should we retrieve with `swb-show-data-in-table'.

This number represents a \"page\" of data.  Each additional page
loaded loads this many rows."
  :type 'integer
  :group 'sql-workbench)

;; These four exist to mirror file-local variables storing the
;; connection info.
(defvar swb-host nil "String determining host.")
(defvar swb-port nil "Number determining port.")
(defvar swb-user nil "String determining user.")
(defvar swb-database nil "String determining database.")

;; TODO: move these state variables into a defstruct.
;; TODO: remove this variable?
(defvar swb-result-buffer nil
  "Result buffer for this workbench.")
(defvar swb-count 0 ;; only makes sense for the "data" views.
  "Number of items in the current table.")
(defvar swb-connection nil
  "Connection to the server for this workbench.")
;; TOOD: make this into a ring.
(defvar swb-query nil
  "Last executed query for this result buffer.")
(defvar swb-metadata nil
  "Metadata for the last returned result set.")
(put 'swb-metadata 'permanent-local t)

(defun swb--get-default-host ()
  "Get default host for this buffer.

First look if there is a connection.  If so, reuse.

Then look at the local variable `swb-host'.

If nothing is found, return nil."
  (cond
   ((swb-iconnection-child-p swb-connection)
    (swb-get-host swb-connection))
   (swb-host)
   (t nil)))

(defun swb--get-default-port ()
  "Get default port for this buffer.

First look if there is a connection.  If so, reuse.

Then look at the local variable `swb-port'.

If nothing is found, return nil."
  (cond
   ((swb-iconnection-child-p swb-connection)
    (swb-get-port swb-connection))
   (swb-port (string-to-number swb-port))
   (t nil)))

(defun swb--get-default-user ()
  "Get default user for this buffer.

First look if there is a connection.  If so, reuse.

Then look at the local variable `swb-user'.

If nothing is found, return nil."
  (cond
   ((swb-iconnection-child-p swb-connection)
    (swb-get-user swb-connection))
   (swb-user)
   (t nil)))

(defun swb--get-default-database ()
  "Get default database for this buffer.

First look if there is a connection.  If so, reuse.

Then look at the local variable `swb-database'.

If nothing is found, return nil."
  (cond
   ((swb-iconnection-child-p swb-connection)
    (swb-get-database swb-connection))
   (swb-database)
   (t nil)))

(defun swb--read-connection (connection-constructor)
  "Read connection data.

CONNECTION-CONSTRUCTOR is a constructor to create temporary
connection when we query for the list of database."
  (let* ((host (read-from-minibuffer "Host: " (swb--get-default-host)))
         (port (read-from-minibuffer "Port: " (--when-let (swb--get-default-port) (number-to-string it))))
         (user (read-from-minibuffer "User: " (swb--get-default-user)))
         (password (read-passwd "Password: "))
         (database (completing-read "Database: "
                                    (swb-get-databases
                                     (funcall connection-constructor "temp" :host host :port (string-to-number port) :user user :password password))
                                    nil t nil nil (swb--get-default-database))))
    (list host (string-to-number port) user password database)))

;; TODO: add a list of named predefined connections I could pick
;; instead of host (like an alias which would expand to the other
;; settings)
;; TODO: make this generic/connection type independent
(defun swb-reconnect (host port user password database)
  "Reconnect this workbench.

HOST, PORT, USER, PASSWORD and DATABASE are connection details."
  (interactive (swb--read-connection 'swb-connection-mysql))
  (let* ((connection (swb-connection-mysql (buffer-name) :host host :port port :user user :password password :database database)))
    (set (make-local-variable 'swb-connection) connection)))

(defun swb-maybe-connect ()
  "If there is no active conncetion, try to (re)connect."
  (unless swb-connection
    (call-interactively 'swb-reconnect)))

(defun swb-clone ()
  "Clone this workbench.

Open new clean workbench with the same connection details."
  (interactive)
  (let* ((buffer-name (generate-new-buffer-name "*swb-workbench*"))
         (connection (clone swb-connection)))
    ;; TODO: abstract this piece and the same code in swb-new-workbench-mysql
    (with-current-buffer (get-buffer-create buffer-name)
      (swb-mode)
      (set (make-local-variable 'swb-connection) connection)
      (pop-to-buffer (current-buffer)))))

;; TODO: add a function to change the active database
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


(defun swb-get-query-bounds-at-point ()
  "Get the bounds of query at point."
  (let ((beg (save-excursion
               (condition-case _err
                   (progn
                     (while (not (and (re-search-backward ";")
                                      (not (nth 4 (syntax-ppss))))))
                     (1+ (point)))
                 (error (point-min)))))
        (end (save-excursion
               (condition-case _err
                   (progn
                     (while (not (and (re-search-forward ";")
                                      (not (nth 4 (syntax-ppss))))))
                     (point))
                 (error (point-max))))))
    (cons beg end)))

;; TODO: this might be connection-specific too, so we should probably
;; move it to the class
(defun swb-get-query-at-point ()
  "Get query at point."
  (-let (((beg . end) (swb-get-query-bounds-at-point)))
    (buffer-substring-no-properties beg end)))

(defun swb--get-result-buffer ()
  "Return the result buffer for this workbench."
  (if (buffer-live-p swb-result-buffer)
      swb-result-buffer
    (if (string-match-p "workbench" (buffer-name))
        (get-buffer-create (replace-regexp-in-string "workbench" "result" (buffer-name)))
      (get-buffer-create (concat "*swb-result-" (buffer-name) "*")))))

(defun swb--result-callback (connection query &optional point source-buffer)
  "Return a result callback.

This callback should be called in the result buffer after it has
received the result set and after this was properly formatted.

CONNECTION is the connection of the server where the result was
obtained from.

QUERY is the query which produced this result.

WARNING: calling this function does nothing except return another
function."
  (lambda (status)
    (swb-result-mode)
    (setq-local swb-connection connection)
    (setq-local swb-query query)
    ;; TODO: this should be done in the connection handler,
    ;; `swb-mysql--format-result-sentinel'.  We can't do it right now
    ;; because the query is not know at that time in the mysql
    ;; handler.  Create an object "query" to abstract this?
    (swb-set-active-queries
     connection
     (-remove-item query (swb-get-active-queries connection)))
    (goto-char (point-min))
    (when (and status
               (< 0 (buffer-size (current-buffer))))
      (let ((window (display-buffer (current-buffer))))
        (with-selected-window window
          ;; decide here if we want to inline the result or let it be
          ;; in a separate window
          (let* ((num-cols (length (swb--result-get-column-names)))
                 (rows (save-excursion
                         (org-table-goto-column 1)
                         (swb--get-column-data)))
                 (inlinep (and (= num-cols 1)
                               (< (length rows) 8))))
            (if (and source-buffer
                     point
                     inlinep)
                (progn
                  (with-current-buffer source-buffer
                    (save-excursion
                      (goto-char point)
                      (-let (((_ . end) (swb-get-query-bounds-at-point)))
                        (goto-char end)
                        (when (looking-at " -- => \\(.*\\);")
                          (delete-region (point) (match-end 0)))
                        (insert (format " -- => %s;"
                                        (let ((data (-map 's-trim (-flatten rows))))
                                          (if (= (length rows) 1)
                                              (car data)
                                            (s-join ", " data))))))))
                  (kill-buffer-and-window))
              (set-window-point window (point-min))
              (forward-line 3)
              (swb-result-forward-cell 1)
              ;; make sure there is no gap... this moves the point to the
              ;; 4th visible line of the window
              (recenter 4))))))))

(defun swb-query-display-result (query buffer &optional point source-buffer)
  "Display result of QUERY in BUFFER.

POINT is current point in the workbench buffer."
  (interactive)
  (swb-query-format-result
   swb-connection query buffer
   (swb--result-callback swb-connection query point source-buffer)))

;; TODO: add something to send multiple queries (region/buffer).  If a
;; region is active, send the region instead of the query.
;; TODO: figure out how to show progress bar (i.e. which query is being executed ATM)
;; TODO: warn before sending unsafe queries
;; TODO: add a version which replaces the SELECT clause with count(*)
;; so you can see only the number of results
(defun swb-send-current-query (&optional new-result-buffer)
  "Send the query under the cursor to the connection of current buffer.

If NEW-RESULT-BUFFER is non-nil, display the result in a separate buffer."
  (interactive "P")
  (swb-maybe-connect)
  ;; TODO: move this `new-result-buffer' directly into
  ;; `swb--get-result-buffer'
  (let ((buffer (if new-result-buffer
                    (generate-new-buffer "*result*")
                  (swb--get-result-buffer)))
        (query (swb-get-query-at-point)))
    (swb-query-display-result query buffer (point) (current-buffer))))

(defun swb--read-table ()
  "Completing read for a table."
  (swb-maybe-connect)
  (let* ((tables (swb-get-tables swb-connection))
         (default (or (--when-let (symbol-at-point)
                        (let ((name (symbol-name it)))
                          (when (member name tables) name)))
                      (car tables))))
    (completing-read "Table: " tables nil t nil nil default)))

;; TODO: open to new window when called with C-u
;; TODO: make this into a generic method
;; TODO: add an option to load additional pages of data
;; TODO: add an option to "hyperlink" to another table using the id at
;; point.  For example, with a point at a column foreing_key of table
;; tags, we should run "select * from tags where foreing_key =
;; <value-under-point>".  We should also ask for additional where
;; constraints if called with C-u?  We should also support the
;; convention that foreign_table_id links to the column id in
;; foreign_table if it is not marked as foreign in the table.
(defun swb-show-data-in-table (table)
  "Show data in TABLE.

Limits to `swb-show-data-row-page-size' lines of output."
  (interactive (list (swb--read-table)))
  (let ((query (format "SELECT * FROM `%s` LIMIT %d;" table swb-show-data-row-page-size))
        (buffer (get-buffer-create (format "*data-%s*" table)))
        (connection swb-connection))
    (swb-query-format-result
     connection query buffer
     (lambda (status)
       ;; Note: we don't need to keep a closure here because the outer
       ;; lambda already makes a closure.  However, the function is
       ;; most often called directly to produce the callback closure
       ;; and not *from* another closure.  Therefore, the code looks a
       ;; bit redundant here, but simplifies the call in most other
       ;; places.
       (funcall (swb--result-callback connection query) status)
       (setq-local swb-count
                   (string-to-number
                    (swb-query-fetch-one
                     connection
                     (format "SELECT COUNT(*) FROM `%s`;" table))))))))

(defun swb-show-number-of-rows-in-table (table)
  "Echo number of rows in TABLE."
  (interactive (list (swb--read-table)))
  (let ((n (swb-query-fetch-one
            swb-connection
            (format "SELECT COUNT(*) FROM `%s`;" table))))
    (message "SWB: %s rows in %s"
             (propertize (swb-format-number n) 'face 'font-lock-constant-face)
             (propertize table 'face 'font-lock-comment-face))))

;; TODO: make this into a generic method
;; TODO: add a version to get `show create table'
;; TODO: show index from <table> shows more detailed information about
;; keys, maybe we could merge this and the `describe table' outputs
;; into one?
;; TODO: add "table editor", ideally from the describe window we
;; should be able to add columns and it should give us preview of
;; "alter table" code which we could then submit (and it would refresh
;; the description)
(defun swb-describe-table (table)
  "Describe TABLE schema."
  (interactive (list (swb--read-table)))
  (swb-query-display-result (format "DESCRIBE `%s`;" table)
                            (get-buffer-create (format "*schema-%s*" table))))

(defun swb-store-connection-to-file ()
  "Store connection details as file-local variables."
  (interactive)
  (save-excursion
    (when (swb-iconnection-child-p swb-connection)
     (add-file-local-variable 'swb-host (swb-get-host swb-connection))
     (add-file-local-variable 'swb-port (number-to-string (swb-get-port swb-connection)))
     (add-file-local-variable 'swb-user (swb-get-user swb-connection))
     (add-file-local-variable 'swb-database (swb-get-database swb-connection)))))

;; TODO: add function to explain current query
;; TODO: add function to list all tables/objects in the database
;; TODO: add something to navigate queries (beg/end-of-defun style)
(defvar swb-mode-map
  (let ((map (make-sparse-keymap)))
    (set-keymap-parent map sql-mode-map)
    (define-key map (kbd "C-c C-d") 'swb-show-data-in-table)
    (define-key map (kbd "C-c C-t") 'swb-describe-table)
    (define-key map (kbd "C-c C-c") 'swb-send-current-query)
    (define-key map (kbd "C-c C-r") 'swb-reconnect)
    (define-key map (kbd "C-c C-s") 'swb-store-connection-to-file)
    (define-key map (kbd "C-c C-n") 'swb-show-number-of-rows-in-table)
    map)
  "Keymap for swb mode.")

;; TODO: add command to switch to a different database on the same host
(define-derived-mode swb-mode sql-mode "SWB"
  "Mode for editing SQL queries."
  (use-local-map swb-mode-map)
  (setq header-line-format swb-header-line-format)
  (when (featurep 'flycheck)
    (flycheck-add-mode 'sql-sqlint 'swb-mode))
  (set (make-local-variable 'swb-result-buffer) (swb--get-result-buffer)))

;;;###autoload (add-to-list 'auto-mode-alist '("\\.swb\\'" . swb-mode))


;;; Result mode

(defun swb-format-number (n &optional delim)
  "Format a number by adding thousand delimiters."
  (setq delim (or delim " "))
  (let ((n (cond
            ((and (stringp n)
                  (integerp (string-to-number n)))
             (string-to-number n))
            ((integerp n) n)
            (t (error "Not an integer number.")))))
    (mapconcat
     'identity
     (--map (apply 'string it)
            (-map 'reverse
                  (nreverse
                   (-partition-all
                    3
                    (nreverse
                     (string-to-list
                      (number-to-string n)))))))
     delim)))

;; TODO: shrink headers on wide columns (for example a column full of
;; zeroes with long name takes up too much space) and put the current
;; column name in the modeline.
(defun swb--make-header-overlay (window ov-start)
  "Put a header line at the top of the result buffer.

WINDOW is the window, OV-START is the first visible point in
WINDOW."
  (ov-clear 'swb-floating-header)
  (when (> ov-start (point-min))
    (let ((ov (make-overlay 0 1))
          (str (buffer-substring-no-properties
                (point-min)
                (save-excursion
                  (goto-char (point-min))
                  (forward-line 3)
                  (forward-char 1)
                  (point)))))
      (overlay-put ov 'swb-floating-header t)
      (overlay-put ov 'display str)
      (move-overlay ov ov-start (1+ ov-start)))))

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
;; TODO: why does this return a list of singletons? Why not just a
;; list.
;; TODO: do we want multi-column support?
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

(defun swb-result-copy-row-sql ()
  "Copy current row as SQL values clause."
  (interactive)
  (save-excursion
    (save-restriction
      (narrow-to-region (line-beginning-position) (line-end-position))
      (let* ((data (car (org-table-to-lisp)))
             (typed-data (-map (-lambda ((type . data))
                                 ;; TODO: this is very crude
                                 (if (string-match-p
                                      (regexp-opt
                                       (list
                                        "STRING"
                                        "DATE"
                                        "DATETIME"
                                        "BLOB"
                                        )) type)
                                     (format "'%s'" data)
                                   data))
                               (-zip (--map (plist-get (cdr it) :type) swb-metadata) data))))
        (kill-new (format "(%s)" (mapconcat 'identity typed-data ", ")))))))

(defun swb--result-get-column-names (&optional n)
  "Return all the columns in the result.

If optional argument N is set get the name of nth column."
  (save-excursion
    (goto-char (point-min))
    (forward-line 1)
    (let* ((header (buffer-substring-no-properties (line-beginning-position) (line-end-position)))
           (columns (-map 's-trim (split-string header "|" t))))
      (if n (nth n columns) columns))))

(defun swb-result-forward-cell (&optional arg)
  "Go forward ARG cells."
  (interactive "p")
  (setq arg (or arg 1))
  (org-table-goto-column (+ arg (org-table-current-column)))
  (skip-syntax-forward " ")
  (when (looking-at-p "^|")
    (forward-char 1))
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
  (cond
   ((<= (line-number-at-pos) 3)
    (let ((cc (current-column)))
      (goto-char (point-min))
      (forward-line 3)
      (forward-char cc)
      (re-search-backward "|")
      (forward-char 1))
    (skip-syntax-forward " "))
   ((not (save-excursion
           (beginning-of-line 2)
           (or (not (org-at-table-p))
               (org-at-table-hline-p))))
    (let ((cc (org-table-current-column)))
      (beginning-of-line 2)
      (org-table-goto-column cc)
      (skip-syntax-forward " ")))))

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

(defun swb-beginning-of-buffer ()
  "Go to the first line of the result set."
  (interactive)
  (let ((cc (org-table-current-column)))
    (goto-char (point-min))
    (forward-line 3)
    (org-table-goto-column cc)))

(defun swb-end-of-buffer ()
  "Go to the last line of the result set."
  (interactive)
  (let ((cc (org-table-current-column)))
    (goto-char (point-max))
    (forward-line -2)
    (org-table-goto-column cc)))

(defun swb-beginning-of-line ()
  "Go to the first column of current row."
  (interactive)
  (org-table-goto-column 1)
  (skip-syntax-forward " "))

(defun swb-end-of-line ()
  "Go to the last column of current row."
  (interactive)
  (org-table-goto-column (length (swb--result-get-column-names)))
  (skip-syntax-forward " "))

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
  (swb-query-display-result swb-query (current-buffer)))

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
             (res (/ (float (apply '+ numbers)) (length numbers)))
             (sres (if (= org-timecnt 0)
                       (format "%.3f" res)
                     (setq diff (* 3600 res)
                           h (floor (/ diff 3600)) diff (mod diff 3600)
                           m (floor (/ diff 60)) diff (mod diff 60)
                           s diff)
                     (format "%.0f:%02.0f:%02.0f" h m s))))
        (kill-new sres)
        (if (org-called-interactively-p 'interactive)
            (message "%s"
                     (substitute-command-keys
                      (format "Average of %d items: %-20s     (\\[yank] will insert result into buffer)"
                              (length numbers) sres))))
        sres))))

;; TODO: read the actual foreign key metadata from table structure
(defun swb-result-follow-foreign-key ()
  "Follow the foreign key under point.

If the foreign key is not declared we try to guess the base table
name from the column name by dropping the _id suffix."
  (interactive)
  (let* ((col-name (swb-get-metadata :name (org-table-current-column))))
    (when (string-suffix-p "_id" col-name)
      (let ((base-table (replace-regexp-in-string "_id\\'" "" col-name))
            (val (s-trim (save-excursion (org-table-get-field)))))
        (swb-query-display-result
         (format "SELECT * FROM %s WHERE id = %s" base-table val)
         (current-buffer))))))

;; TODO: pridat podporu na editovanie riadkov priamo v result sete
;; TODO: add helpers to add rows to the table (M-RET)
;; TODO: add font-locking
;; - query the server for types of columns
;;   - distinguish dates, numbers, strings, blobs (we should also shorten these somehow!), nulls
;;   - primary keys in bold
(defvar swb-result-mode-map
  (let ((map (make-sparse-keymap)))
    (set-keymap-parent map org-mode-map)
    (define-key map [remap beginning-of-buffer] 'swb-beginning-of-buffer)
    (define-key map [remap beginning-of-line] 'swb-beginning-of-line)
    (define-key map [remap end-of-buffer] 'swb-end-of-buffer)
    (define-key map [remap end-of-line] 'swb-end-of-line)
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
    ;; TODO: put this under a nested map so we can have multiple
    ;; export types
    (define-key map "c" 'swb-copy-column-csv)
    (define-key map "r" 'swb-result-copy-row-sql)
    (define-key map "e" 'swb-result-show-cell)
    (define-key map (kbd "C-c C-c") 'swb-result-submit)
    (define-key map "q" 'quit-window)
    (define-key map (kbd "<return>") 'swb-result-follow-foreign-key)
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

;; TODO: make column optional and grab it from current buffer?
(defun swb-get-metadata (property column)
  "Get metadata PROPERTY for COLUMN.

Column starts at 1."
  (if (eq property :name)
      (car (nth (1- column) swb-metadata))
    (plist-get (cdr (nth (1- column) swb-metadata)) property)))

(defun swb-result-fontify-cell ()
  "Fontify cell."
  (backward-char 1)
  ;; TODO: put "column" property on the text?
  (let ((cc (org-table-current-column)))
    (when (< 0 cc)
      (let* ((current-type (swb-get-metadata :type cc))
             ;; TODO: precompute this
             (face (cond
                    ((string-match-p "LONG\\|TINY" current-type)
                     font-lock-builtin-face)
                    ((string-match-p "DOUBLE" current-type)
                     font-lock-keyword-face)
                    ((string-match-p "STRING" current-type)
                     font-lock-string-face)
                    ((string-match-p "DATE" current-type)
                     font-lock-function-name-face))))
        (if (> (line-number-at-pos) 3)
            face
          'org-table)))))

(defun swb-result-fontify-json (limit)
  "Fontify cells which appear to hold JSON content with `json-mode'."
  (while (re-search-forward (rx "|" (1+ " ") (or "{" "[{") 34) limit t)
    (let ((type (swb-get-metadata :type (org-table-current-column))))
      (when (string-match-p type (regexp-opt (list "STRING" "BLOB")))
        (org-src-font-lock-fontify-block
         'json
         (save-excursion (org-table-beginning-of-field 1) (point))
         (save-excursion (org-table-end-of-field 1) (point)))))))

(defvar-local swb-result-cell-position nil
  "Position in the result buffer corresponding to the cell being edited.")

(defvar-local swb-result-pending-updates nil
  "List of pending updates for this result buffer.")

;; TODO: this is not safe if we do not pull all the columns as it
;; might only include one of the composite keys
(defun swb--get-primary-keys ()
  "Get the indices of primary keys of the current result set.

Note that these might not represent the full key of the table."
  (-find-indices (-lambda ((_ &keys :flags flags))
                   (and flags (string-match-p "PRI_KEY" flags)))
                 swb-metadata))

(defun swb-cell-edit-submit-result ()
  (interactive)
  (let ((target-point swb-result-cell-position)
        (replacement-value-raw (buffer-string))
        (replacement-value-table (json-read-from-string (buffer-string))))
    (with-current-buffer swb-result-buffer
      (save-excursion
        (goto-char target-point)
        (let ((inhibit-read-only t))
          (org-table-get-field nil (format " %s " replacement-value-table))
          (org-table-align)))
      (goto-char (set-window-point (get-buffer-window (current-buffer)) target-point))
      (let ((primary-keys (swb--get-primary-keys))
            (row (car
                  (save-excursion
                    (save-restriction
                      (narrow-to-region (line-beginning-position) (line-end-position))
                      ;; TODO: this has to be replaced by something
                      ;; that is aware of the underlying types.  We
                      ;; should probably add some better abstraction
                      ;; for working with the result tables.
                      (org-table-to-lisp))))))
        (push (list :keys (--zip-with (list :name it :value other)
                                      (-select-by-indices primary-keys (swb--result-get-column-names))
                                      (-select-by-indices primary-keys row))
                    :name (swb--result-get-column-names (1- (org-table-current-column)))
                    :value replacement-value-raw)
              swb-result-pending-updates)))
    (remove-hook 'kill-buffer-hook 'swb-cell-edit-cancel 'local)
    (kill-buffer-and-window)))

(defun swb-cell-edit-cancel ()
  (interactive))

(defvar swb-cell-edit-mode-map
  (let ((map (make-sparse-keymap)))
    (set-keymap-parent map json-mode-map)
    (define-key map (kbd "C-c C-s") 'swb-cell-edit-submit-result)
    (define-key map (kbd "C-x C-s") 'swb-cell-edit-submit-result)
    (define-key map (kbd "C-c C-c") 'swb-cell-edit-submit-result)
    (define-key map (kbd "C-c C-k") 'swb-cell-edit-cancel)
    map))

(define-derived-mode swb-cell-edit-mode json-mode "Swb cell edit"
  "Mode for displaying and editing result cells.")

(defun swb-result-show-cell ()
  "Open the cell in a separate window for editation.

The buffer is opened in `swb-cell-edit-mode'.

No edits or changes to the content of this buffer are reflected
back in the database or the result view.  This command merely
presents a convenient way to work with the value of the current
cell in a separate buffer."
  (interactive)
  (let ((content (s-trim (save-excursion (org-table-get-field))))
        (type (swb-get-metadata :type (org-table-current-column)))
        (result-buffer (current-buffer))
        (result-point (point)))
    (pop-to-buffer
     (with-current-buffer (get-buffer-create "*swb-result-edit-cell*")
       (erase-buffer)
       (insert
        (cond
         ((string-match-p (rx (or "STRING" "DATE")) type)
          (format "\"%s\"" content))
         (t content)))
       (swb-cell-edit-mode)
       (json-mode-beautify)
       (setq-local swb-result-buffer result-buffer)
       (setq-local swb-result-cell-position result-point)
       (add-hook 'kill-buffer-hook 'swb-cell-edit-cancel nil 'local)
       (current-buffer)))))

(defun swb--result-generate-update-for-row (table key data)
  "Generate update query in TABLE for row matching KEY to DATA."
  (let ((key-string
         (mapconcat
          (-lambda ((&plist :name name
                            :value value))
            (format "`%s` = %s" name value))
          key
          " AND "))
        (data-string
         (mapconcat
          (-lambda ((&plist :name name
                            :value value))
            (format "`%s` = %s" name value))
          data
          ", ")))
    (format "UPDATE `%s` SET %s WHERE %s" table data-string key-string)))

(defun swb-result-update-table ()
  "Generate update queries for all the pending changes in the current buffer."
  (let* ((update-data
          (-map (-lambda ((key . data))
                  (cons key (-map 'cadr (-group-by (-lambda ((&plist :name name)) name)
                                                   (-map 'cddr data)))))
                (-group-by (-lambda ((&plist :keys keys)) keys) swb-result-pending-updates)))
         (table (plist-get (cdar swb-metadata) :original-table))
         (queries (-map (-lambda ((key . data))
                          (swb--result-generate-update-for-row table key data))
                        update-data)))
    queries))

(defun swb-result-submit ()
  "Execute all pending updates in the current result buffer."
  (interactive)
  (let ((queries (swb-result-update-table)))
    (-each queries
      (lambda (query)
        (when (y-or-n-p (format "Execute query: %s" query))
          (swb-query-format-result
           swb-connection query (generate-new-buffer " *swb-temp*")
           (lambda (status) (kill-buffer))))))
    (setq-local swb-result-pending-updates nil)))

;; TODO: implement "query ring" so we can back and forth from the
;; result buffer itself.
(define-derived-mode swb-result-mode org-mode "Swb result"
  "Mode for displaying results of sql queries."
  (read-only-mode 1)
  (set (make-local-variable 'org-mode-hook) nil)
  (setq mode-line-format '((10 (:eval (format "(%d,%d)"
                                              (- (line-number-at-pos) 3)
                                              (org-table-current-column))))
                           "%b"
                           (:eval (when (use-region-p)
                                    (format "     (Sum: %s, Avg: %s)" (org-table-sum) (swb-org-table-avg))))
                           (:eval (when swb-count
                                    (format "     (%s rows of %s total)"
                                            (swb-format-number (min swb-count swb-show-data-row-page-size))
                                            (swb-format-number swb-count))))))
  (use-local-map swb-result-mode-map)
  (font-lock-add-keywords
   nil
   '((" \\(.+?\\) |"
      (1 (swb-result-fontify-cell) t))
     ("|\\( *?NULL *\\)"
      (1 '(:background "#e6a8df" :foreground "black") t))
     (swb-result-fontify-json))
   :append)
  (add-hook 'window-scroll-functions 'swb--make-header-overlay nil t)
  (visual-line-mode -1)
  (shut-up (toggle-truncate-lines 1)))

(provide 'sql-workbench)
;;; sql-workbench.el ends here
