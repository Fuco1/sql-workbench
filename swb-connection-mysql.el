;;; swb-connection-mysql.el --- Implementation of connection for MySQL. -*- lexical-binding: t -*-

;; Copyright (C) 2015-2017 Matúš Goljer <matus.goljer@gmail.com>

;; Author: Matúš Goljer <matus.goljer@gmail.com>
;; Maintainer: Matúš Goljer <matus.goljer@gmail.com>
;; Version: 0.0.1
;; Created: 26th July 2015
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

;;; Code:

(require 'dash)
(require 's)

(require 'eieio)
(require 'swb-iconnection)

;; For the interactive sentinel
(declare-function swb-result-mode "sql-workbench")
(declare-function swb-result-forward-cell "sql-workbench")

(defun swb-mysql--prepare-buffer (buffer)
  "Prepare BUFFER to receive the query results."
  (with-current-buffer buffer
    (read-only-mode -1)
    (erase-buffer)
    (set (make-local-variable 'font-lock-defaults) nil)
    (set (make-local-variable 'font-lock-keywords) nil)))

(defun swb-mysql--fix-table-to-org-hline ()
  "Replace the initial and terminal the + in the hline with |."
  (beginning-of-line)
  (delete-char 1)
  (insert "|")
  (end-of-line)
  (delete-char -1)
  (insert "|"))

;; Field   4:  `name`
;; Catalog:    `def`
;; Database:   `test`
;; Table:      `b`
;; Org_table:  `users`
;; Type:       VAR_STRING
;; Collation:  utf8_general_ci (33)
;; Length:     765
;; Max_length: 11
;; Decimals:   0
;; Flags:      NOT_NULL NO_DEFAULT_VALUE

(defun swb-mysql--process-metadata (raw-metadata)
  "Parse metadata."
  (let (r)
    (with-temp-buffer
      (insert raw-metadata)
      (goto-char (point-min))
      (while (re-search-forward "Field.*?`\\(.*?\\)`" nil t)
        (let ((name (match-string-no-properties 1))
              (properties nil))
          (re-search-forward "Database.*?`\\(.*?\\)`" nil t)
          (push :database properties)
          (push (match-string-no-properties 1) properties)
          (re-search-forward "Table.*?`\\(.*?\\)`" nil t)
          (push :table properties)
          (push (match-string-no-properties 1) properties)
          (re-search-forward "Org_Table.*?`\\(.*?\\)`" nil t)
          (push :original-table properties)
          (push (match-string-no-properties 1) properties)
          (re-search-forward "Type:[[:space:]]*\\(.*?\\)$" nil t)
          (push :type properties)
          (push (match-string-no-properties 1) properties)
          (re-search-forward "Flags:[ \t]*\\(.*?\\)$" nil t)
          (push :flags properties)
          (push (match-string-no-properties 1) properties)
          (push (cons name (nreverse properties)) r))))
    (nreverse r)))

(defun swb-mysql--format-result-sentinel (proc state callback)
  "Sentinel for PROC once its STATE is exit.

Format the table so that it is a valid `org-mode' table.

CALLBACK is called after the process has finished."
  ;; TODO: move this cleanup elsewhere, the display code could be
  ;; reused between backends
  (when (or (equal state "finished\n")
            (equal state "exited abnormally with code 1\n"))
    (with-current-buffer (process-buffer proc)
      (goto-char (point-min))
      (delete-region (point) (search-forward "--------------" nil t 2))
      (let* ((raw-metadata (progn
                             (delete-region (point) (progn (skip-syntax-forward " ") (point)))
                             (if (looking-at-p "^Field")
                                 (delete-and-extract-region
                                  (point)
                                  (save-excursion
                                    (when (re-search-forward "^+-" nil t)
                                      (backward-char 2))
                                    (point)))
                               ""))))
        (when (looking-at "^+-")
          (progn
            (swb-mysql--fix-table-to-org-hline)
            (forward-line 2)
            (when (looking-at "^+-")
              (swb-mysql--fix-table-to-org-hline))
            (goto-char (point-max))
            (when (re-search-backward "^+-" nil t)
              (swb-mysql--fix-table-to-org-hline))
            (forward-line 1)))
        ;; here we can be looking at the
        ;; - Query OK
        ;; - ERROR
        ;; - [number] rows in set
        (cond
         ((looking-at-p "^[0-9]+ row")
          (message "%s" (delete-and-extract-region (point) (line-end-position))))
         ((looking-at-p "^Query OK")
          (message "%s" (delete-and-extract-region (point) (line-end-position))))
         ((looking-at-p "^ERROR")
          (message "%s" (delete-and-extract-region (point) (line-end-position)))))
        (delete-region (point) (point-max))
        (setq-local swb-metadata
                    (swb-mysql--process-metadata raw-metadata))
        (when callback (funcall callback (equal state "finished\n")))))))

(defclass swb-connection-mysql (swb-iconnection)
  ((engine :type string :initform "mysql"))
  :documentation
  "Connection implementation for MySQL.")

(defmethod swb-prepare-cmd-args ((connection swb-connection-mysql) query extra-args)
  (-concat extra-args
           (unless (member "-B" extra-args)
             (list "-vv"))
           (list "-A"
                 "--column-type-info"
                 "-e" query
                 "-h" (oref connection host)
                 "-P" (number-to-string (oref connection port))
                 "-u" (oref connection user)
                 (concat "-p" (oref connection password)))
           (when (slot-boundp connection :database)
             (list (oref connection database)) )))

(defmethod swb-query ((this swb-connection-mysql) query buffer &rest args)
  (swb-mysql--prepare-buffer buffer)
  (let* ((cmd-args (swb-prepare-cmd-args this query (plist-get args :extra-args)))
         (proc (apply 'start-process "swb-query" buffer "mysql" cmd-args))
         (sentinel (plist-get args :sentinel)))
    (when sentinel
      (set-process-sentinel proc sentinel))
    buffer))

(defmethod swb-query-synchronously ((this swb-connection-mysql) query buffer &rest args)
  (swb-mysql--prepare-buffer buffer)
  (let* ((cmd-args (swb-prepare-cmd-args this query (plist-get args :extra-args))))
    (apply 'call-process "mysql" nil buffer nil cmd-args)
    buffer))

;; The sentinel is responsible for setting up proper state for the
;; result buffer, such as setting `swb-query' to the current query.
(defmethod swb-query-format-result ((this swb-connection-mysql) query buffer &optional callback)
  (let ((active-queries (swb-get-active-queries this)))
    (push query active-queries)
    (swb-set-active-queries this active-queries)
    (swb-query this query buffer :extra-args '("-t") :sentinel
               (lambda (proc state)
                 (swb-mysql--format-result-sentinel proc state callback)))))

(defconst swb-mysql--batch-switches (list "-B" "-N" "--column-names")
  "Switch to toggle batch-mode.")

(defmethod swb-query-fetch-column ((this swb-connection-mysql) query)
  (with-temp-buffer
    (swb-query-synchronously this query (current-buffer) :extra-args swb-mysql--batch-switches)
    (goto-char (point-min))
    (while (looking-at-p "^mysql:") (kill-region (point) (1+ (line-end-position))))
    (cdr (-map 's-trim (split-string (buffer-string) "\n" t)))))

(defun swb-mysql--fetch-tuples-and-column-names (connection query)
  "Mysql helper for `swb-query-fetch-*'.

Fetch data like `swb-query-fetch-tuples' but as the first item
put a list of column names.

CONNECTION is an instance of `swb-connection-mysql', QUERY is the
SQL query."
  (with-temp-buffer
    (swb-query-synchronously connection query (current-buffer) :extra-args swb-mysql--batch-switches)
    (let* ((rows (--map (-map 's-trim (split-string it "\t" t)) (split-string (buffer-string) "\n" t))))
      (-drop-while (-lambda ((car)) (and (stringp car) (string-prefix-p "mysql:" car))) rows))))

(defmethod swb-query-fetch-tuples ((this swb-connection-mysql) query)
  (cdr (swb-mysql--fetch-tuples-and-column-names this query)))

(defmethod swb-query-fetch-plist ((this swb-connection-mysql) query)
  (-let* (((columns . data) (swb-mysql--fetch-tuples-and-column-names this query))
          (columns (--map (intern (concat ":" it)) columns)))
    (-map (lambda (row)
            (let (r)
              (-zip-with
               (lambda (name datum)
                 (push name r)
                 (push datum r))
               columns row)
              (nreverse r)))
          data)))

(defmethod swb-query-fetch-alist ((this swb-connection-mysql) query)
  (-let* (((columns . data) (swb-mysql--fetch-tuples-and-column-names this query))
          (columns (--map (intern it) columns)))
    (-map (lambda (row)
            (let (r)
              (-zip-with
               (lambda (name datum)
                 (push (cons name datum) r))
               columns row)
              (nreverse r)))
          data)))

(defmethod swb-get-databases ((this swb-connection-mysql))
  (swb-query-fetch-column this "show databases;"))

(defmethod swb-get-tables ((this swb-connection-mysql))
  (swb-query-fetch-column this "show tables;"))

(defmethod swb-get-table-info-query ((this swb-connection-mysql) table)
  (format "describe %s" table))

(defmethod swb-R-get-connection ((this swb-connection-mysql) &optional var)
  (let ((conf (format
               "list(user = %S, password = %S, host = %S, port = %S, dbname = %S)"
               (oref this user)
               (oref this password)
               (oref this host)
               (oref this port)
               (oref this database)))
        (var (or var "swb__con__")))
    (format "%s <- rlang::invoke(dbConnect, c(MariaDB(), %s))" var conf)))

(provide 'swb-connection-mysql)
;;; swb-connection-mysql.el ends here
