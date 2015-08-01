;;; swb-connection-mysql.el --- Implementation of connection for MySQL. -*- lexical-binding: t -*-

;; Copyright (C) 2015 Matúš Goljer <matus.goljer@gmail.com>

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
    (fundamental-mode)))

(defun swb-mysql--prepare-cmd-args (query connection extra-args)
  "Prepare the argument list.

QUERY is the query, CONNECTION is an instance of
`swb-connection-mysql', EXTRA-ARGS are any extra arguments to
pass to the process."
  (-concat extra-args
           (list "-A"
                 "-e" query
                 "-h" (oref connection host)
                 "-P" (number-to-string (oref connection port))
                 "-u" (oref connection user)
                 (concat "-p" (oref connection password)))
           (when (slot-boundp connection :database)
             (list (oref connection database)) )))

(defun swb-mysql--fix-table-to-org-hline ()
  "Replace the initial and terminal the + in the hline with |."
  (beginning-of-line)
  (delete-char 1)
  (insert "|")
  (end-of-line)
  (delete-char -1)
  (insert "|"))

(defun swb-mysql--display-result-sentinel (proc _state query)
  "Pop to buffer with the output of PROC once it finished.

Format the table so that it is a valid `org-mode' table.

QUERY is the current executed query."
  ;; TODO: move this cleanup elsewhere, the display code could be
  ;; reused between backends
  (with-current-buffer (process-buffer proc)
    (goto-char (point-min))
    (when (looking-at "^+-")
      (swb-mysql--fix-table-to-org-hline))
    (forward-line 2)
    (when (looking-at "^+-")
      (swb-mysql--fix-table-to-org-hline))
    (goto-char (point-max))
    (when (re-search-backward "^+-" nil t)
      (swb-mysql--fix-table-to-org-hline))
    (swb-result-mode)
    (setq-local swb-query query)
    (goto-char (point-min))
    (let ((window (display-buffer (current-buffer))))
      (with-selected-window window
        (set-window-point window (point-min))
        (forward-line 3)
        (swb-result-forward-cell 1)
        ;; make sure there is no gap... this moves the point to the
        ;; 4th visible line of the window
        (recenter 4)))))

(defclass swb-connection-mysql (swb-iconnection)
  ()
  :documentation
  "Connection implementation for MySQL.")

(defmethod swb-query ((this swb-connection-mysql) query buffer &rest args)
  (swb-mysql--prepare-buffer buffer)
  (let* ((cmd-args (swb-mysql--prepare-cmd-args query this (plist-get args :extra-args)))
         (proc (apply 'start-process "swb-query" buffer "mysql" cmd-args))
         (sentinel (plist-get args :sentinel)))
    (when sentinel
      (set-process-sentinel proc sentinel))
    buffer))

(defmethod swb-query-synchronously ((this swb-connection-mysql) query buffer &rest args)
  (swb-mysql--prepare-buffer buffer)
  (let* ((cmd-args (swb-mysql--prepare-cmd-args query this (plist-get args :extra-args))))
    (apply 'call-process "mysql" nil buffer nil cmd-args)
    buffer))

;; The sentinel is responsible for setting up proper state for the
;; result buffer, such as setting `swb-query' to the current query.
(defmethod swb-query-display-result ((this swb-connection-mysql) query buffer)
  (swb-query this query buffer :extra-args '("-t") :sentinel
             (lambda (proc _state)
               (swb-mysql--display-result-sentinel proc _state query))))

(defconst swb-mysql--batch-switches (list "-B" "-N" "--column-names")
  "Switch to toggle batch-mode.")

(defmethod swb-query-fetch-column ((this swb-connection-mysql) query)
  (with-temp-buffer
    (swb-query-synchronously this query (current-buffer) :extra-args swb-mysql--batch-switches)
    (cdr (-map 's-trim (split-string (buffer-string) "\n" t)))))

(defun swb-mysql--fetch-tuples-and-column-names (connection query)
  "Mysql helper for `swb-query-fetch-*'.

Fetch data like `swb-query-fetch-tuples' but as the first item
put a list of column names.

CONNECTION is an instance of `swb-connection-mysql', QUERY is the
SQL query."
  (with-temp-buffer
    (swb-query-synchronously connection query (current-buffer) :extra-args swb-mysql--batch-switches)
    (--map (-map 's-trim (split-string it "\t" t)) (split-string (buffer-string) "\n" t))))

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

(provide 'swb-connection-mysql)
;;; swb-connection-mysql.el ends here
