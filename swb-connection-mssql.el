;;; swb-connection-mssql.el --- Implementation of connection for MSSQL. -*- lexical-binding: t -*-

;; Copyright (C) 2021 Matúš Goljer <matus.goljer@gmail.com>

;; Author: Matúš Goljer <matus.goljer@gmail.com>
;; Maintainer: Matúš Goljer <matus.goljer@gmail.com>
;; Version: 0.0.1
;; Created: 21 January 2021
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
(require 'swb-connection-mysql)

(defclass swb-connection-mssql (swb-iconnection)
  ((engine :type string :initform "mssql"))
  :documentation
  "Connection implementation for MSSQL.")

(defun swb-mssql--process-metadata ()
  (let ((cols (swb--result-get-column-names)))
    (--map (list it :type "string") cols)))

(defun swb-mssql--format-result-sentinel (proc state callback)
  "Sentinel for PROC once its STATE is exit.

Format the table so that it is a valid `org-mode' table.

CALLBACK is called after the process has finished."
  ;; TODO: move this cleanup elsewhere, the display code could be
  ;; reused between backends
  (when (or (equal state "finished\n")
            (equal state "exited abnormally with code 1\n"))
    (with-current-buffer (process-buffer proc)
      (goto-char (point-min))
      (while (re-search-forward "^+-" nil t) (replace-match "|-"))
      (goto-char (point-min))
      (while (re-search-forward "-+$" nil t) (replace-match "-|"))
      (goto-char (point-max))
      (delete-char -1)
      (beginning-of-line)
      (cond
       ((looking-at-p "^([0-9]+ row")
        (message "%s" (replace-regexp-in-string
                       "()" ""
                       (delete-and-extract-region (point) (line-end-position)))))
       ((looking-at-p "^Query OK")
        (message "%s" (delete-and-extract-region (point) (line-end-position))))
       ((looking-at-p "^ERROR")
        (message "%s" (delete-and-extract-region (point) (line-end-position)))))
      (delete-region (point) (point-max))
      (setq-local swb-metadata (swb-mssql--process-metadata))
      (when callback (funcall callback (equal state "finished\n"))))))

(defmethod swb-prepare-cmd-args ((connection swb-connection-mssql) query extra-args)
  (-concat extra-args
           (list "-Q" query
                 "-S" (oref connection host)
                 "-U" (oref connection user)
                 "-P" (oref connection password))
           (when (slot-boundp connection :database)
             (list "-d" (oref connection database)))))

(defmethod swb-query ((this swb-connection-mssql) query buffer &rest args)
  (swb-mysql--prepare-buffer buffer)
  (let* ((cmd-args (swb-prepare-cmd-args this query (plist-get args :extra-args)))
         (proc (apply 'start-process "swb-query" buffer "mssql-cli" cmd-args))
         (sentinel (plist-get args :sentinel)))
    (when sentinel
      (set-process-sentinel proc sentinel))
    buffer))

(defmethod swb-query-synchronously ((this swb-connection-mssql) query buffer &rest args)
  (swb-mysql--prepare-buffer buffer)
  (let* ((cmd-args (swb-prepare-cmd-args this query (plist-get args :extra-args))))
    (apply 'call-process "mssql-cli" nil buffer nil cmd-args)
    buffer))

(defmethod swb-query-format-result ((this swb-connection-mssql) query buffer &optional callback)
  (let ((active-queries (swb-get-active-queries this)))
    (push query active-queries)
    (swb-set-active-queries this active-queries)
    (swb-query this query buffer
               :sentinel
               (lambda (proc state)
                 (swb-mssql--format-result-sentinel proc state callback)))))

(defmethod swb-query-fetch-column ((this swb-connection-mssql) query)
  (with-temp-buffer
    (swb-query-synchronously this query (current-buffer))
    (goto-char (point-min))
    (kill-region (point) (save-excursion
                           (forward-line 3)
                           (backward-char)
                           (point)))
    (goto-char (point-max))
    (kill-region (point) (re-search-backward "^+-" nil t))
    (goto-char (point-min))
    (while (re-search-forward "^| " nil t) (replace-match ""))
    (goto-char (point-min))
    (while (re-search-forward " |$" nil t) (replace-match ""))
    (cdr (-map 's-trim (split-string (buffer-string) "\n" t)))))

(defmethod swb-get-databases ((this swb-connection-mssql))
  (swb-query-fetch-column this "\\ld"))

(defmethod swb-get-tables ((this swb-connection-mssql))
  (swb-query-fetch-column this "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE='BASE TABLE';"))

(defmethod swb-get-table-info ((this swb-connection-mssql) table)
  (swb-query-fetch-column
   this
   (format "select COLUMN_NAME as Field
            from information_schema.columns
            where table_name = '%s'
            order by ordinal_position;"
           table)))

(defmethod swb-company-get-table-columns ((this swb-connection-mssql) table)
  (swb-get-table-info this table))

(provide 'swb-connection-mssql)
;;; swb-connection-mssql.el ends here
