;;; company-swb.el --- swb backend for company. -*- lexical-binding: t -*-

;; Copyright (C) 2017 Matúš Goljer <matus.goljer@gmail.com>

;; Author: Matúš Goljer <matus.goljer@gmail.com>
;; Maintainer: Matúš Goljer <matus.goljer@gmail.com>
;; Version: 0.0.1
;; Created: 21st November 2016
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

;; TODO: Better error handling (font-lock the error)

;;; Code:

(require 'dash)
(require 's)

(require 'sql-workbench)

(defun company-swb (command &optional arg &rest ignored)
  (interactive (list 'interactive))
  (cl-case command
    (meta (company-swb--meta arg))
    (sorted t)
    (annotation (company-swb--annotation arg))
    (interactive (company-begin-backend 'company-swb))
    (prefix (and (eq major-mode 'swb-mode)
                 (company-grab-symbol)))
    (candidates (let* ((tables (swb--get-tables (swb-get-query-at-point)))
                       (table-alias (save-excursion
                                      (backward-char (1+ (length arg)))
                                      (when (looking-at "\\.")
                                        (company-grab-symbol))))
                       (tables (or (--when-let (--first
                                                (equal (cadr it) table-alias)
                                                tables)
                                     (list it))
                                   tables)))
                  (--filter (string-prefix-p arg it)
                            (-concat
                             (-mapcat (-lambda ((table alias))
                                        (--map (propertize
                                                it 'meta table)
                                               (swb-company-get-table-columns
                                                swb-connection
                                                table)))
                                      tables)
                             ;; TODO: this is often invalid... we need
                             ;; to decide by context if we want to add
                             ;; all tables or only those in `tables'
                             (unless table-alias
                               (--map (propertize it 'meta "table")
                                      (swb-get-tables swb-connection)))))))))

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

(provide 'company-swb)
;;; company-swb.el ends here
