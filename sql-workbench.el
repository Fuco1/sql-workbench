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

;;; Code:

(require 'dash)
(require 's)

(defstruct connection-details host port user password database)

(defun swb--fix-mysql-to-org-hline ()
  "Replaces the initial and terminal the + in the hline with |."
  (beginning-of-line)
  (delete-char 1)
  (insert "|")
  (end-of-line)
  (delete-char -1)
  (insert "|"))

(defun swb-query-sentinel (proc state)
  (with-current-buffer (process-buffer proc)
    (goto-char (point-min))
    (when (looking-at "^+-")
      (swb--fix-mysql-to-org-hline))
    (forward-line 2)
    (when (looking-at "^+-")
      (swb--fix-mysql-to-org-hline))
    (goto-char (point-max))
    (when (re-search-backward "^+-" nil t)
      (swb--fix-mysql-to-org-hline))
    (unless swb-buffer (rename-buffer (generate-new-buffer-name "*swb-query-result*")))
    (if swb-silent
        (kill-buffer)
      (swb-result-mode)
      (goto-char (point-min))
      (let ((window (display-buffer (current-buffer))))
        (with-selected-window window
          (set-window-point window (point-min))
          (forward-line 3)
          (swb-result-forward-cell 1))))))

(defvar swb--batch-switches-mysql (list "-B" "-N")
  "Switches to toggle batch-mode")

(cl-defun swb-run-sql-mysql (query connection &key extra-switches silent synchronous buffer)
  "Run a QUERY at CONNECTION."
  (let* ((buffer (with-current-buffer (or buffer (generate-new-buffer " *swb-query*"))
                   (read-only-mode -1)
                   (erase-buffer)
                   (set (make-local-variable 'swb-silent) silent)
                   (set (make-local-variable 'swb-buffer) buffer)
                   (current-buffer)))
         (args (-concat extra-switches
                        (list "-A"
                              "-e" query
                              "-h" (connection-details-host connection)
                              "-P" (connection-details-port connection)
                              "-u" (connection-details-user connection)
                              (concat "-p" (connection-details-password connection)))
                        (--if-let (connection-details-database connection) (list it) nil)))
         (proc (if synchronous
                   (apply 'call-process "mysql" nil buffer nil args)
                 (apply 'start-process "swb-query" buffer "mysql" args))))
    (unless synchronous (set-process-sentinel proc 'swb-query-sentinel))
    buffer))


;; TODO: spravit tabular-mode tabulku so schemou


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
(defun swb-copy-column-csv ()
  "Put the values of the column into kill-ring as comma-separated string."
  (interactive)
  (save-excursion
    (-let* (((beg . end) (swb--get-column-bounds))
            (col-data (org-table-copy-region
                       (save-excursion
                         (goto-char beg)
                         (swb-result-down-cell 2)
                         (point))
                       end)))
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
  "Go forward one cell."
  (interactive "p")
  (org-table-goto-column (+ arg (org-table-current-column)))
  (skip-syntax-forward " "))

(defun swb-result-backward-cell (&optional arg)
  "Go forward one cell."
  (interactive "p")
  (org-table-goto-column (- (org-table-current-column) arg))
  (skip-syntax-forward " "))

(defun swb-result-up-cell (&optional arg)
  "Go up one cell."
  (interactive "p")
  (let ((cc (org-table-current-column)))
    (forward-line (- arg))
    (org-table-goto-column cc)))

(defun swb-result-down-cell (&optional arg)
  "Go down one cell."
  (interactive "p")
  (let ((cc (org-table-current-column)))
    (forward-line arg)
    (org-table-goto-column cc)))

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

(defun swb-result-goto-column (column-name)
  "Go to column named COLUMN-NAME."
  (interactive (list (completing-read "Column: "
                                      (swb--result-get-column-names)
                                      nil t)))
  (let ((column-number (1+ (--find-index (equal column-name it) (swb--result-get-column-names)))))
    (org-table-goto-column column-number)))

;; TODO: sort ma blby regexp na datum, berie len timestamp <yyyy-mm-dd>... a napr ignoruje hodiny
(defun swb-sort-rows ()
  "Sort rows of the result table."
  (interactive)
  (unwind-protect
      (progn
        (read-only-mode -1)
        (call-interactively 'org-table-sort-lines))
    (read-only-mode 1)))

(defvar swb-result-mode-map
  (let ((map (make-sparse-keymap)))
    (set-keymap-parent map org-mode-map)
    (define-key map "+" 'org-table-sum)
    (define-key map "f" 'swb-result-forward-cell)
    (define-key map "b" 'swb-result-backward-cell)
    (define-key map "p" 'swb-result-up-cell)
    (define-key map "n" 'swb-result-down-cell)
    (define-key map "g" 'swb-result-goto-column)
    (define-key map "s" 'swb-sort-rows)
    (define-key map "c" 'swb-copy-column-csv)
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

(define-derived-mode swb-result-mode org-mode "Swb result"
  "Mode for displaying results of sql queries."
  (read-only-mode 1)
  (set (make-local-variable 'org-mode-hook) nil)
  (use-local-map swb-result-mode-map)
  (visual-line-mode -1)
  (toggle-truncate-lines 1))

(provide 'sql-workbench)
;;; sql-workbench.el ends here
