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

;; TODO: Make this less MYSQL-centric... ideally we should have some
;; interfaces with dynamic dispatch?
;; TODO: Add function to clone the current workbench: basically just
;; open a new buffer with the same connection.

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

(defvar swb-result-buffer nil
  "Result buffer for this workbench.")

(defun swb--read-connection (connection-constructor)
  "Read connection data.

CONNECTION-CONSTRUCTOR is a constructor to create temporary
connection when we query for the list of database."
  (let* ((host (read-from-minibuffer "Host: " (when swb-connection (oref swb-connection host))))
         (port (read-from-minibuffer "Port: " (when swb-connection (number-to-string (oref swb-connection port)))))
         (user (read-from-minibuffer "User: " (when swb-connection (oref swb-connection user))))
         (password (read-passwd "Password: "))
         (database (completing-read "Database: "
                                    (swb-get-databases
                                     (funcall connection-constructor "temp" :host host :port (string-to-number port) :user user :password password))
                                    nil t nil nil (when swb-connection (oref swb-connection database)))))
    (list host (string-to-number port) user password database)))

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
    (get-buffer-create (replace-regexp-in-string "workbench" "result" (buffer-name)))))

;; TODO: add something to send multiple queries (region/buffer)
;; TODO: figure out how to show progress bar (i.e. which query is being executed ATM)
;; TODO: warn before sending unsafe queries
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
  (completing-read "Table: " (swb-get-tables swb-connection) nil t))

;; TODO: make this into a generic method
(defun swb-show-data-in-table (table)
  "Show data in TABLE.

Limits to 500 lines of output."
  (interactive (list (swb--read-table)))
  (swb-query-display-result swb-connection (format "SELECT * FROM `%s` LIMIT 500;" table)
                            (get-buffer-create (format "*data-%s*" table))))

;; TODO: make this into a generic method
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

;; TODO: store connection details to .swb files (host, port, user, database, NO PASSWORD!)
(define-derived-mode swb-mode sql-mode "SWB"
  "Mode for editing SQL queries."
  (use-local-map swb-mode-map)
  (set (make-local-variable 'swb-result-buffer)
       (get-buffer-create (replace-regexp-in-string "workbench" "result" (buffer-name)))))

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
(defun swb-copy-column-csv ()
  "Put the values of the column into `kill-ring' as comma-separated string."
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

;; TODO: pridat podporu na editovanie riadkov priamo v result sete
;; TODO: we should be able to edit the query which produced this
;; result and re-run it, possibly in different window
(defvar swb-result-mode-map
  (let ((map (make-sparse-keymap)))
    (set-keymap-parent map org-mode-map)
    (define-key map "+" 'org-table-sum)
    (define-key map "f" 'swb-result-forward-cell)
    (define-key map "b" 'swb-result-backward-cell)
    (define-key map "p" 'swb-result-up-cell)
    (define-key map "n" 'swb-result-down-cell)
    ;; TODO: add `revert' which should probably go on g, then we need
    ;; to move goto-c elsewhere
    (define-key map "g" 'swb-result-goto-column)
    (define-key map "s" 'swb-sort-rows)
    ;; TODO: add various export options: line/selection/table/column
    ;; as sql, csv, xml (??)
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
