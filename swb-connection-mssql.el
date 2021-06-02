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
(require 'sqlcmd)

(defclass swb-connection-mssql (swb-iconnection)
  ((engine :type string :initform "mssql")
   (comint :initform nil)
   (prev-database :type string :initform ""))
  :documentation
  "Connection implementation for MSSQL.")

(defun swb-mssql--process-metadata (metadata)
  (with-temp-buffer
    (insert metadata)
    (swb-mssql--sqlcmd-table-to-org-table (current-buffer))
    (-let* ((data (-remove-item 'hline (org-table-to-lisp)))
            ((header . items) data)
            (header (--map (intern (concat ":" it)) header))
            (raw-plist (--map (-interleave header it) items)))
      (--map (-cons*
              (plist-get it :name)
              :type
              (plist-get it :system_type_name)
              it)
             raw-plist))))

(defun swb-mssql--sqlcmd-table-to-org-table (buffer)
  "Format the sqlcmd table output as `org-mode' table.

BUFFER is the buffer with the raw query output"
  (with-current-buffer buffer
    (goto-char (point-min))
    (insert "|-\n")
    (while (re-search-forward "^" nil t) (insert "|"))
    (goto-char (point-max))
    (re-search-backward "rows affected" nil t)
    (forward-line -1)
    (delete-region (point) (point-max))
    (goto-char (point-max))
    (insert "|-")
    (goto-char (point-min))
    (forward-line 3)
    (org-table-align)
    ;; We call `org-table-align' twice because first call only fixes
    ;; the table whereas the second call also minimizes the column
    ;; widths.
    (org-table-align)))

(defun swb-mssql--format-result-buffer (buffer callback)
  "Callback from the comint output filter to process the result.

This is called on the raw query output after all the output was
received."
  (with-current-buffer buffer
    (if (save-excursion
          (goto-char (point-min))
          (looking-at-p "Msg "))
        (if (= (save-excursion (goto-char (point-max)) (line-number-at-pos)) 2)
            (progn
              (goto-char (point-min))
              (message (buffer-substring (line-beginning-position) (line-end-position))))
          (display-buffer buffer))
      ;; this is the first "n rows affected" for inserting the
      ;; sp_describe_first_result_set result into a temporary table
      (delete-region
       (point-min)
       (save-excursion
         (goto-char (point-min))
         (re-search-forward "rows affected" nil t)
         (forward-line 1)
         (point)))
      (let ((data (delete-and-extract-region
                   (point-min)
                   (save-excursion
                     (goto-char (point-min))
                     (re-search-forward "rows affected" nil t)
                     (forward-line 1)
                     (point)))))
        (setq-local swb-metadata (swb-mssql--process-metadata data)))
      (swb-mssql--sqlcmd-table-to-org-table (current-buffer))
      (when callback (funcall callback t)))))

(defun swb--mssql-send-query (query &optional sink get-metadata)
  "Send QUERY to the current-buffer's process.

The `current-buffer' is assumed to be derived from `comint-mode'.

If SINK is non-nil, set it as
`sqlcmd-suppressed-output-sink-function'. It is assumed to be a
function and it is called from `sqlcmd-maybe-suppress-output'
every time input is received until it is unset or set to nil."
  (setq-local sqlcmd-suppressed-output-sink-function (or sink t))
  (comint-send-string
   (get-buffer-process (current-buffer))
   (format
    "%sDECLARE @query nvarchar(max) = \"%s\";%s
EXEC sp_executesql @query;
go"
    (if get-metadata
        "drop table if exists #swb_query_meta;
create table #swb_query_meta (is_hidden bit NOT NULL, column_ordinal int NOT NULL, name sysname NULL, is_nullable bit NOT NULL, system_type_id int NOT NULL, system_type_name nvarchar(256) NULL, max_length smallint NOT NULL, precision tinyint NOT NULL, scale tinyint NOT NULL, collation_name sysname NULL, user_type_id int NULL, user_type_database sysname NULL, user_type_schema sysname NULL, user_type_name sysname NULL, assembly_qualified_type_name nvarchar(4000), xml_collection_id int NULL, xml_collection_database sysname NULL, xml_collection_schema sysname NULL, xml_collection_name sysname NULL, is_xml_document bit NOT NULL, is_case_sensitive bit NOT NULL, is_fixed_length_clr_type bit NOT NULL, source_server sysname NULL, source_database sysname NULL, source_schema sysname NULL, source_table sysname NULL, source_column sysname NULL, is_identity_column bit NULL, is_part_of_unique_key bit NULL, is_updateable bit NULL, is_computed_column bit NULL, is_sparse_column_set bit NULL, ordinal_in_order_by_list smallint NULL, order_by_list_length smallint NULL, order_by_is_descending smallint NULL, tds_type_id int NOT NULL, tds_length int NOT NULL, tds_collation_id int NULL, tds_collation_sort_id tinyint NULL);\n"
      "")
    (replace-regexp-in-string "\"" "\"\"" query)
    (if get-metadata
        "\ninsert #swb_query_meta EXEC sp_describe_first_result_set @query, null, 0;
select name, system_type_name from #swb_query_meta;
drop table #swb_query_meta"
      "")))
  (comint-send-input nil t))

(defun swb--mssql-create-comint-maybe (connection)
  "Maybe (re)connect CONNECTION or recreate it if its parameters change."
  (when (or (not (equal (oref connection prev-database)
                        (oref connection database)))
            (not (oref connection comint))
            (not (buffer-live-p (oref connection comint)))
            (not (get-buffer-process (oref connection comint))))
    (when (oref connection comint)
      (kill-buffer (oref connection comint)))
    (oset connection prev-database (oref connection database))
    (oset connection comint
          (sqlcmd
           (oref connection host)
           (oref connection user)
           (oref connection password)
           (unless (string-empty-p (oref connection database))
             (oref connection database))
           nil 'no-display))))

(defmethod swb-query ((this swb-connection-mssql) query buffer &rest args)
  (swb--mssql-create-comint-maybe this)
  (swb-mysql--prepare-buffer buffer)
  (let ((comint (oref this comint))
        (sentinel (plist-get args :sentinel)))
    (with-current-buffer comint
      (swb--mssql-send-query
       query
       (lambda (output)
         (with-current-buffer buffer
           (insert output)
           (save-excursion
             (goto-char (point-max))
             (beginning-of-line)
             (when (and sentinel
                        (looking-at-p "1> "))
               (funcall sentinel)))))
       'get-metadata))
    buffer))

(defmethod swb-query-synchronously ((this swb-connection-mssql) query buffer &rest args)
  (swb--mssql-create-comint-maybe this)
  (swb-mysql--prepare-buffer buffer)
  (let ((comint (oref this comint))
        (done nil))
    (with-current-buffer comint
      (swb--mssql-send-query
       query
       (lambda (output)
         (with-current-buffer buffer
           (insert output)
           (save-excursion
             (goto-char (point-max))
             (beginning-of-line)
             (when (looking-at-p "1> ")
               (setq done t)))))))
    (while (not done)
      (sleep-for 0.01))
    buffer))

(defmethod swb-query-format-result ((this swb-connection-mssql) query buffer &optional callback)
  (let ((active-queries (swb-get-active-queries this)))
    (push query active-queries)
    (swb-set-active-queries this active-queries)
    (swb-query this query buffer
               :sentinel
               (lambda ()
                 (swb-mssql--format-result-buffer buffer callback)))))

(defmethod swb-query-fetch-column ((this swb-connection-mssql) query)
  (let ((data (swb-query-fetch-plist this query)))
    (--map (cadr it) data)))

(defmethod swb-query-fetch-tuples ((this swb-connection-mssql) query &optional with-header)
  (let* ((plists (swb-query-fetch-plist this query))
         (data (--map (-map 'cadr (-partition 2 it)) plists))
         (header (-map (lambda (x) (substring (symbol-name (car x)) 1))
                       (-partition 2 (car plists)))))
    (if with-header
        (cons header data)
      data)))

(defmethod swb-query-fetch-plist ((this swb-connection-mssql) query)
  (with-temp-buffer
    (swb-query-synchronously this query (current-buffer))
    (swb-mssql--sqlcmd-table-to-org-table (current-buffer))
    (goto-char (point-min))
    (-let* ((data (-remove-item 'hline (org-table-to-lisp)))
            ((header . items) data)
            (header (--map (intern (concat ":" it)) header)))
      (--map (-interleave header it) items))))

(defmethod swb-get-databases ((this swb-connection-mssql))
  (swb-query-fetch-column this "SELECT name FROM sys.databases;"))

(defmethod swb-get-tables ((this swb-connection-mssql))
  (swb-query-fetch-column this "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE='BASE TABLE';"))

(defmethod swb-get-table-info-query ((this swb-connection-mssql) table)
  (format
   "SELECT
  c.Column_Name as [Field],
  MAX(c.DATA_TYPE) as [Type],
  MAX(c.IS_NULLABLE) as [Null],
  STRING_AGG(tc.Constraint_Type, ', ') as [Key],
  MAX(c.COLUMN_DEFAULT) as [Default]
FROM
  INFORMATION_SCHEMA.COLUMNS c
LEFT JOIN
  INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE ccu on (
    c.Column_Name = ccu.Column_Name AND
    c.Table_Name = ccu.Table_Name
  )
LEFT JOIN
  INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc on (
    ccu.Constraint_Name = tc.Constraint_Name AND
    ccu.Table_Name = tc.Table_Name
  )
WHERE
  c.Table_Name = '%s'
GROUP BY c.Column_Name, c.ordinal_position
order by c.ordinal_position;"
   table))

(defmethod swb-R-get-connection ((this swb-connection-mssql) &optional var)
  (let ((conf (format
               "list(uid = %S, pwd = %S, server = %S, port = %S, database = %S, driver = \"ODBC Driver 17 for SQL Server\")"
               (oref this user)
               (oref this password)
               (oref this host)
               (oref this port)
               (oref this database)))
        (var (or var "swb__con__")))
    (format "%s <- rlang::invoke(dbConnect, c(odbc::odbc(), %s))" var conf)))

(provide 'swb-connection-mssql)
;;; swb-connection-mssql.el ends here
