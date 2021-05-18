;;; swb-iconnection.el --- Basic interface for talking to databases.

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

;; Here we define the interface for talking to databases.  To support
;; a new database, it would be enough to implement all the required
;; methods.

;;; Code:

(require 'eieio)

(defclass swb-iconnection ()
  ((host :initarg :host
         :initform "localhost"
         :type string
         :protection :protected
         :accessor swb-get-host
         :documentation
         "IP or URL where the database is located.
Should not contain port number, use the `port' attribute for
that.")
   (port :initarg :port
         :type integer
         :initform 3306
         :protection :protected
         :accessor swb-get-port
         :documentation "Port.")
   (user :initarg :user
         :type string
         :protection :protected
         :accessor swb-get-user
         :documentation "User.")
   (password :initarg :password
             :type string
             :protection :protected
             :documentation "Password.")
   (database :initarg :database
             :initform ""
             :type string
             :protection :protected
             :accessor swb-get-database
             :documentation "Database.")
   (engine :type string)
   (active-queries :initarg :activequeries
                   :initform nil
                   :protection :protected
                   :accessor swb-get-active-queries
                   :writer swb-set-active-queries
                   :documentation "Actively running queries for this connection."))
  :abstract t
  :documentation
  "Connection interface for work with the database.")

(defmethod swb-prepare-cmd-args ((this swb-iconnection) query extra-args)
  "Prepare the argument list for the RDBS client process.

THIS is an instance of `swb-iconnection'.

QUERY is the query.

EXTRA-ARGS are any extra arguments to pass to the process.")

;; TODO: show status in the mode line or header line somehow.  The
;; sentinel can update it once the process is finished.
(defmethod swb-query ((this swb-iconnection) query buffer &rest args)
  "Run a QUERY asynchronously.

BUFFER is a buffer where the result is stored.

ARGS is a plist with additional arguments:

- :extra-args are extra arguments which should be passed to the
  underlying process.")

(defmethod swb-query-synchronously ((this swb-iconnection) query buffer &rest args)
  "Run a QUERY synchronously.

BUFFER is a buffer where the result is stored.

ARGS is a plist with additional arguments:

- :extra-args are extra arguments which should be passed to the
  underlying process.")

(defmethod swb-query-format-result ((this swb-iconnection) query buffer &optional callback)
  "Run QUERY and format its result in a `swb-result-mode' compatible way.

BUFFER is a buffer where the result is stored.

The backend *must* make sure to run the CALLBACK function once
the result is received in its entirety and properly rendered (as
an org table).  One option is to wrap it into the process
sentinel code and call when the state changes to finished.

The CALLBACK function takes one argument, t or nil indicating if
the query ended successfully (t) or with an error (nil).")

(defmethod swb-query-fetch-column ((this swb-iconnection) query)
  "Run QUERY and return a list of values.

The query should return one column only.  The resulting list is
such that each successive element of the list represent nth row
of the result set (= column).

Data are retrieved synchronously.")

(defmethod swb-query-fetch-one ((this swb-iconnection) query)
  "Run QUERY and return a value.

The query should return one column and one row only.

Data are retrieved synchronously."
  (car (swb-query-fetch-column this query)))

(defmethod swb-query-fetch-tuples ((this swb-iconnection) query)
  "Run QUERY and return a list of tuples, one for each row.

Each tuple contains as many elements as there were columns
returned, in that order.

Data are retrieved synchronously.")

(defmethod swb-query-fetch-plist ((this swb-iconnection) query)
  "Run QUERY and return a list of plists, one for each row.

Each plist has as key the symbol :column and as value the
corresponding value.

Data are retrieved synchronously.")

(defmethod swb-query-fetch-alist ((this swb-iconnection) query)
  "Run QUERY and return a list of alists, one for each row.

Each alist has as key the symbol `column' and as value the
corresponding value.

Data are retrieved synchronously.")

;; Helper methods

(defmethod swb-get-databases ((this swb-iconnection))
  "Return a list of databases available at this connection.")

(defmethod swb-get-tables ((this swb-iconnection))
  "Return a list of tables available at this connection in current database.")

(defmethod swb-get-table-info ((this swb-iconnection) table)
  "Return information about TABLE.

The returned data is backend specific.")

(defmethod swb-company-get-table-columns ((this swb-iconnection) table)
  (--map (plist-get it :Field) (swb-get-table-info this table)))

(defmethod swb-connection-use-database ((this swb-iconnection) database)
  "Set DATABASE as default database for this connection"
  (oset this database database))

(defmethod swb-R-get-connection ((this swb-iconnection) &optional var)
  (let ((conf (format
               "list(user = %S, password = %S, host = %S, port = %S, dbname = %S)"
               (oref this user)
               (oref this password)
               (oref this host)
               (oref this port)
               (oref this database)))
        (var (or var "swb__con__")))
    (format "%s <- rlang::invoke(dbConnect, c(MariaDB(), %s))" var conf)))

(provide 'swb-iconnection)
;;; swb-iconnection.el ends here
