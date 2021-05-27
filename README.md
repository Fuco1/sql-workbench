# sql-workbench

Working with SQL the convenient way!

# Using sql-workbench

Currently supported engines are MySQL and MSSQL (work in progress).
The available features (non-exhaustive list):

| Feature                        | MySQL | MSSQL |
|--------------------------------|-------|-------|
| Send queries                   | ✓     | ✓     |
| Get column types and metadata  | ✓     | ✓     |
| Company-based autocompletion   | ✓     | ✓     |
| Quick data preview             | ✓     | ❌    |
| Describe table                 | ✓     | ✓    |
| Show number of rows in a table | ✓     | ❌    |
| Query for list of all tables   | ✓     | ✓     |
| Copy data from result buffer   | ✓     | ✓     |

## Connecting to a server

Run `M-x swb-new-workbench`. It will prompt for `engine` `host`,
`port`, `user`, `password`, and the database name to use.

The workbench buffer will open.

## Using the workbench buffer

The workbench (source) buffer is the sql-workbench's main interface to
the database. Here are some things you can do with it.

* `swb-send-current-query` (`C-c C-c`) takes the sql statement at
  point and runs it on the database. The results will be displayed in
  the `*swb-results*` buffer.  With `C-u` the result will be shown in
  a new permanent buffer, meaning it will not replace its content
  after a new query is run.  With `C-0` the results will be inserted
  in-line into the source buffer.  With `C-1` (experimental) a
  time-series graph will be inserted into the source buffer.  This
  feature requires a working R installation with several packages, see
  the `swb-send-current-query` function help for info.

* `swb-describe-table` (`C-c C-t`) prompts for a table name, and
  displays the table schema in the `*swb-results*` buffer.

* `swb-show-data-in-table` (`C-c C-d`) prompts for a table name, and
  displays the first 500 entries in that table.

Because the workbench buffer is just a regular buffer you can do all
the usual things with it including saving it to a file and then
reopening it later.  It is also autosaved and backed up (if this is
enabled) so you don't have to worry about losing your work.

You can store the current connection information (except password)
using `swb-store-connection-to-file` (`C-c C-s`). The information will
be appended as [file-local
variables](https://www.gnu.org/software/emacs/manual/html_node/emacs/File-Variables.html).
Next time you open the file these will automatically become
buffer-local.  If you then execute a statement sql-workbench will
automatically reconnect using the stored connection information.  This
makes resuming work between sessions super easy.

If you customize the variable `swb-crypt-key` to be an email
associated with a gpg key, the password will be also stored as
encrypted base64-encoded string with this key set as recipient.

## Using the results buffer

The results buffer uses `swb-result-mode` which is derived from
`org-mode` and contains an Org Mode table.  All the features of Org
Mode (tables) therefore work automatically in the results buffer as
well.  However, the button is made read-only to prevent accidental
change of the data.  Consequently, some commands work without the `C-`
or `C-c` prefixes for increased convenience.

Use `f`, `b`, `n`, `p` (or arrow keys) for navigation, `j` to jump to
a specific column.

Use `s` to sort rows.  The sorting happens "offline" in the result
buffer only, not by querying the database server.

`+` and `%` produce the sum or the average of the column or a region.

`c` and `r` allow you to copy the column or row(s) in various formats,
such as csv, php array, R tibble or SQL values.

`g` will revert the buffer by running the same query again.

For more information run `C-h m` in the result buffer and see the list
of key bindings.

# Integration with other packages

## company

There is an experimental [company](http://company-mode.github.io/) backend `company-swb`.  To enable it run

    (push 'company-swb company-backends)

and then enable `M-x company-mode` in the swb buffer.
