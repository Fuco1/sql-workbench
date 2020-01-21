# sql-workbench

Working with SQL the convenient way!

# Using sql-workbench

Currently, only MySQL is supported.

## Connecting to a MySQL instance

Run `M-x swb-new-workbench-mysql`. It will prompt for `host`, `port`, `user`, `password`, and the database inside the MySQL server to use.

The workbench buffer will open.

## Using the workbench buffer

The workbench buffer is the sql-workbench's main interface to the database. Here are some things you can do with it.

* `swb-send-current-query` (`C-c C-c`) takes the sql statement at point and runs it on the database. The results will be displayed in the `*swb-results*` buffer.

* `swb-describe-table` (`C-c C-t`) prompts for a table name, and displays the table schema in the `*swb-results*` buffer.

* `swb-show-data-in-table` (`C-c C-d`) prompts for a table name, and displays the first 500 entries in that table.

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

# Integration with other packages

## company

There is an experimental [company](http://company-mode.github.io/) backend `company-swb`.  To enable it run

    (push 'company-swb company-backends)

and then enable `M-x company-mode` in the swb buffer.
