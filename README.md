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

# Integration with other packages

## company

There is an experimental [company](http://company-mode.github.io/) backend `company-swb`.  To enable it run

    (push 'company-swb company-backends)

and then enable `M-x company-mode` in the swb buffer.
