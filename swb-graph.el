(defun swb-gnuplot (result outfile)
  (with-temp-buffer
    (--each result
      (insert (s-join " " it) "\n"))
    (call-process-region (point-min) (point-max) "gnuplot" nil nil nil
                         "-e" (format "outfile='%s'" outfile) "/home/matus/test/plot.gnu")))
