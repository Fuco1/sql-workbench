(defvar swb-package-directory (file-name-directory load-file-name))

(defun swb-gnuplot (result outfile)
  (with-temp-buffer
    (cd swb-package-directory)
    (--each result
      (insert (s-join " " it) "\n"))
    (call-process-region (point-min) (point-max) "gnuplot" nil nil nil
                         "-e" (format "outfile='%s'" outfile) "plot.gnu")))
