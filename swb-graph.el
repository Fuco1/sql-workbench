(defvar swb-package-directory (file-name-directory load-file-name))

(defun swb-gnuplot (result outfile)
  (let ((infile (make-temp-file "swb-gnuplot-source")))
    (with-temp-file infile
      (--each result
        (insert (s-join " " it) "\n")))
    (call-process "gnuplot" nil nil nil
                  "-e" (format "outfile='%s'" outfile)
                  "-e" (format "infile='%s'" infile)
                  "-e" (format "termheight='%s'" (* 25 (default-font-height)))
                  (concat swb-package-directory "/plot.gnu"))
    (delete-file infile)))
