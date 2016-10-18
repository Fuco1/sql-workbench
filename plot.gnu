# change a color of border.
set border lw 1 lc rgb "#babdb6"
# change text colors of  tics
set xtics textcolor rgb "#babdb6"
set ytics textcolor rgb "#babdb6"
# change text colors of labels
set xlabel "X" textcolor rgb "#babdb6"
set ylabel "Y" textcolor rgb "#babdb6"
# change a text color of key
set key textcolor rgb "#babdb6"

set terminal pngcairo size 700, termheight background rgb '#212526'
set output outfile
set timefmt '%Y-%m-%d'
#plot infile using 1:2 w l title columnheader, '' using 1:3 w l title columnheader
plot for [col=2:10] infile using 0:col w l title columnheader(col)
