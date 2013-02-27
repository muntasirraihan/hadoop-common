#!/bin/sh
PREFIX="$1"
TITLE="$2"
XLABEL="$3"
YLABEL="$4"
num_columns=$(awk -F',' "{if(NR == 1) print NF}" < "$PREFIX.csv")

gnuplot << EOF
# adapted from Brighten Godfrey's example at
# http://youinfinitesnake.blogspot.com/2011/02/attractive-scientific-plots-with.html
set terminal svg size 640,480 fname "Gill Sans" fsize 9 rounded dashed
set output "$PREFIX.svg"

# Line style for axes
set style line 80 lt 0
set style line 80 lt rgb "#808080"

# Line style for grid
set style line 81 lt 3  # dashed
set style line 81 lt rgb "#808080" lw 0.5  # grey

set grid back linestyle 81
set border 3 back linestyle 80 # Remove border on top and right.  These
             # borders are useless and make it harder
             # to see plotted lines near the border.
    # Also, put it in grey; no need for so much emphasis on a border.
set xtics nomirror
set ytics nomirror

#set log x
#set mxtics 10    # Makes logscale look good.

# Line styles: try to pick pleasing colors, rather
# than strictly primary colors or hard-to-see colors
# like gnuplot's default yellow.  Make the lines thick
# so they're easy to see in small plots in papers.
set style line 1 lt 1
set style line 2 lt 1
set style line 3 lt 1
set style line 4 lt 1
set style line 1 lt rgb "#A00000" lw 2 pt 7
set style line 2 lt rgb "#00A000" lw 2 pt 9
set style line 3 lt rgb "#5060D0" lw 2 pt 5
set style line 4 lt rgb "#F25900" lw 2 pt 13

set xlabel "$XLABEL"
set ylabel "$YLABEL"

set title "$TITLE"

set key autotitle columnhead
set key bottom right

set datafile separator ","

plot for [i=2:$num_columns] "$PREFIX.csv" using 1:i with lines ls (i-1)
EOF
