set terminal pdfcairo font 'Times,22' linewidth 4 rounded dashlength 2 size 5,5

# Line style for axes
set style line 80 lt 1 lc rgb "#808080"

# Line style for grid
set style line 81 lt 0 # dashed
set style line 81 lt rgb "#808080"  # grey

set grid back linestyle 81
set border 3 back linestyle 80 # Remove border on top and right.  These
             # borders are useless and make it harder
             # to see plotted lines near the border.
    # Also, put it in grey; no need for so much emphasis on a border.
set xtics nomirror
set ytics nomirror

# Coral
set style line 1 lt rgb "#fc8d62" lw 1 pt 1
# Blue-grey (network wait)
set style line 2 lt rgb "#8da0cb" lw 1 pt 6
# Pink (compute)
set style line 3 lt rgb "#e78ac3" lw 1 pt 2
# Lime green (Result serialization)
set style line 9 lt rgb "#a6d854" lw 1 pt 3
# Teal (output write wait)
set style line 5 lt rgb "#66c2a5" lw 1 pt 4
# Yellow (scheduler delay)
set style line 6 lt rgb "#ffd92f" lw 1 pt 5
# Tan / khaki (unused?)
set style line 7 lt rgb "#e5c494" lw 1 pt 7
# Grey (task deserialization)
set style line 8 lt rgb "#8f8f8f" lw 1 pt 8
# Orange (GC)
set style line 4 lt rgb "#ffa500" lw 1 pt 8
# Light gray (queue time)
set style line 10 lt rgb "#f3f3f3" lw 1 pt 8
# Blue (broadcast wait)
set style line 11 lt rgb "#2b8cbe" lw 1 pt 8
# Red (mystery time)
set style line 12 lt rgb "#ff0000" lw 1 pt 8

set xlabel "Time (ms)" offset 0,0.5
set key above horizontal font ", 16" samplen 2
