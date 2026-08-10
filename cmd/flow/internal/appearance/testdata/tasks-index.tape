# The task index: a two-column list whose second column wraps, followed by the
# prose that says where to go next. What this pins is the wrap point and the
# hanging indent that keeps a wrapped description under its own column.
#
# Run with `make appearance` (or `vhs` directly, with `flow` on PATH).

Hide
Output out.txt

Set Shell bash
Set FontSize 8
Set Padding 0

# The pty, not the browser window, decides where the CLI wraps, so the width
# this pins is `stty cols` rather than any geometry vhs sets. See doc.go.
Type "PS1='$ '; stty cols 80 rows 45; cd $HOME; clear"
Enter
Sleep 1s

Type "flow tasks"
Enter
Sleep 2s

Show
Sleep 300ms
