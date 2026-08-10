# One task in full. This is the densest aligned surface the CLI has: two label
# columns, a constraint column that wraps into its own indent, and a copyable
# step at the end. Alignment here is carried by padding a pipe cannot see, so a
# regression is invisible to every test that only reads for substrings.

Hide
Output out.txt

Set Shell bash
Set FontSize 8
Set Padding 0

Type "PS1='$ '; stty cols 80 rows 45; cd $HOME; clear"
Enter
Sleep 1s

Type "flow tasks http"
Enter
Sleep 2s

Show
Sleep 300ms
