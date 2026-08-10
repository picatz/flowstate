# The first surface anyone sees: what was created, then the NEXT block naming
# the commands to run. The blank line before NEXT and the two-space indent under
# it are the shape every next-command element in the CLI shares, so this tape is
# the reference recording for that element as much as for `flow init`.

Hide
Output out.txt

Set Shell bash
Set FontSize 8
Set Padding 0

Type "PS1='$ '; stty cols 80 rows 45; cd $HOME; rm -rf demo; mkdir demo; cd demo; clear"
Enter
Sleep 1s

Type "flow init"
Enter
Sleep 2s

Show
Sleep 300ms
