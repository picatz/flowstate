# The sad path, which is the one that matters most: nobody reads the ERROR
# block until something has already gone wrong. Port 9 is the discard port, so
# the refusal is immediate and the same everywhere rather than a timeout.
#
# What this pins is that an error still has a NEXT block, and that the two read
# as one shape with the happy paths above.

Hide
Output out.txt

Set Shell bash
Set FontSize 8
Set Padding 0

Type "PS1='$ '; stty cols 80 rows 45; cd $HOME; clear"
Enter
Sleep 1s

Type "FLOWSTATE_ADDRESS=http://127.0.0.1:9 flow list"
Enter
Sleep 2s

Show
Sleep 300ms
