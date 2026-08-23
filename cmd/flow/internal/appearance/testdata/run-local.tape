# What a person is handed by a finished run, which is the surface
# picatz/flowstate#544 is about and the one nothing here recorded.
#
# The four tapes beside this one all pin a surface that only prints. This one
# pins a surface that decides: on a terminal a run says what happened and stops,
# and the run document a pipe receives is deliberately absent from this frame.
# An appearance test is the only instrument that can see that absence — every
# other test here reads stdout for substrings, and the whole claim is about a
# stream nobody is reading.
#
# So the golden is the venue line, the run's own narration, the status pill, and
# the `outputs` block naming what the workflow answered with. If a JSON document
# ever reappears under that block, it appears here.
#
# The workflow is written by the tape rather than taken from examples/ because a
# recording gets a fresh HOME and no path into the repository. It declares an
# output on purpose: a workflow that produces nothing would record a frame that
# cannot show the difference between "said nothing" and "had nothing to say".

Hide
Output out.txt

Set Shell bash
Set FontSize 8
Set Padding 0

Type "PS1='$ '; stty cols 80 rows 45; cd $HOME; rm -rf demo; mkdir demo; cd demo"
Enter
Sleep 1s

Type "cat > workflow.yaml <<'EOF'"
Enter
Type "edition: v2026.3"
Enter
Type "name: release"
Enter
Type "vars:"
Enter
Type "  release: 2026.9.0"
Enter
Type "outputs:"
Enter
Type "  summary:"
Enter
Type "    value: ${'placed ' + vars.release}"
Enter
Type "steps:"
Enter
Type "  - id: place"
Enter
Type "    log:"
Enter
Type "      message: ${'placing ' + vars.release}"
Enter
Type "EOF"
Enter
Sleep 1s

Type "clear"
Enter
Sleep 500ms

Type "flow run local workflow.yaml"
Enter
Sleep 2s

Show
Sleep 300ms
