// Command flowstate-plugin-docker runs one operator-defined container to
// completion.
//
// # What this is, and what it is not
//
// It is a task that runs a container. It is not a sandbox, and this file says
// so before anything else does, because the difference is the whole of the
// honesty here.
//
// A Docker Engine socket is ambient daemon authority. A process holding one can
// create privileged containers, bind-mount any host path, join the host network
// namespace, and read every other container's output - regardless of what this
// plugin's task schema says. A first-party implementation can decline to ask for
// those, and this one does, but declining is vetted code rather than a boundary:
// the daemon would have honoured the request. So a worker running this plugin
// has put the plugin - and anything that can make it act - inside the trusted
// computing base, and the operator's grants file is what bounds what it will
// ask for rather than what it could.
//
// THREAT_MODEL.md's three verbs name this exactly: process separation *bounds*
// what crosses the socket, admission decides what runs, and the *confine*
// column is the substrate's - the container runtime's own isolation, which this
// plugin requests and does not enforce. Nothing here is called sandboxing.
//
// # The contract, which is issue #1348's
//
// The task takes a run grant's name and some parameters. An operator's grants
// file holds everything else, and everything in it is checked when the worker
// starts:
//
//   - Image identity is a canonical digest. A tag is refused - not warned about
//   - because a tag is a name somebody can move between the moment a
//     deployment reviewed an image and the moment a node runs it.
//   - Argv is explicit and has no shell. Environment is assembled from nothing:
//     the container sees what the grant names and no variable of the worker's.
//   - The root filesystem is read-only and the user is non-root, unless the
//     grant says otherwise in as many words.
//   - Mounts are grant identifiers the operator resolves to host paths, never
//     paths from a workflow, and they are read-only unless the grant says
//     otherwise.
//   - The network is none unless the operator granted a named one.
//   - CPU, memory, process count, wall time and output bytes are all required,
//     all bounded by this plugin's own ceilings, and a grant over a ceiling is a
//     startup failure.
//   - Cancellation stops and removes the container before returning, within a
//     bounded cleanup window, and reports an outcome that distinguishes "did not
//     start" from "ran" from "unknown".
//
// # Why there is no docker.pull, docker.exec or docker.build
//
// Each is a different authority with a different contract. Pulling is the
// daemon's, and a grant naming a digest is what makes the image this plugin runs
// the image an operator reviewed; exec attaches to a container somebody else's
// grant created; build turns a workflow's input into an image, which is the
// supply chain this repository's oci plugin exists to check rather than to
// manufacture. None of them is a flag on this task.
//
// # Registry credentials
//
// There are none here. An image the daemon cannot pull is a run that fails,
// and the fix is the operator's: pre-pull it, or configure the daemon's own
// credentials. A registry credential in a task input would put the deployment's
// pull authority into workflow history, which #1348 names and this plugin
// declines to do.
package main
