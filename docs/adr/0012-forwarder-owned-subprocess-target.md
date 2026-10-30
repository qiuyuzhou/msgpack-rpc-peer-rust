# Own one subprocess per shared target session

The subprocess target mode starts one subprocess for all callers of a session forwarder, preserving the shared-session semantics of ADR 0011 rather than creating a subprocess per caller. A subprocess disconnection ends the forwarder without automatic restart or replay, because restarting cannot preserve target-side session state or establish the outcomes of earlier operations. The forwarder owns termination and reaping of its direct child; management of descendant processes is outside this mode's scope.

The selected CLI syntax is `mrpc forward --listen <address> -- <program> <args...>`, mutually exclusive with an explicit address target and available only for `forward`. Arguments are passed directly without implicit shell interpretation. The subprocess inherits the working directory, environment, and stderr; its stdin and stdout carry binary MessagePack-RPC only, and the initial mode adds no dedicated cwd or environment options.

A supplied command overrides `MRPC_TARGET`, so an inherited address does not prevent command mode. An explicit `--target` together with a command is an argument error, as is a trailing `--` without a program. Without a command, address selection retains its existing behavior.

Subprocess exit (including status zero) or stdout EOF ends the shared session and stops caller admission. As with address-target disconnection, the forwarder exits with status 1 and reports the subprocess status when available; EOF also triggers cleanup if the subprocess is still alive. Existing target-notification output remains subject to ADR 0011 and the existing bounded output drain.

On Ctrl-C or Unix SIGTERM, the forwarder stops caller admission, closes the RPC connection and subprocess stdin, and allows up to five seconds for the subprocess to exit before forcibly terminating and reaping it. The forwarder does not explicitly relay signals to the subprocess and exits with status 130 or 143 respectively. Every failure path also cleans up the direct child; the subprocess exit grace period is independent of the request `--timeout`.

Startup requires successful process creation and pipe setup, with no additional RPC readiness handshake. A missing or non-executable program fails immediately; an unresponsive peer is handled by existing request timeouts, and invalid protocol traffic follows the library's existing policy.
