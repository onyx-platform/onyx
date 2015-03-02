## Scratch pad for interface changes

- `read-batch` -> `read-segments`
- `write-batch` -> `write-segments`

- Make decompress/compress/apply more rigid. No polymorphism here.
- Make compress/decompress fn configurable through catalog entry.

- Global compression reconfiguration?

- Keep timeouts in plugins. Plugins are good at canceling long reads or writes for themselves.
- Configure order that different multimethods are invoked? Exclude some? Precompile the functions?

- Having more "things" is okay.

- `read-segments` doesn't have to go off on its own thread, it can figure out when to come back.
  - This helps keeping 1 thread per peer


- Possible threading models
  - 1 peer -> 1 thread
    - easy to think about, easy to pick how many vpeers you'd want
  - 1 peer -> 2 threads
    - 1 thead for reading, 1 thread for processing/writing
    - Works well with CPUS of 2/4/8/16 cores
    - Might not be too efficient since writes are IO work
  - 1 peer -> 3 threads
    - 1 thread for reading, 1 thread for processing, 1 thread for writing
    - Isolates byte shoveling
    - Awkward for the number of cores available
    - Lower number of vpeers would be the norm
    - Can isolate a fourth thread to make it divisible by 2 again?

- Threads that would be good for `go` blocks:
  - Log subscriber thread
  - Inbox loop
  - Outbox loop

- Candidates for real threads:
  - Acker thread
  - Completion thread

