## Scratch pad for acking ideas

This is a brain dump on my ideas for tackling fast message acknowledgment.

- Fast messaging relies on prestablishing sockets before messaging takes place
  - Do the handshake just once
  - The HTTP prototype got to skip this constraint, but going fast with websockets and Aeron means that it needs to be addressed

- One option to handle this is to open a socket connection to every other peer in the cluster
  - Really starts to become too many sockets at scale
  - This is why Storm makes you configure the number of ackers to an integer value

- Storm's approach has a few flaws
  - Can't control which node becomes the acker
    - Input nodes are already getting hammered by the acker nodes, would prefer not to double up the load
  - Percentage values are better to elastically scale than integer numbers

- Can ackers be pulled out into their own service?
- Could use the log to mark acker values, but writing to disk is a lot slower than a network call, and will eat up disk space very fast.

### Possible Solution 1 - Configurable Ackers

This solution relies on the user setting and tuning a value that indicates what percentage of peers will act as ackers.

- `submit-job` now takes a few more optional parameters that have defaults
  - `:acker/percentage`, mapped to an integer between `1` and `100`. This indicates the percentage of peers *executing this job* that will act as ackers. The number of peers as ackers are rounded up to the nearest whole number, and must at least be 1. This number will fluctuate at runtime with as peers join and leave the job. Default is `10`.
  - `:acker/exempt-input-tasks`, mapped to a boolean. Any peer executing an `:input` task is not elligible to be an acker. This is useful is the amount of network IO that this peer is doing is already too high to be useful as an acker. Default is `false`.
  - `:acker/exempt-output-tasks`, mapped to a boolean. Any peer executing an `:output` task is not elliginle to be an acker. Default is `false`.
  - `:acker/exempt-tasks`, mapped to a vector of keywords that represent task names. Any peer executing one of these task names is not elligble to be an acker. Default is `[]`.