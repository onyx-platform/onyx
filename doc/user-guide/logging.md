## Logging

This chapter details how to inspect and modify the logs that Onyx produces.

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](http://doctoc.herokuapp.com/)*

- [Timbre](#timbre)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

### Timbre

Onyx uses [Timbre](https://github.com/ptaoussanis/timbre) for
logging.

By default, all Onyx output is logged to a file called `onyx.log` in
the same directory as where the peer or coordinator jar is
executing. The logging configuration can be overridden completely, see
below.

Onyx's default logging configuration writes all `WARN` and `FATAL`
messages to standard out, also.

#### Overriding the log file name and path

In order to override the log file location add `:onyx.log/file` to
both the environment config sent into `start-env` as well as the
peer config sent into `start-peer-group`.

Both relative and absolute paths are supported by Timbre.

#### Overriding the Timbre log config

Similarly, to override the full Timbre log config map, construct the
Timbre configuration and add it to both the environment and peer
config maps under the `:onyx.log/config` key.  Note that the
`onyx.log/config` map will be merged with the existing Timbre
configuration rather than replacing it completely.  In practice this
means that extra configuration must be sent in to, for example,
disable appenders that are enabled by default.  See the examples
below.

#### Examples

The following example simply changes the output file.

```clojure
(let [log-config {:onyx.log/file "/var/log/onyx.log"}
      peer-config (merge my-peer-config log-config)
      env-config (merge my-env-config log-config)]
  (onyx.api/start-env env-config)
  (onyx.api/start-peer-group peer-config)

  ;; ...
  )
```

This example uses Timbre to redirect Onyx logs into the regular Java
logging system using the
[log-config](https://github.com/palletops/log-config) library.

```clojure
(require '[com.palletops.log-config.timbre.tools-logging :as tl])
(let [log-config {:onyx.log/config {:appenders
                                    {:spit {:enabled? false}
                                     :standard-out {:enabled? false}
                                     :rotor {:enabled? false}
                                     :jl (tl/make-tools-logging-appender
                                          {:enabled? true
                                           :fmt-output-opts {:nofonts? true}})}
                                    :min-level :trace}}
      peer-config (merge my-peer-config log-config)
      env-config (merge my-env-config log-config)]
  (onyx/start-env env-config)
  (onyx/start-peer-group peer-config)

  ;; ...
  )
```

See the Timbre
[example configuration](https://github.com/ptaoussanis/timbre#configuration)
for more information on valid values for the `:onyx.log/config` map.
