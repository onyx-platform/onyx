## Logging

This chapter details how to inspect and modify the logs that Onyx produces.

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](http://doctoc.herokuapp.com/)*

- [Timbre](#timbre)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

### Timbre

By default, all Onyx output is logged to a file called `onyx.log` in the same directory as where the peer or coordinator jar is executing. The logging configuration can be overridden completely since it uses [Timbre](https://github.com/ptaoussanis/timbre). See the Timbre [example configuration](https://github.com/ptaoussanis/timbre#configuration).

Onyx's logging configuration writes all `WARN` and `FATAL` messages to standard out, also.
