## Frequently Asked Questions

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [What does Onyx use internally for compression by default?](#what-does-onyx-use-internally-for-compression-by-default)
- [Why does my job hang at the repl?](#why-does-my-job-hang-at-the-repl)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

##### What does Onyx use internally for compression by default?

Unless otherwise overridden in the Peer Pipeline API, Onyx will use [Nippy](https://github.com/ptaoussanis/nippy). This can be override by setting the peer configuration with `:onyx.messaging/compress-fn` and `:onyx.messaging/decompress-fn`. See the Information Model documentation for more information.

##### Why does my job hang at the repl?

Onyx requires that for each task in a workflow, at least one vpeer is started. For example, the following workflow would require *at least* 4 peers

```
(def workflow
  [[:in :add]
  [:add :inc]
  [:inc :out]])
```
