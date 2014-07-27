## Constraints

There are a few things that are strikingly different between Onyx and other compute platforms. I'd like to spell them out clearly here.

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->

- [Constraints](#constraints)
  - [Plugins Only](#plugins-only)
  - [Maps Only](#maps-only)
  - [Deployment On You](#deployment-on-you)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->


### Plugins Only

The only way to get data into or out of Onyx is via HornetQ. Plugins provide the illusion that this isn't true, but it's a constraint that underpins much of the design. This is helpful to keep in mind because it aids the understanding of plugins. Plugins move data from the target input source onto a HornetQ queue, and the opposite for output.

### Maps Only

With the exception of the sentinel value (Clojure keyword `:done`), the only type that a segment may take is a map. Cascading and Storm have the notion of Fields. I don't think this is a good idea because it require intermediaries to understand the name and position of all subsegment values between the source and the target.

### Deployment On You

Unlike Hadoop and Storm, the Onyx application jar is not transfered to the nodes on the cluster via a command line utility. Configuration management, such as Chef and Puppet, have come far enough to allow for an improved deployment story - including rolling releases if you have that requirement.