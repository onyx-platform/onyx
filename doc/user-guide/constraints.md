## Constraints

There are a few things that are strikingly different between Onyx and other compute platforms. I'd like to spell them out clearly here.

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](http://doctoc.herokuapp.com/)*

- [Plugins Only](#plugins-only)
- [Maps Only](#maps-only)
- [Deployment On You](#deployment-on-you)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

### Maps Only

With the exception of the sentinel value (Clojure keyword `:done`), the only type that a segment may take is a map. Cascading and Storm have the notion of Fields. This isn't such a hot abstraction because it requires intermediaries to understand the name and position of all subsegment values between the source and the target.

### Deployment On You

Unlike Hadoop and Storm, the Onyx application jar is not transferred to the nodes on the cluster via a command line utility. Configuration management tools, such as Chef and Puppet, have come far enough to allow for an improved deployment story - including rolling releases if you have that requirement. The Onyx team has seen success in the field using both CloudFormation and Mesos/Marathon.

