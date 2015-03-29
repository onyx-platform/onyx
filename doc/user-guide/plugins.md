## Plugins

Plugins serve as an abstract to compose mechanisms for getting data into and out of HornetQ. See the README.md of the project for a list of official Onyx plugins, or keep reading to roll your own.

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](http://doctoc.herokuapp.com/)*

- [Interfaces](#interfaces)
- [Bootstrapping](#bootstrapping)
- [Templates](#templates)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

### Interfaces

In order to implement a plugin, a number of multimethods need to be extended from the [Pipeline Extensions API](../../src/onyx/peer/pipeline_extensions.clj). Reader plugins will implement `read-batch`. Writer plugins will implement `write-batch` and `seal-resource`. See the docstrings for instructions on implementation.

### Bootstrapping

Sometimes, it's advantageous for a plugin to figure out what segments to push into Onyx without the user explicitly helping. For example, the SQL plugin uses a `partition-keys` function to scan the range of keys in a table, then slice it up into evenly sized key spaces. The key spaces are sent down to the next task, where peers read records out of the SQL table for their particular key space.

This sort of dynamic behavior is accomplished via bootstrapping. By setting `:onyx/bootstrap?` to `true`, Onyx will enqueue a single segment (which should be ignored), followed by the sentinel. In the `apply-fn` function of the Pipeline extensions, you can perform a computation to dynamically generate segments to be passed downstream.

### Templates

To help move past the boilerplate of creating new plugins, use Leiningen with [`onyx-plugin`](https://github.com/MichaelDrogalis/onyx-plugin) to generate a template.
