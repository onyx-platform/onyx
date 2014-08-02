## Plugins

Plugins are a particularly straightforward concept in Onyx. They serve to either get data in or out of HornetQ. See the README.md of the project for a list of offical Onyx plugins.

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->

- [Plugins](#plugins)
  - [Bootstrapping](#bootstrapping)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

### Bootstrapping

Sometimes, it's adventageous for a plugin to figure out what segments to push into Onyx without the user explicitly helping. For example, the SQL plugin uses a `partition-keys` function to scan the range of keys in a table, then slice it up into evenly sized key spaces. The key spaces are sent down to the next task, where peers read records out of the SQL table for their particular key space.

This sort of dynamic behavior is accomplished via bootstrapping. By setting `:onyx/bootstrap?` to `true`, Onyx will enqueue a single segment (which should be ignored), followed by the sentinel. In the `apply-fn` function of the Pipeline extentions, you can perform a computation to dynamically generate segments to be passed downstream.

