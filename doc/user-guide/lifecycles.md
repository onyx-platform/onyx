## Lifecycles

Lifecycles are a feature that allow you to control code that executes at particular points during task execution on each peer. Lifecycles are data driven and composable.

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](http://doctoc.herokuapp.com/)*

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

### Summary

There are several interesting points to execute arbitrary code during a task in Onyx. Onyx lets you plug in and calls functions before a task, after a task, before a batch, and after a batch on every peer. Additionally, there is another lifecycle hook that allows you to delay starting a task in case you need to do some work like acquiring a lock under contention. A peer's lifecycle is isolated to itself, and lifecycles never coordinate across peers.





