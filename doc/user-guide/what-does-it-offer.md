## What does Onyx offer?

In this chapter, I'll enumerate and explain the reasons that I built Onyx.

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](http://doctoc.herokuapp.com/)*

- [An Information Model](#an-information-model)
- [Temporal Decoupling](#temporal-decoupling)
- [Elimination of Macros](#elimination-of-macros)
- [Plain Clojure Functions](#plain-clojure-functions)
- [Testing Without Mocking](#testing-without-mocking)
- [Easy Parameterization of Workflows](#easy-parameterization-of-workflows)
- [Transactional Semantics](#transactional-semantics)
- [Transparent Code Reuse for Batch and Streaming](#transparent-code-reuse-for-batch-and-streaming)
- [Aspect Orientation](#aspect-orientation)
- [AOT Nothing](#aot-nothing)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->


#### An Information Model

Information models are often superior to APIs, and almost always better than DSLs. The hyper-flexibility of a data structure literal allows Onyx workflows and catalogs to be constructed *at a distance*, meaning on another machine, in a different language, by another program, etc.

The information model for an Onyx workflow has the distinct advantage that it's possible to compile *other* workflows (perhaps a datalog) into the workflow that Onyx understands. The Information Model chapter describes a potential target for compilation.

#### Temporal Decoupling

To the extent that Onyx places data at the highest importance, very few Onyx constructs actually need to be generated at the same time as job submission or peer registration. Programs can create workflows, drop them to a database, and pull them out at a later time without any problems. We've removed the context that Cascading always seems to require.

#### Elimination of Macros

Macros are a tremendously powerful tool, but are often inappropriate for end-user consumption of an API. Onyx goes beyond Storm's `defbolt` and `defspout` by making vanilla Clojure functions shine. These functions need no context to execute and do not require any dynamic bindings.

#### Plain Clojure Functions

To the same point above, we want plain Clojure functions to be the building blocks for application logic. Onyx's functions can be tested directly without any special test runner.

#### Testing Without Mocking

In general, your design is in trouble when you've reached for `with-redefs` or something along those lines to mock functions. Onyx places a high importance around programming against interfaces, and even more-so around putting space inbetween small components with channels. Onyx programs can be tested in development mode, and moved to production mode with little to no changes. If you'd like to change your input or output plugins, all you need to do is reassociate the catalog entry with something like an in-memory plugin. No mocking code required.

#### Easy Parameterization of Workflows

I think it's particularly telling that many compute frameworks don't offer an easy way to parameterize workflows. Onyx puts space between the caller and the function definition. Parameterize tasks inside the catalog, and update the catalog entry at will. Additionally, Onyx allows peers to spin up their own parameters at boot-up time. See [the tests](https://github.com/MichaelDrogalis/onyx/blob/0.4.x/test/onyx/peer/params_test.clj) for an example of this alternate technique for parameterization.

#### Transactional Semantics

Inside of the pipelines that Onyx lays, data needs to be moved around between nodes in the HornetQ cluster. Onyx uses transactions to move that data - meaning we gain transactional execution semantics. Onyx won't replay batches after they're been committed to storage. Note that pre-commit failure will cause the entire batch to be replayed, just as a database transaction might partially execute and rollback in the face of failure.

#### Transparent Code Reuse for Batch and Streaming

Onyx uses the notion of a *sentinel value* to transparently switch between streaming and batching modes. This makes it really easy to be able to reuse the same code for both batch and streaming computations. It's considered good practice to back a stream processor with an underlying batch computation to regenerate the entire output data set at will (as seen by Nathan Marz's Lambda Architecture). Onyx makes this simple by using plain Clojure functions.

#### Aspect Orientation

Clojure functions again serve as a huge win. [Dire](https://github.com/MichaelDrogalis/dire) is a library that supports aspects, meaning you can keep your application logic water-tight away from logging, preconditions, and error handling.

#### AOT Nothing

Onyx AOT's absolutely nothing on your behalf. When you're ready to stand your jar up, simply uberjar and start executing on the target machine. Hadoop and Storm cause dependency hell (In Storm's case, you're restricted to Clojure 1.5.1 because you're locked in by the Executor) by providing their own dependencies on top of yours. Onyx won't mess with your dependencies.

