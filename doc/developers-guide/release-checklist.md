## Release Checklist

We have a handful of tasks that need to be done every time we release.
Make sure each of these are completed at release time!

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](http://doctoc.herokuapp.com/)*

- [Pre-release](#pre-release)
- [Post-release](#post-release)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

### Pre-release

- Update `changes.md` to include any changes that went into this release. Prefix any backwards-incompatible changes with **Breaking change**.
- Update all example code in the `onyx-examples` repository.
- Update the version of Onyx used in `onyx-starter`.
- Update the version of Onyx used in `onyx-template.`
- Update the version of Onyx used in `onyx-plugin.`
- Write release notes in `doc/release-notes` if this is a major version. We typically use this to summarize the theme of the release.
- Ensure that the build matrix for Onyx is all green. This includes the `master` and release branch of all plugins and associated projects as passing. Further, if any projects run a `compatibility` branch to test Onyx's edge revision, ensure that their tests are passing too.
- Run the local performance benchmark by invoking `lein test` in the `onyx-benchmark` project. This is just a smoke test. We shouldn't see any major regressions here.
- Run the full AWS cloud benchmark overnight. Examine the graphs in Grafana and make sure there are no performance regressions. Ensure that the test is running for a long period of time to detect memory leaks.

### Post-release

- Tweet from the OnyxPlatform account.
- Announce on Gitter and Slack.
- Make a post on Reddit /r/clojure.
- Make a post on the Google Clojure group.
- Post to HackerNews if the release has substantially new features or performance gains.
- Send out a newsletter.