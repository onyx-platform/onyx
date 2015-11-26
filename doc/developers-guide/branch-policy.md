## Branch Policy

This section describes which branches to submit changes to and we how tend to work in general.

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](http://doctoc.herokuapp.com/)*

- [Day-to-Day Development](#day-to-day-development)
- [Release Branches](#release-branches)
- [Merging Pull Requests](#merging-pull-requests)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

### Day-to-Day Development

In the Onyx core repository, we branch off of the `develop` branch, make changes, and submit pull requests back to `develop`. We consider develop to be "unstable". All changes on develop should be code reviewed, but may not necessarily have been benchmarked. Further, patches to `develop` have not been checked against dependent plugin repositories. This happens on `master`.

When we're ready to move closer to a release, we merge `develop` into `master`. Master is considered to be our stable branch - something we can ship at any point in time. Whenever code is pushed to `master`, our automatic deployment system notifies all dependent projects (plugins, metrics, templates). These dependent projects update their dependency on Onyx to the Git SHA of Onyx core that caused them to be notified. Their tests run against this edge build. `master` should be benchmarked as we near release time.

Any urgent bug fixes can be applied directly to master - this includes minor documentation fixes.

### Release Branches

When we're ready to release, we execute the deployment scripts that we created in each repository. Another section of this guide goes into exactly how we deploy. The important thing that we want to note in this section is that the code from `master` is merged into a release branch, such as `0.8.x`. We typically set the release branch as the default branch on GitHub. Any urgent fixes may be backported from master into older release branches. As a result, we never commit directly to release branches - code always goes through `master`, at a minimum, and more typically through `develop`.

### Merging Pull Requests

We often get pull requests fixing minor things. Since our default branch is a release branch, contributors usually don't read this and send their pull request without changing the branch. For minor things, it's acceptable to merge the pull request on the release branch and promptly merging the patch back into `master`, then into `develop`. For other patches, you might want to take the patch directly on master. Rather than asking the contributor to change their branch (this is a pain on GitHub), you can merge the patch from the command line:

```text
$ git checkout -b the-patch-branch master
$ git pull https://github.com/contributor/onyx.git the-patch-branch

# Ensure patch sanity here.

$ git checkout master
$ git merge --no-ff the-patch-branch
$ git push origin master
```
