## Branch Policy

This section describes which branches to submit changes to and how we tend to work in general.

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](http://doctoc.herokuapp.com/)*

- [Day-to-Day Development](#day-to-day-development)
- [Release Branches](#release-branches)
- [Merging Pull Requests](#merging-pull-requests)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

### Day-to-Day Development

In the Onyx core repository, we branch off of the `master` branch, make changes, and submit pull requests back to `master`.

### Release Branches

When we're ready to release, we execute the deployment scripts that we created in each repository. Another section of this guide goes into exactly how we deploy. The important thing that we want to note in this section is that the code from `master` is merged into a release branch, such as `0.8.x`. We typically set the release branch as the default branch on GitHub. Any urgent fixes may be backported from master into older release branches. As a result, we never commit directly to release branches - code always goes through `master`.

### Merging Pull Requests

We often get pull requests fixing minor things. Since our default branch is a release branch, contributors usually don't read this and send their pull request without changing the branch. For minor things, it's acceptable to merge the pull request on the release branch and promptly merging the patch back into `master`. For other patches, you might want to take the patch directly on master. Rather than asking the contributor to change their branch (this is a pain on GitHub), you can merge the patch from the command line:

```text
$ git checkout -b the-patch-branch master
$ git pull https://github.com/contributor/onyx.git the-patch-branch

# Ensure patch sanity here.

$ git checkout master
$ git merge --no-ff the-patch-branch
$ git push origin master
```
