#!/bin/bash

# Update to release version.
git checkout master
lein set-version $2
sed -i '' "s/$1/$2/g" README.md
sed -i '' "s/$3/$4/g" README.md
sed -i '' "s/$3/$4/g" circle.yml
git rm -rf doc/api
lein doc

# Push and deploy release.
git commit -am "Release version $2."
git tag $2
git push origin $2
git push origin master

# Merge artifacts into release branch.
git checkout $4
git merge master
git push origin $4

# Prepare next release cycle.
git checkout master
lein set-version
git commit -am "Prepare for next release cycle."
git push origin master
