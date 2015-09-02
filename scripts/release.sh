#!/bin/bash

if [[ "$#" -ne 4 ]]; then
    echo "Usage: $0 old-version new-version old-release-branch new-release-branch"
    echo "Example: $0 0.7.1 0.8.0 0.7.x 0.8.x"
else
  # Update to release version.
  git checkout master
  git pull --rebase
  lein set-version $2
  sed -i '' "s/$1/$2/g" README.md
  sed -i '' "s/$3/$4/g" README.md
  sed -i '' "s/$3/$4/g" circle.yml
  git rm -rf doc/api
  lein doc

  # Push and deploy release.
  git add .
  git commit -m "Release version $2."
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
  git add .
  git commit -m "Prepare for next release cycle."
  git push origin master
fi
