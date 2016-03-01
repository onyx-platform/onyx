#!/bin/bash

set -o errexit
set -o pipefail
set -o nounset
set -o xtrace

cd "$(dirname "$0")/.."

if [[ "$#" -ne 2 ]]; then
	echo "Usage: $0 new-version release-branch"
	echo "Example: $0 0.8.0.4 0.8.x"
	echo "Three digit release number e.g. 0.8.0 will update onyx dependency and release plugin as 0.8.0.0"
fi

new_version=$1

version_type=$(echo "$1"|sed s/".*-"//g)
version_base=$(echo "$1"|sed s/"-.*"//g)

#biggest_tag=$(git tag|tail -1)
#if [[ $biggest_tag > $version_base || $biggest_tag == $version_base && $version_type != "" ]]; then
#	echo $version_base" version base is smaller than greatest tag" $biggest_tag
#	echo "enter Y to override and release anyway."
#	read answer
#	if [[ $answer != "Y" ]]; then
#		exit 1
#	fi
#fi

release_branch=$2
current_version=`lein pprint :version | sed s/\"//g`

# Update to release version.
git checkout master
git stash
git pull

if [[ "$new_version" == *[.]*[.]*[.]* ]]; 
then 
	echo "Four digit release number "$new_version" therefore releasing plugin without updating Onyx dependency"
	new_plugin_version=$new_version
elif [[ "$new_version" == *[.]*[.]* ]]; 
then
	core_version=$new_version
	lein update-dependency org.onyxplatform/onyx $core_version
	if [[ "$version_type" == "$new_version" ]]; then 
		new_plugin_version=$version_base".0"
	else
		new_plugin_version=$version_base".0-"$version_type
	fi

else
	echo "Unhandled version number scheme. Exiting"
	exit 1
fi

lein set-version $new_plugin_version

sed -i.bak "s/$current_version/$new_plugin_version/g" README.md
git add README.md project.clj

git commit -m "Release version $new_plugin_version."
git tag $new_plugin_version
git push origin $new_plugin_version
git push origin master

# Merge artifacts into release branch.
git checkout -b $release_branch || git checkout $release_branch
git pull || true
git merge -m "Merge branch 'master' into $release_branch" master -X theirs
git push -u origin $release_branch

# Prepare next release cycle.
git checkout master
lein set-version
snapshot_version=`lein pprint :version | sed s/\"//g`
sed -i.bak "s/$new_plugin_version/$snapshot_version/g" README.md
git commit -m "Prepare for next release cycle." project.clj README.md
git push origin master
