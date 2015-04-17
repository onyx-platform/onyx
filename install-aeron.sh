set -x
set -e
if [ ! -e Aeron ]; then
  git clone https://github.com/real-logic/Aeron.git
  cd Aeron
  git checkout cb3588760f88aac614ec5bbae9e05d1c7a675968
  ./gradlew
  gradle install
fi
