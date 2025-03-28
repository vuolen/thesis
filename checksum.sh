
folder=$1

echo "Checksum of file names: "
LC_ALL=POSIX
cd $folder
find . -type f -print0 | sort -z | sha1sum

echo "Checksum of item files: "
find . -type f -name "*_item.json" | sort | xargs jq '.files[].url, .files[].path, .files[].checksum' | sha1sum

cd - > /dev/null