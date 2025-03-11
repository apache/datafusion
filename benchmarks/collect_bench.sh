trap 'git checkout main' EXIT #checkout to main on exit
ARG1=$1
echo $ARG1

main(){

git fetch upstream main
git checkout main

# get current major version 
output=$(cargo metadata --format-version=1 --no-deps | jq '.packages[] | select(.name == "datafusion") | .version')
major_version=$(echo "$output" | grep -oE '[0-9]+' | head -n1)

# run for current main
echo "current major version: $major_version"  
export RESULTS_DIR="results/main"
./bench.sh run $ARG1

# run for last 5 major releases
for i in {1..5}; do
    echo "running benchmark on  $((major_version-i)).0.0"
    git fetch upstream $((major_version-i)).0.0
    git checkout $((major_version-i)).0.0
    export RESULTS_DIR="results/$((major_version-i)).0.0"
    ./bench.sh run $ARG1
done
}

main