#!/usr/bin/env bash
#
# Run all etcd tests
# ./scripts/test.sh
# ./scripts/test.sh -vq
#
#
# Run specified test pass
#
# $ PASSES=unit ./scripts/test.sh
# $ PASSES=integration ./scripts/test.sh
#
#
# Run tests for one package
# Each pass has different default timeout, if you just run tests in one package or 1 test case then you can set TIMEOUT
# flag for different expectation
#
# $ PASSES=unit PKG=./wal TIMEOUT=1m ./scripts/test.sh
# $ PASSES=integration PKG=./clientv3 TIMEOUT=1m ./scripts/test.sh
#
# Run specified unit tests in one package
# To run all the tests with prefix of "TestNew", set "TESTCASE=TestNew ";
# to run only "TestNew", set "TESTCASE="\bTestNew\b""
#
# $ PASSES=unit PKG=./wal TESTCASE=TestNew TIMEOUT=1m ./scripts/test.sh
# $ PASSES=unit PKG=./wal TESTCASE="\bTestNew\b" TIMEOUT=1m ./scripts/test.sh
# $ PASSES=integration PKG=./client/integration TESTCASE="\bTestV2NoRetryEOF\b" TIMEOUT=1m ./scripts/test.sh
#
# KEEP_GOING_SUITE must be set to true to keep going with the next suite execution, passed to PASSES variable when there is a failure
# in a particular suite.
# KEEP_GOING_MODULE must be set to true to keep going with execution when there is failure in any module.
#
# Run code coverage
# COVERDIR must either be a absolute path or a relative path to the etcd root
# $ COVERDIR=coverage PASSES="build cov" ./scripts/test.sh
# $ go tool cover -html ./coverage/cover.out
set -e

# Consider command as failed when any component of the pipe fails:
# https://stackoverflow.com/questions/1221833/pipe-output-and-capture-exit-status-in-bash
set -o pipefail
set -o nounset

# The test script is not supposed to make any changes to the files
# e.g. add/update missing dependencies. Such divergences should be 
# detected and trigger a failure that needs explicit developer's action.
export GOFLAGS=-mod=readonly
export ETCD_VERIFY=all

source ./scripts/test_lib.sh
source ./scripts/build_lib.sh

OUTPUT_FILE=${OUTPUT_FILE:-""}

if [ -n "${OUTPUT_FILE}" ]; then
  log_callout "Dumping output to: ${OUTPUT_FILE}"
  exec > >(tee -a "${OUTPUT_FILE}") 2>&1
fi

PASSES=${PASSES:-"gofmt bom dep build unit"}
KEEP_GOING_SUITE=${KEEP_GOING_SUITE:-false}
PKG=${PKG:-}
SHELLCHECK_VERSION=${SHELLCHECK_VERSION:-"v0.8.0"}

if [ -z "${GOARCH:-}" ]; then
  GOARCH=$(go env GOARCH);
fi

# determine whether target supports race detection
if [ -z "${RACE:-}" ] ; then
  if [ "$GOARCH" == "amd64" ]; then
    RACE="--race"
  else
    RACE="--race=false"
  fi
else
  RACE="--race=${RACE:-true}"
fi

# This options make sense for cases where SUT (System Under Test) is compiled by test.
COMMON_TEST_FLAGS=("${RACE}")
if [[ -n "${CPU:-}" ]]; then
  COMMON_TEST_FLAGS+=("--cpu=${CPU}")
fi 

log_callout "Running with ${COMMON_TEST_FLAGS[*]}"

RUN_ARG=()
if [ -n "${TESTCASE:-}" ]; then
  RUN_ARG=("-run=${TESTCASE}")
fi

function build_pass {
  log_callout "Building etcd"
  run_for_modules run go build "${@}" || return 2
  GO_BUILD_FLAGS="-v" etcd_build "${@}"
  GO_BUILD_FLAGS="-v" tools_build "${@}"
}

################# REGULAR TESTS ################################################

# run_unit_tests [pkgs] runs unit tests for a current module and givesn set of [pkgs]
function run_unit_tests {
  local pkgs="${1:-./...}"
  shift 1
  # shellcheck disable=SC2086
  GOLANG_TEST_SHORT=true go_test "${pkgs}" "parallel" : -short -timeout="${TIMEOUT:-3m}" "${COMMON_TEST_FLAGS[@]}" "${RUN_ARG[@]}" "$@"
}

function unit_pass {
  run_for_modules run_unit_tests "$@"
}

function integration_extra {
  if [ -z "${PKG}" ] ; then
    run_for_module "tests"  go_test "./integration/v2store/..." "keep_going" : -timeout="${TIMEOUT:-5m}" "${RUN_ARG[@]}" "${COMMON_TEST_FLAGS[@]}" "$@" || return $?
  else
    log_warning "integration_extra ignored when PKG is specified"
  fi
}

function integration_pass {
  run_for_module "tests" go_test "./integration/..." "parallel" : -timeout="${TIMEOUT:-15m}" "${COMMON_TEST_FLAGS[@]}" "${RUN_ARG[@]}" -p=2 "$@" || return $?
  run_for_module "tests" go_test "./common/..." "parallel" : --tags=integration -timeout="${TIMEOUT:-15m}" "${COMMON_TEST_FLAGS[@]}" -p=2 "${RUN_ARG[@]}" "$@" || return $?
  integration_extra "$@"
}

function e2e_pass {
  # e2e tests are running pre-build binary. Settings like --race,-cover,-cpu does not have any impact.
  run_for_module "tests" go_test "./e2e/..." "keep_going" : -timeout="${TIMEOUT:-30m}" "${RUN_ARG[@]}" "$@" || return $?
  run_for_module "tests" go_test "./common/..." "keep_going" : --tags=e2e -timeout="${TIMEOUT:-30m}" "${RUN_ARG[@]}" "$@"
}

function robustness_pass {
  # e2e tests are running pre-build binary. Settings like --race,-cover,-cpu does not have any impact.
  run_for_module "tests" go_test "./robustness" "keep_going" : -timeout="${TIMEOUT:-30m}" "${RUN_ARG[@]}" "$@"
}

function integration_e2e_pass {
  run_pass "integration" "${@}"
  run_pass "e2e" "${@}"
}

# generic_checker [cmd...]
# executes given command in the current module, and clearly fails if it
# failed or returned output.
function generic_checker {
  local cmd=("$@")
  if ! output=$("${cmd[@]}"); then
    echo "${output}"
    log_error -e "FAIL: '${cmd[*]}' checking failed (!=0 return code)"
    return 255
  fi
  if [ -n "${output}" ]; then
    echo "${output}"
    log_error -e "FAIL: '${cmd[*]}' checking failed (printed output)"
    return 255
  fi
}

function grpcproxy_pass {
  run_pass "grpcproxy_integration" "${@}"
  run_pass "grpcproxy_e2e" "${@}"
}

function grpcproxy_integration_pass {
  run_for_module "tests" go_test "./integration/..." "fail_fast" : \
      -timeout=30m -tags cluster_proxy "${COMMON_TEST_FLAGS[@]}" "$@"
}

function grpcproxy_e2e_pass {
  run_for_module "tests" go_test "./e2e" "fail_fast" : \
      -timeout=30m -tags cluster_proxy "${COMMON_TEST_FLAGS[@]}" "$@"
}

################# COVERAGE #####################################################

# pkg_to_coverflag [prefix] [pkgs]
# produces name of .coverprofile file to be used for tests of this package
function pkg_to_coverprofileflag {
  local prefix="${1}"
  local pkgs="${2}"
  local pkgs_normalized
  prefix_normalized=$(echo "${prefix}" | tr "./ " "__+")
  if [ "${pkgs}" == "./..." ]; then
    pkgs_normalized="all"
  else
    pkgs_normalized=$(echo "${pkgs}" | tr "./ " "__+")
  fi
  mkdir -p "${coverdir}/${prefix_normalized}"
  echo -n "-coverprofile=${coverdir}/${prefix_normalized}/${pkgs_normalized}.coverprofile"
}

function not_test_packages {
  for m in $(modules); do
    if [[ $m =~ .*/etcd/tests/v3 ]]; then continue; fi
    if [[ $m =~ .*/etcd/v3 ]]; then continue; fi
    echo "${m}/..."
  done
}

# split_dir [dir] [num]
function split_dir {
  local d="${1}"
  local num="${2}"
  local i=0
  for f in "${d}/"*; do
    local g=$(( i % num ))
    mkdir -p "${d}_${g}"
    mv "${f}" "${d}_${g}/"
    (( i++ ))
  done
}

function split_dir_pass {
  split_dir ./covdir/integration 4
}


# merge_cov_files [coverdir] [outfile]
# merges all coverprofile files into a single file in the given directory.
function merge_cov_files {
  local coverdir="${1}"
  local cover_out_file="${2}"
  log_callout "Merging coverage results in: ${coverdir}"
  # gocovmerge requires not-empty test to start with:
  echo "mode: set" > "${cover_out_file}"

  local i=0
  local count
  count=$(find "${coverdir}"/*.coverprofile | wc -l)
  for f in "${coverdir}"/*.coverprofile; do
    # print once per 20 files
    if ! (( "${i}" % 20 )); then
      log_callout "${i} of ${count}: Merging file: ${f}"
    fi
    run_go_tool "github.com/gyuho/gocovmerge" "${f}" "${cover_out_file}"  > "${coverdir}/cover.tmp" 2>/dev/null
    if [ -s "${coverdir}"/cover.tmp ]; then
      mv "${coverdir}/cover.tmp" "${cover_out_file}"
    fi
    (( i++ ))
  done
}

# merge_cov [coverdir]
function merge_cov {
  log_callout "[$(date)] Merging coverage files ..."
  coverdir="${1}"
  for d in "${coverdir}"/*/; do
    d=${d%*/}  # remove the trailing "/"
    merge_cov_files "${d}" "${d}.coverprofile" &
  done
  wait
  merge_cov_files "${coverdir}" "${coverdir}/all.coverprofile"
}

function cov_pass {
  # shellcheck disable=SC2153
  if [ -z "${COVERDIR:-}" ]; then
    log_error "COVERDIR undeclared"
    return 255
  fi

  local coverdir
  coverdir=$(readlink -f "${COVERDIR}")
  mkdir -p "${coverdir}"
  find "${coverdir}" -print0 -name '*.coverprofile' | xargs -0 rm

  local covpkgs
  covpkgs=$(not_test_packages)
  local coverpkg_comma
  coverpkg_comma=$(echo "${covpkgs[@]}" | xargs | tr ' ' ',')
  local gocov_build_flags=("-covermode=set" "-coverpkg=$coverpkg_comma")

  local failed=""

  log_callout "[$(date)] Collecting coverage from unit tests ..."
  for m in $(module_dirs); do
    GOLANG_TEST_SHORT=true run_for_module "${m}" go_test "./..." "parallel" "pkg_to_coverprofileflag unit_${m}" -short -timeout=30m \
       "${gocov_build_flags[@]}" "$@" || failed="$failed unit"
  done

  log_callout "[$(date)] Collecting coverage from integration tests ..."
  run_for_module "tests" go_test "./integration/..." "parallel" "pkg_to_coverprofileflag integration" \
      -timeout=30m "${gocov_build_flags[@]}" "$@" || failed="$failed integration"
  # integration-store-v2
  run_for_module "tests" go_test "./integration/v2store/..." "keep_going" "pkg_to_coverprofileflag store_v2" \
      -timeout=5m "${gocov_build_flags[@]}" "$@" || failed="$failed integration_v2"
  # integration_cluster_proxy
  run_for_module "tests" go_test "./integration/..." "parallel" "pkg_to_coverprofileflag integration_cluster_proxy" \
      -tags cluster_proxy -timeout=30m "${gocov_build_flags[@]}" || failed="$failed integration_cluster_proxy"

  local cover_out_file="${coverdir}/all.coverprofile"
  merge_cov "${coverdir}"

  # strip out generated files (using GNU-style sed)
  sed --in-place -E "/[.]pb[.](gw[.])?go/d" "${cover_out_file}" || true

  sed --in-place -E "s|go.etcd.io/etcd/api/v3/|api/|g" "${cover_out_file}" || true
  sed --in-place -E "s|go.etcd.io/etcd/client/v3/|client/v3/|g" "${cover_out_file}" || true
  sed --in-place -E "s|go.etcd.io/etcd/client/pkg/v3|client/pkg/v3/|g" "${cover_out_file}" || true
  sed --in-place -E "s|go.etcd.io/etcd/etcdctl/v3/|etcdctl/|g" "${cover_out_file}" || true
  sed --in-place -E "s|go.etcd.io/etcd/etcdutl/v3/|etcdutl/|g" "${cover_out_file}" || true
  sed --in-place -E "s|go.etcd.io/etcd/pkg/v3/|pkg/|g" "${cover_out_file}" || true
  sed --in-place -E "s|go.etcd.io/etcd/server/v3/|server/|g" "${cover_out_file}" || true

  # held failures to generate the full coverage file, now fail
  if [ -n "$failed" ]; then
    for f in $failed; do
      log_error "--- FAIL:" "$f"
    done
    log_warning "Despite failures, you can see partial report:"
    log_warning "  go tool cover -html ${cover_out_file}"
    return 255
  fi

  log_success "done :) [see report: go tool cover -html ${cover_out_file}]"
}

######### Code formatting checkers #############################################

function shellcheck_pass {
  SHELLCHECK=shellcheck
  if ! tool_exists "shellcheck" "https://github.com/koalaman/shellcheck#installing"; then
    log_callout "Installing shellcheck $SHELLCHECK_VERSION"
    wget -qO- "https://github.com/koalaman/shellcheck/releases/download/${SHELLCHECK_VERSION}/shellcheck-${SHELLCHECK_VERSION}.linux.x86_64.tar.xz" | tar -xJv -C /tmp/ --strip-components=1
    mkdir -p ./bin
    mv /tmp/shellcheck ./bin/
    SHELLCHECK=./bin/shellcheck
  fi
  generic_checker run ${SHELLCHECK} -fgcc scripts/*.sh
}

function shellws_pass {
  TAB=$'\t'
  log_callout "Ensuring no tab-based indention in shell scripts"
  local files
  files=$(find ./ -name '*.sh' -print0 | xargs -0 )
  # shellcheck disable=SC2206
  files=( ${files[@]} "./scripts/build-binary.sh" "./scripts/build-docker.sh" "./scripts/release.sh" )
  log_cmd "grep -E -n $'^ *${TAB}' ${files[*]}"
  # shellcheck disable=SC2086
  if grep -E -n $'^ *${TAB}' "${files[@]}" | sed $'s|${TAB}|[\\\\tab]|g'; then
    log_error "FAIL: found tab-based indention in bash scripts. Use '  ' (double space)."
    local files_with_tabs
    files_with_tabs=$(grep -E -l $'^ *\\t' "${files[@]}")
    log_warning "Try: sed -i 's|\\t|  |g' $files_with_tabs"
    return 1
  else
    log_success "SUCCESS: no tabulators found."
    return 0
  fi
}

function markdown_you_find_eschew_you {
  local find_you_cmd="find . -name \\*.md ! -path '*/vendor/*' ! -path './Documentation/*' ! -path './gopath.proto/*' ! -path './release/*' -exec grep -E --color '[Yy]ou[r]?[ '\\''.,;]' {} + || true"
  run eval "${find_you_cmd}"
}

function markdown_you_pass {
  # TODO: ./CONTRIBUTING.md:## Get your pull request reviewed
  generic_checker markdown_you_find_eschew_you
}

function markdown_marker_pass {
  # TODO: check other markdown files when marker handles headers with '[]'
  if tool_exists "marker" "https://crates.io/crates/marker"; then
    generic_checker run marker --skip-http --root ./Documentation 2>&1
  fi
}

function govet_pass {
  run_for_modules generic_checker run go vet
}

function govet_shadow_pass {
  # TODO: we should ignore the generated packages?
  #
  # stderr: etcdserverpb/gw/rpc.pb.gw.go:2100:3: declaration of "ctx" shadows declaration at line 2005
  local shadow
  shadow=$(tool_get_bin "golang.org/x/tools/go/analysis/passes/shadow/cmd/shadow")
  run_for_modules generic_checker run go vet -all -vettool="${shadow}"
}

function unparam_pass {
  # TODO: transport/listener.go:129:60: newListenConfig - result 1 (error) is always nil
  run_for_modules generic_checker run_go_tool "mvdan.cc/unparam"
}

function staticcheck_pass {
  # TODO: we should upgrade pb or ignore the pb package
  #
  # versionpb/version.pb.go:69:15: proto.RegisterFile is deprecated: Use protoregistry.GlobalFiles.RegisterFile instead.  (SA1019)
  run_for_modules generic_checker run_go_tool "honnef.co/go/tools/cmd/staticcheck"
}

function revive_pass {
  # TODO: etcdserverpb/raft_internal_stringer.go:15:1: should have a package comment
  run_for_modules generic_checker run_go_tool "github.com/mgechev/revive" -config "${ETCD_ROOT_DIR}/tests/revive.toml" -exclude "vendor/..." -exclude "out/..."
}

function unconvert_pass {
  # TODO: pb package should be filtered out.
  run_for_modules generic_checker run_go_tool "github.com/mdempsky/unconvert" unconvert -v
}

function ineffassign_per_package {
  # bash 3.x compatible replacement of: mapfile -t gofiles < <(go_srcs_in_module)
  local gofiles=()
  while IFS= read -r line; do gofiles+=("$line"); done < <(go_srcs_in_module)

  # TODO: ineffassign should work with package instead of files
  run_go_tool github.com/gordonklaus/ineffassign "${gofiles[@]}"
}

function ineffassign_pass {
  run_for_modules generic_checker ineffassign_per_package
}

function nakedret_pass {
  # TODO: nakedret should work with -set_exit_status
  run_for_modules generic_checker run_go_tool "github.com/alexkohler/nakedret"
}

function license_header_per_module {
  # bash 3.x compatible replacement of: mapfile -t gofiles < <(go_srcs_in_module)
  local gofiles=()
  while IFS= read -r line; do gofiles+=("$line"); done < <(go_srcs_in_module)
  run_go_tool "github.com/google/addlicense" --check "${gofiles[@]}"
}

function license_header_pass {
  run_for_modules generic_checker license_header_per_module
}

function receiver_name_for_package {
  # bash 3.x compatible replacement of: mapfile -t gofiles < <(go_srcs_in_module)
  local gofiles=()
  while IFS= read -r line; do gofiles+=("$line"); done < <(go_srcs_in_module)

  recvs=$(grep 'func ([^*]' "${gofiles[@]}"  | tr  ':' ' ' |  \
    awk ' { print $2" "$3" "$4" "$1 }' | sed "s/[a-zA-Z\\.]*go//g" |  sort  | uniq  | \
    grep -Ev  "(Descriptor|Proto|_)"  | awk ' { print $3" "$4 } ' | sort | uniq -c | grep -v ' 1 ' | awk ' { print $2 } ')
  if [ -n "${recvs}" ]; then
    # shellcheck disable=SC2206
    recvs=($recvs)
    for recv in "${recvs[@]}"; do
      log_error "Mismatched receiver for $recv..."
      grep "$recv" "${gofiles[@]}" | grep 'func ('
    done
    return 255
  fi
}

function receiver_name_pass {
  run_for_modules receiver_name_for_package
}

# goword_for_package package
# checks spelling and comments in the 'package' in the current module
#
function goword_for_package {
  # bash 3.x compatible replacement of: mapfile -t gofiles < <(go_srcs_in_module)
  local gofiles=()
  while IFS= read -r line; do gofiles+=("$line"); done < <(go_srcs_in_module)
  
  local gowordRes

  # spellchecking can be enabled with GOBINARGS="--tags=spell"
  # but it requires heavy dependencies installation, like:
  # apt-get install libaspell-dev libhunspell-dev hunspell-en-us aspell-en

  # only check for broke exported godocs
  if gowordRes=$(run_go_tool "github.com/chzchzchz/goword" -use-spell=false "${gofiles[@]}" | grep godoc-export | sort); then
    log_error -e "goword checking failed:\\n${gowordRes}"
    return 255
  fi
  if [ -n "$gowordRes" ]; then
    log_error -e "goword checking returned output:\\n${gowordRes}"
    return 255
  fi
}


function goword_pass {
  run_for_modules goword_for_package || return 255
}

function go_fmt_for_package {
  # We utilize 'go fmt' to find all files suitable for formatting,
  # but reuse full power gofmt to perform just RO check.
  go fmt -n "$1" | sed 's| -w | -d |g' | sh
}

function gofmt_pass {
  run_for_modules generic_checker go_fmt_for_package
}

function bom_pass {
  log_callout "Checking bill of materials..."
  # https://github.com/golang/go/commit/7c388cc89c76bc7167287fb488afcaf5a4aa12bf
  # shellcheck disable=SC2207
  modules=($(modules_exp))

  # Internally license-bill-of-materials tends to modify go.sum
  run cp go.sum go.sum.tmp || return 2
  run cp go.mod go.mod.tmp || return 2

  output=$(GOFLAGS=-mod=mod run_go_tool github.com/coreos/license-bill-of-materials \
    --override-file ./bill-of-materials.override.json \
    "${modules[@]}")
  code="$?"

  run cp go.sum.tmp go.sum || return 2
  run cp go.mod.tmp go.mod || return 2

  if [ "${code}" -ne 0 ] ; then
    log_error -e "license-bill-of-materials (code: ${code}) failed with:\\n${output}"
    return 255
  else
    echo "${output}" > "bom-now.json.tmp"
  fi
  if ! diff ./bill-of-materials.json bom-now.json.tmp; then
    log_error "modularized licenses do not match given bill of materials"
    return 255
  fi
  rm bom-now.json.tmp
}

######## VARIOUS CHECKERS ######################################################

function dump_deps_of_module() {
  local module
  if ! module=$(run go list -m); then
    return 255
  fi
  run go list -f "{{if not .Indirect}}{{if .Version}}{{.Path}},{{.Version}},${module}{{end}}{{end}}" -m all
}

# Checks whether dependencies are consistent across modules
function dep_pass {
  local all_dependencies
  all_dependencies=$(run_for_modules dump_deps_of_module | sort) || return 2

  local duplicates
  duplicates=$(echo "${all_dependencies}" | cut -d ',' -f 1,2 | sort | uniq | cut -d ',' -f 1 | sort | uniq -d) || return 2

  for dup in ${duplicates}; do
    log_error "FAIL: inconsistent versions for depencency: ${dup}"
    echo "${all_dependencies}" | grep "${dup}" | sed "s|\\([^,]*\\),\\([^,]*\\),\\([^,]*\\)|  - \\1@\\2 from: \\3|g"
  done
  if [[ -n "${duplicates}" ]]; then
    log_error "FAIL: inconsistent dependencies"
    return 2
  else
    log_success "SUCCESS: dependencies are consistent across modules"
  fi
}

function release_pass {
  rm -f ./bin/etcd-last-release
  # to grab latest patch release; bump this up for every minor release
  UPGRADE_VER=$(git tag -l --sort=-version:refname "v3.5.*" | head -1 | cut -d- -f1)
  if [ -n "${MANUAL_VER:-}" ]; then
    # in case, we need to test against different version
    UPGRADE_VER=$MANUAL_VER
  fi
  if [[ -z ${UPGRADE_VER} ]]; then
    UPGRADE_VER="v3.5.0"
    log_warning "fallback to" ${UPGRADE_VER}
  fi

  local file="etcd-$UPGRADE_VER-linux-$GOARCH.tar.gz"
  log_callout "Downloading $file"

  set +e
  curl --fail -L "https://github.com/etcd-io/etcd/releases/download/$UPGRADE_VER/$file" -o "/tmp/$file"
  local result=$?
  set -e
  case $result in
    0)  ;;
    *)  log_error "--- FAIL:" ${result}
      return $result
      ;;
  esac

  tar xzvf "/tmp/$file" -C /tmp/ --strip-components=1
  mkdir -p ./bin
  mv /tmp/etcd ./bin/etcd-last-release
}

function mod_tidy_for_module {
  # Watch for upstream solution: https://github.com/golang/go/issues/27005
  local tmpModDir
  tmpModDir=$(mktemp -d -t 'tmpModDir.XXXXXX')
  run cp "./go.mod" "${tmpModDir}" || return 2

  # Guarantees keeping go.sum minimal
  # If this is causing too much problems, we should
  # stop controlling go.sum at all.
  rm go.sum
  run go mod tidy || return 2

  set +e
  local tmpFileGoModInSync
  diff -C 5 "${tmpModDir}/go.mod" "./go.mod"
  tmpFileGoModInSync="$?"

  # Bring back initial state
  mv "${tmpModDir}/go.mod" "./go.mod"

  if [ "${tmpFileGoModInSync}" -ne 0 ]; then
    log_error "${PWD}/go.mod is not in sync with 'go mod tidy'"
    return 255
  fi
  set -e
}

function mod_tidy_pass {
  run_for_modules mod_tidy_for_module
}

function proto_annotations_pass {
  "${ETCD_ROOT_DIR}/scripts/verify_proto_annotations.sh"
}

function genproto_pass {
  "${ETCD_ROOT_DIR}/scripts/verify_genproto.sh"
}

function goimport_for_module {
  GOFILES=$(run go list  --f "{{with \$d:=.}}{{range .GoFiles}}{{\$d.Dir}}/{{.}}{{\"\n\"}}{{end}}{{end}}" ./...) || return 2
  TESTGOFILES=$(run go list  --f "{{with \$d:=.}}{{range .TestGoFiles}}{{\$d.Dir}}/{{.}}{{\"\n\"}}{{end}}{{end}}" ./...) || return 2
  cd "${ETCD_ROOT_DIR}/tools/mod"
  FILESNEEDSFIX=$(echo "${GOFILES}" "${TESTGOFILES}" | grep -v '.gw.go' | grep -v '.pb.go' | xargs -n 100 go run golang.org/x/tools/cmd/goimports -l -local go.etcd.io)
  if [ -n "$FILESNEEDSFIX" ]; then
    log_error -e "the following files are not sync with 'goimports'. run 'make fix'\\n$FILESNEEDSFIX"
    return 255
  fi
}

function goimport_pass {
  run_for_modules goimport_for_module
}

########### MAIN ###############################################################

function run_pass {
  local pass="${1}"
  shift 1
  log_callout -e "\\n'${pass}' started at $(date)"
  if "${pass}_pass" "$@" ; then
    log_success "'${pass}' PASSED and completed at $(date)"
    return 0
  else
    log_error "FAIL: '${pass}' FAILED at $(date)"
    if [ "$KEEP_GOING_SUITE" = true ]; then
      return 2
    else
      exit 255
    fi
  fi
}

log_callout "Starting at: $(date)"
fail_flag=false
for pass in $PASSES; do
  if run_pass "${pass}" "${@}"; then
    continue
  else
    fail_flag=true
  fi
done
if [ "$fail_flag" = true ]; then
  log_error "There was FAILURE in the test suites ran. Look above log detail"
  exit 255
fi

log_success "SUCCESS"
