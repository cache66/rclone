#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TMP_DIR="${TMPDIR:-/tmp}/rclone-resume-v1-$$"
BIN_PATH="${TMP_DIR}/rclone"
CACHE_DIR="${TMP_DIR}/cache"
RCLONE_CONFIG="${TMP_DIR}/rclone.conf"
RC_PORT="${RESUME_TEST_RC_PORT:-55742}"
RC_URL="http://127.0.0.1:${RC_PORT}"
KEEP_TMP="${RESUME_TEST_KEEP_TMP:-0}"
S3_REMOTE="${RESUME_TEST_S3_REMOTE:-}"
EAS_S3_CONF="${RESUME_TEST_EAS_S3_CONF:-$HOME/eas-s3-prod/s3proxy-3.0.0/conf/s3proxy-tidb.local.conf}"
EAS_S3_REMOTE_NAME="resume-eas-s3"
LOCAL_INTERRUPT_TIMEOUT="${RESUME_TEST_LOCAL_TIMEOUT:-2}"
S3_INTERRUPT_TIMEOUT="${RESUME_TEST_S3_TIMEOUT:-8}"

RCLONE_PID=""

cleanup() {
	if [[ -n "${RCLONE_PID}" ]] && kill -0 "${RCLONE_PID}" 2>/dev/null; then
		kill "${RCLONE_PID}" >/dev/null 2>&1 || true
		wait "${RCLONE_PID}" >/dev/null 2>&1 || true
	fi
	if [[ "${KEEP_TMP}" != "1" ]]; then
		rm -rf "${TMP_DIR}"
	else
		echo "保留测试目录: ${TMP_DIR}"
	fi
}
trap cleanup EXIT

need_cmd() {
	command -v "$1" >/dev/null 2>&1 || {
		echo "缺少依赖命令: $1" >&2
		exit 1
	}
}

log() {
	printf '\n[%s] %s\n' "$(date '+%H:%M:%S')" "$*"
}

fail() {
	echo "失败: $*" >&2
	exit 1
}

api_post() {
	local path="$1"
	local data="${2:-{}}"
	curl -fsS -X POST \
		-H 'Content-Type: application/json' \
		-d "${data}" \
		"${RC_URL}/${path}"
}

rclone_cmd() {
	"${BIN_PATH}" --config "${RCLONE_CONFIG}" "$@"
}

assert_file_exists() {
	[[ -f "$1" ]] || fail "文件不存在: $1"
}

assert_dir_exists() {
	[[ -d "$1" ]] || fail "目录不存在: $1"
}

assert_json() {
	local json="$1"
	local expr="$2"
	printf '%s' "${json}" | jq -e "${expr}" >/dev/null || fail "JSON 断言失败: ${expr}"
}

assert_json_gt() {
	local json="$1"
	local expr="$2"
	local value
	value="$(printf '%s' "${json}" | jq -r "${expr}")"
	[[ "${value}" =~ ^[0-9]+$ ]] || fail "JSON 数值断言失败: ${expr} -> ${value}"
	(( value > 0 )) || fail "JSON 数值未大于 0: ${expr} -> ${value}"
}

assert_json_eq_text() {
	local json="$1"
	local expr="$2"
	local expected="$3"
	local value
	value="$(printf '%s' "${json}" | jq -r "${expr}")"
	[[ "${value}" == "${expected}" ]] || fail "JSON 文本断言失败: ${expr} -> ${value}, 期望 ${expected}"
}

make_large_tree() {
	local root="$1"
	rm -rf "${root}"
	mkdir -p "${root}/a/aa" "${root}/b/bb" "${root}/empty/inner"
	for i in $(seq 1 12); do
		dd if=/dev/zero of="${root}/a/file-${i}.bin" bs=256K count=1 status=none
	done
	for i in $(seq 1 8); do
		dd if=/dev/zero of="${root}/b/bb/file-${i}.bin" bs=256K count=1 status=none
	done
	printf 'resume-local\n' > "${root}/root.txt"
}

build_rclone() {
	log "构建 rclone 测试二进制"
	mkdir -p "${TMP_DIR}" "${CACHE_DIR}"
	(
		cd "${ROOT_DIR}"
		GOPROXY=https://goproxy.cn,direct go build -o "${BIN_PATH}" .
	)
}

setup_rclone_config() {
	: > "${RCLONE_CONFIG}"

	if [[ -n "${S3_REMOTE}" ]]; then
		log "使用外部指定的 S3 远端: ${S3_REMOTE}"
		return
	fi

	if [[ ! -f "${EAS_S3_CONF}" ]]; then
		log "未找到本地 eas-s3 配置，S3 场景将跳过"
		return
	fi

	local endpoint identity credential console_user console_password
	endpoint="$(sed -n 's/^s3proxy\.endpoint=//p' "${EAS_S3_CONF}" | tail -n 1)"
	identity="$(sed -n 's/^s3proxy\.identity=//p' "${EAS_S3_CONF}" | tail -n 1)"
	credential="$(sed -n 's/^s3proxy\.credential=//p' "${EAS_S3_CONF}" | tail -n 1)"
	console_user="$(sed -n 's/^s3proxy\.console\.auth\.bootstrap-username=//p' "${EAS_S3_CONF}" | tail -n 1)"
	console_password="$(sed -n 's/^s3proxy\.console\.auth\.bootstrap-password=//p' "${EAS_S3_CONF}" | tail -n 1)"

	if [[ -n "${console_user}" && -n "${console_password}" ]]; then
		identity="${console_user}"
		credential="${console_password}"
	fi

	if [[ -z "${endpoint}" || -z "${identity}" || -z "${credential}" ]]; then
		log "本地 eas-s3 配置不完整，S3 场景将跳过"
		return
	fi

	cat > "${RCLONE_CONFIG}" <<EOF
[${EAS_S3_REMOTE_NAME}]
type = s3
provider = Minio
access_key_id = ${identity}
secret_access_key = ${credential}
endpoint = ${endpoint}
v2_auth = false
EOF

	S3_REMOTE="${EAS_S3_REMOTE_NAME}:"
	log "已自动对接本地 eas-s3: ${endpoint}"
}

start_rcd() {
	log "启动 RC 服务 ${RC_URL}"
	"${BIN_PATH}" rcd \
		--rc-no-auth \
		--rc-addr "127.0.0.1:${RC_PORT}" \
		--cache-dir "${CACHE_DIR}" \
		--log-level INFO >/dev/null 2>&1 &
	RCLONE_PID=$!

	for _ in $(seq 1 20); do
		if curl -fsS -X POST "${RC_URL}/rc/noop" >/dev/null 2>&1; then
			return
		fi
		sleep 0.5
	done
	fail "RC 服务启动失败"
}

run_local_interrupt_resume() {
	local src="${TMP_DIR}/local-src"
	local dst="${TMP_DIR}/local-dst"
	local job_id="resume-local-interrupt"

	log "场景 1: 本地文件系统扫描/传输中断后继续"
	make_large_tree "${src}"
	rm -rf "${dst}"
	mkdir -p "${dst}"

	set +e
	timeout --signal=TERM "${LOCAL_INTERRUPT_TIMEOUT}s" \
		"${BIN_PATH}" copy "${src}" "${dst}" \
		--resume \
		--resume-id "${job_id}" \
		--cache-dir "${CACHE_DIR}" \
		--transfers 1 \
		--checkers 1 \
		--bwlimit 128k \
		--create-empty-src-dirs >/dev/null 2>&1
	local rc=$?
	set -e
	[[ ${rc} -ne 0 ]] || fail "预期第一次 copy 被中断"

	local status_json
	status_json="$(api_post "sync/resume/status" "{\"jobId\":\"${job_id}\"}")"
	assert_json "${status_json}" '.found == true'

	"${BIN_PATH}" copy "${src}" "${dst}" \
		--resume \
		--resume-id "${job_id}" \
		--cache-dir "${CACHE_DIR}" \
		--transfers 4 \
		--checkers 4 \
		--create-empty-src-dirs >/dev/null

	"${BIN_PATH}" check "${src}" "${dst}" --one-way >/dev/null
	assert_dir_exists "${dst}/empty"
	assert_dir_exists "${dst}/empty/inner"

	status_json="$(api_post "sync/resume/status" "{\"jobId\":\"${job_id}\"}")"
	assert_json "${status_json}" '.found == false'
}

run_failed_retry_resume() {
	local src="${TMP_DIR}/fail-src"
	local dst="${TMP_DIR}/fail-dst"
	local job_id="resume-local-failed-retry"

	log "场景 2: 失败文件记录与下次优先重试"
	rm -rf "${src}" "${dst}"
	mkdir -p "${src}" "${dst}"
	printf 'ok\n' > "${src}/ok.txt"
	printf 'blocked\n' > "${src}/blocked.txt"
	chmod 000 "${src}/blocked.txt"

	set +e
	"${BIN_PATH}" copy "${src}" "${dst}" \
		--resume \
		--resume-id "${job_id}" \
		--cache-dir "${CACHE_DIR}" \
		--resume-error-limit 10 >/dev/null 2>&1
	local rc=$?
	set -e
	[[ ${rc} -ne 0 ]] || fail "预期第一次 copy 因失败文件退出"

	local status_json
	status_json="$(api_post "sync/resume/status" "{\"jobId\":\"${job_id}\"}")"
	assert_json "${status_json}" '.found == true'
	assert_json "${status_json}" '.totals.pending_failed_count == 1'
	assert_json "${status_json}" '.failed | length == 1'

	chmod 644 "${src}/blocked.txt"
	"${BIN_PATH}" copy "${src}" "${dst}" \
		--resume \
		--resume-id "${job_id}" \
		--cache-dir "${CACHE_DIR}" >/dev/null

	assert_file_exists "${dst}/ok.txt"
	assert_file_exists "${dst}/blocked.txt"
	"${BIN_PATH}" check "${src}" "${dst}" --one-way >/dev/null

	status_json="$(api_post "sync/resume/status" "{\"jobId\":\"${job_id}\"}")"
	assert_json "${status_json}" '.found == false'
}

run_clear_api() {
	local src="${TMP_DIR}/clear-src"
	local dst="${TMP_DIR}/clear-dst"

	log "场景 3: status/clear 接口"
	make_large_tree "${src}"
	rm -rf "${dst}"
	mkdir -p "${dst}"

	set +e
	timeout --signal=TERM "${LOCAL_INTERRUPT_TIMEOUT}s" \
		"${BIN_PATH}" copy "${src}" "${dst}" \
		--resume \
		--cache-dir "${CACHE_DIR}" \
		--transfers 1 \
		--checkers 1 \
		--bwlimit 128k >/dev/null 2>&1
	set -e

	local status_json
	status_json="$(api_post "sync/resume/status" "{\"srcFs\":\"${src}\",\"dstFs\":\"${dst}\"}")"
	assert_json "${status_json}" '.found == true'
	local job_id
	job_id="$(printf '%s' "${status_json}" | jq -r '.jobId')"
	[[ -n "${job_id}" && "${job_id}" != "null" ]] || fail "未能从 status 接口获取 jobId"

	local clear_json
	clear_json="$(api_post "sync/resume/clear" "{\"srcFs\":\"${src}\",\"dstFs\":\"${dst}\"}")"
	assert_json "${clear_json}" '.cleared == true'

	status_json="$(api_post "sync/resume/status" "{\"jobId\":\"${job_id}\"}")"
	assert_json "${status_json}" '.found == false'
}

run_error_limit_stop() {
	local src="${TMP_DIR}/limit-src"
	local dst="${TMP_DIR}/limit-dst"
	local job_id="resume-local-error-limit"

	log "场景 4: 失败上限超限后停止并保留状态"
	rm -rf "${src}" "${dst}"
	mkdir -p "${src}" "${dst}"
	printf 'blocked\n' > "${src}/a-blocked.txt"
	printf 'later\n' > "${src}/z-later.txt"
	chmod 000 "${src}/a-blocked.txt"

	set +e
	"${BIN_PATH}" copy "${src}" "${dst}" \
		--resume \
		--resume-id "${job_id}" \
		--cache-dir "${CACHE_DIR}" \
		--resume-error-limit 0 \
		--transfers 1 \
		--checkers 1 >/dev/null 2>&1
	local rc=$?
	set -e
	[[ ${rc} -ne 0 ]] || fail "预期失败上限超限后退出"

	local status_json
	status_json="$(api_post "sync/resume/status" "{\"jobId\":\"${job_id}\"}")"
	assert_json "${status_json}" '.found == true'
	assert_json "${status_json}" '.totals.pending_failed_count == 1'
	assert_json "${status_json}" '.scan.over_error_limit == true'
	assert_json "${status_json}" '.failed | length == 1'

	chmod 644 "${src}/a-blocked.txt"
	"${BIN_PATH}" copy "${src}" "${dst}" \
		--resume \
		--resume-id "${job_id}" \
		--cache-dir "${CACHE_DIR}" \
		--resume-error-limit 10 \
		--transfers 1 \
		--checkers 1 >/dev/null

	assert_file_exists "${dst}/a-blocked.txt"
	assert_file_exists "${dst}/z-later.txt"
	status_json="$(api_post "sync/resume/status" "{\"jobId\":\"${job_id}\"}")"
	assert_json "${status_json}" '.found == false'
}

run_status_stats_fields() {
	local src="${TMP_DIR}/stats-src"
	local dst="${TMP_DIR}/stats-dst"
	local job_id="resume-local-status-stats"

	log "场景 5: status 接口统计字段校验"
	rm -rf "${src}" "${dst}"
	mkdir -p "${src}" "${dst}"
	for i in $(seq 1 6); do
		mkdir -p "${src}/dir-${i}"
		dd if=/dev/zero of="${src}/dir-${i}/file-${i}.bin" bs=128K count=1 status=none
	done

	set +e
	timeout --signal=TERM 6s \
		"${BIN_PATH}" copy "${src}" "${dst}" \
		--resume \
		--resume-id "${job_id}" \
		--cache-dir "${CACHE_DIR}" \
		--transfers 1 \
		--checkers 1 \
		--bwlimit 64k >/dev/null 2>&1
	local rc=$?
	set -e
	[[ ${rc} -ne 0 ]] || fail "预期 status 统计场景第一次 copy 被中断"

	local status_json
	status_json="$(api_post "sync/resume/status" "{\"jobId\":\"${job_id}\"}")"
	assert_json "${status_json}" '.found == true'
	assert_json_gt "${status_json}" '.totals.files'
	assert_json_gt "${status_json}" '.totals.bytes'

	"${BIN_PATH}" copy "${src}" "${dst}" \
		--resume \
		--resume-id "${job_id}" \
		--cache-dir "${CACHE_DIR}" >/dev/null

	"${BIN_PATH}" check "${src}" "${dst}" --one-way >/dev/null
	status_json="$(api_post "sync/resume/status" "{\"jobId\":\"${job_id}\"}")"
	assert_json "${status_json}" '.found == false'
}

run_multi_interrupt_resume() {
	local src="${TMP_DIR}/multi-src"
	local dst="${TMP_DIR}/multi-dst"
	local job_id="resume-local-multi-interrupt"

	log "场景 6: 多次连续中断恢复"
	make_large_tree "${src}"
	rm -rf "${dst}"
	mkdir -p "${dst}"

	for round in 1 2; do
		set +e
		timeout --signal=TERM "${LOCAL_INTERRUPT_TIMEOUT}s" \
			"${BIN_PATH}" copy "${src}" "${dst}" \
			--resume \
			--resume-id "${job_id}" \
			--cache-dir "${CACHE_DIR}" \
			--transfers 1 \
			--checkers 1 \
			--bwlimit 128k \
			--create-empty-src-dirs >/dev/null 2>&1
		local rc=$?
		set -e
		[[ ${rc} -ne 0 ]] || fail "预期第 ${round} 次 copy 被中断"
		local status_json
		status_json="$(api_post "sync/resume/status" "{\"jobId\":\"${job_id}\"}")"
		assert_json "${status_json}" '.found == true'
	done

	"${BIN_PATH}" copy "${src}" "${dst}" \
		--resume \
		--resume-id "${job_id}" \
		--cache-dir "${CACHE_DIR}" \
		--create-empty-src-dirs >/dev/null

	"${BIN_PATH}" check "${src}" "${dst}" --one-way >/dev/null
	local status_json
	status_json="$(api_post "sync/resume/status" "{\"jobId\":\"${job_id}\"}")"
	assert_json "${status_json}" '.found == false'
}

run_preseeded_destination() {
	local src="${TMP_DIR}/preseed-src"
	local dst="${TMP_DIR}/preseed-dst"
	local job_id="resume-local-preseeded"

	log "场景 7: 目标端已有部分文件"
	make_large_tree "${src}"
	rm -rf "${dst}"
	mkdir -p "${dst}/a" "${dst}/b/bb"
	cp "${src}/root.txt" "${dst}/root.txt"
	cp "${src}/a/file-1.bin" "${dst}/a/file-1.bin"
	cp "${src}/b/bb/file-1.bin" "${dst}/b/bb/file-1.bin"

	set +e
	timeout --signal=TERM "${LOCAL_INTERRUPT_TIMEOUT}s" \
		"${BIN_PATH}" copy "${src}" "${dst}" \
		--resume \
		--resume-id "${job_id}" \
		--cache-dir "${CACHE_DIR}" \
		--transfers 1 \
		--checkers 1 \
		--bwlimit 128k \
		--create-empty-src-dirs >/dev/null 2>&1
		set -e

	"${BIN_PATH}" copy "${src}" "${dst}" \
		--resume \
		--resume-id "${job_id}" \
		--cache-dir "${CACHE_DIR}" \
		--create-empty-src-dirs >/dev/null

	"${BIN_PATH}" check "${src}" "${dst}" --one-way >/dev/null
}

run_special_filenames() {
	local src="${TMP_DIR}/special-src"
	local dst="${TMP_DIR}/special-dst"
	local job_id="resume-local-special-names"

	log "场景 8: 特殊文件名"
	rm -rf "${src}" "${dst}"
	mkdir -p "${src}/空 目录" "${dst}"
	printf 'zh\n' > "${src}/中文.txt"
	printf 'space\n' > "${src}/space file.txt"
	printf 'upper\n' > "${src}/CaseFile.TXT"
	printf 'lower\n' > "${src}/casefile.txt"
	printf 'unicode-a\n' > "${src}/é.txt"
	printf 'bracket\n' > "${src}/[demo].txt"
	printf 'nested\n' > "${src}/空 目录/子 文件.txt"

	set +e
	timeout --signal=TERM "${LOCAL_INTERRUPT_TIMEOUT}s" \
		"${BIN_PATH}" copy "${src}" "${dst}" \
		--resume \
		--resume-id "${job_id}" \
		--cache-dir "${CACHE_DIR}" \
		--transfers 1 \
		--checkers 1 \
		--bwlimit 64k \
		--create-empty-src-dirs >/dev/null 2>&1
	set -e

	"${BIN_PATH}" copy "${src}" "${dst}" \
		--resume \
		--resume-id "${job_id}" \
		--cache-dir "${CACHE_DIR}" \
		--create-empty-src-dirs >/dev/null

	"${BIN_PATH}" check "${src}" "${dst}" --one-way >/dev/null
	assert_file_exists "${dst}/中文.txt"
	assert_file_exists "${dst}/space file.txt"
	assert_file_exists "${dst}/CaseFile.TXT"
	assert_file_exists "${dst}/casefile.txt"
	assert_file_exists "${dst}/é.txt"
	assert_file_exists "${dst}/[demo].txt"
	assert_file_exists "${dst}/空 目录/子 文件.txt"
}

run_many_small_files() {
	local src="${TMP_DIR}/many-src"
	local dst="${TMP_DIR}/many-dst"
	local job_id="resume-local-many-small"

	log "场景 9: 较大规模小文件"
	rm -rf "${src}" "${dst}"
	mkdir -p "${src}" "${dst}"
	for i in $(seq 1 1500); do
		printf 'file-%03d\n' "${i}" > "${src}/file-${i}.txt"
	done

	set +e
	timeout --signal=TERM 2s \
		"${BIN_PATH}" copy "${src}" "${dst}" \
		--resume \
		--resume-id "${job_id}" \
		--cache-dir "${CACHE_DIR}" \
		--transfers 1 \
		--checkers 1 \
		--bwlimit 64k >/dev/null 2>&1
		local rc=$?
	set -e
	[[ ${rc} -ne 0 ]] || fail "预期小文件场景第一次 copy 被中断"

	local status_json
	status_json="$(api_post "sync/resume/status" "{\"jobId\":\"${job_id}\"}")"
	assert_json "${status_json}" '.found == true'

	"${BIN_PATH}" copy "${src}" "${dst}" \
		--resume \
		--resume-id "${job_id}" \
		--cache-dir "${CACHE_DIR}" >/dev/null

	"${BIN_PATH}" check "${src}" "${dst}" --one-way >/dev/null
}

run_repeated_failures() {
	local src="${TMP_DIR}/repeat-fail-src"
	local dst="${TMP_DIR}/repeat-fail-dst"
	local job_id="resume-local-repeat-fail"

	log "场景 10: 失败文件反复失败"
	rm -rf "${src}" "${dst}"
	mkdir -p "${src}" "${dst}"
	printf 'bad\n' > "${src}/blocked.txt"
	chmod 000 "${src}/blocked.txt"
	local previous_failures=0

	for round in 1 2; do
		set +e
		"${BIN_PATH}" copy "${src}" "${dst}" \
			--resume \
			--resume-id "${job_id}" \
			--cache-dir "${CACHE_DIR}" \
			--resume-error-limit 10 \
			--retries 1 >/dev/null 2>&1
		local rc=$?
		set -e
		[[ ${rc} -ne 0 ]] || fail "预期第 ${round} 次失败文件场景退出"

		local status_json
		status_json="$(api_post "sync/resume/status" "{\"jobId\":\"${job_id}\"}")"
		assert_json "${status_json}" '.found == true'
		assert_json "${status_json}" '.failed | length == 1'
		local failures
		failures="$(printf '%s' "${status_json}" | jq -r '.failed[0].failures')"
		[[ "${failures}" =~ ^[0-9]+$ ]] || fail "失败次数不是数字: ${failures}"
		(( failures > previous_failures )) || fail "失败次数未递增: ${failures} <= ${previous_failures}"
		previous_failures="${failures}"
	done

	chmod 644 "${src}/blocked.txt"
	"${BIN_PATH}" copy "${src}" "${dst}" \
		--resume \
		--resume-id "${job_id}" \
		--cache-dir "${CACHE_DIR}" \
		--retries 1 >/dev/null

	assert_file_exists "${dst}/blocked.txt"
}

run_resume_id_isolation() {
	local src="${TMP_DIR}/iso-src"
	local dst="${TMP_DIR}/iso-dst"
	local job_a="resume-local-iso-a"
	local job_b="resume-local-iso-b"

	log "场景 11: resume_id 隔离"
	make_large_tree "${src}"
	rm -rf "${dst}"
	mkdir -p "${dst}"

	set +e
	timeout --signal=TERM "${LOCAL_INTERRUPT_TIMEOUT}s" \
		"${BIN_PATH}" copy "${src}" "${dst}" \
		--resume \
		--resume-id "${job_a}" \
		--cache-dir "${CACHE_DIR}" \
		--transfers 1 \
		--checkers 1 \
		--bwlimit 128k >/dev/null 2>&1
	local rc=$?
	set -e
	[[ ${rc} -ne 0 ]] || fail "预期 resume_id 隔离场景第一次 copy 被中断"

	local status_json
	status_json="$(api_post "sync/resume/status" "{\"jobId\":\"${job_a}\"}")"
	assert_json "${status_json}" '.found == true'

	status_json="$(api_post "sync/resume/status" "{\"jobId\":\"${job_b}\"}")"
	assert_json "${status_json}" '.found == false'

	"${BIN_PATH}" copy "${src}" "${dst}" \
		--resume \
		--resume-id "${job_b}" \
		--cache-dir "${CACHE_DIR}" >/dev/null

	status_json="$(api_post "sync/resume/status" "{\"jobId\":\"${job_a}\"}")"
	assert_json "${status_json}" '.found == true'

	"${BIN_PATH}" copy "${src}" "${dst}" \
		--resume \
		--resume-id "${job_a}" \
		--cache-dir "${CACHE_DIR}" >/dev/null

	"${BIN_PATH}" check "${src}" "${dst}" --one-way >/dev/null
	status_json="$(api_post "sync/resume/status" "{\"jobId\":\"${job_a}\"}")"
	assert_json "${status_json}" '.found == false'
}

run_sigkill_interrupt_resume() {
	local src="${TMP_DIR}/sigkill-src"
	local dst="${TMP_DIR}/sigkill-dst"
	local job_id="resume-local-sigkill"

	log "场景 12: SIGKILL 强制中断后恢复"
	make_large_tree "${src}"
	rm -rf "${dst}"
	mkdir -p "${dst}"

	set +e
	"${BIN_PATH}" copy "${src}" "${dst}" \
		--resume \
		--resume-id "${job_id}" \
		--cache-dir "${CACHE_DIR}" \
		--transfers 1 \
		--checkers 1 \
		--bwlimit 128k \
		--create-empty-src-dirs >/dev/null 2>&1 &
	local pid=$!
	sleep 2
	kill -9 "${pid}" >/dev/null 2>&1 || true
	wait "${pid}" >/dev/null 2>&1 || true
	set -e

	local status_json
	status_json="$(api_post "sync/resume/status" "{\"jobId\":\"${job_id}\"}")"
	assert_json "${status_json}" '.found == true'

	"${BIN_PATH}" copy "${src}" "${dst}" \
		--resume \
		--resume-id "${job_id}" \
		--cache-dir "${CACHE_DIR}" \
		--create-empty-src-dirs >/dev/null

	"${BIN_PATH}" check "${src}" "${dst}" --one-way >/dev/null
}

run_corrupt_state_recovery() {
	local src="${TMP_DIR}/corrupt-src"
	local dst="${TMP_DIR}/corrupt-dst"
	local job_id="resume-local-corrupt"
	local db_path="${CACHE_DIR}/kv/resume.bolt"

	log "场景 13: resume 状态损坏"
	rm -rf "${src}" "${dst}"
	mkdir -p "${src}" "${dst}" "$(dirname "${db_path}")"
	printf 'ok\n' > "${src}/file.txt"

	printf 'not-a-bolt-db' > "${db_path}"
	set +e
	"${BIN_PATH}" copy "${src}" "${dst}" \
		--resume \
		--resume-id "${job_id}" \
		--cache-dir "${CACHE_DIR}" >/tmp/rclone-resume-corrupt.log 2>&1
	local rc=$?
	set -e
	[[ ${rc} -ne 0 ]] || fail "预期损坏状态场景返回错误"

	rm -f "${db_path}"
	"${BIN_PATH}" copy "${src}" "${dst}" \
		--resume \
		--resume-id "${job_id}" \
		--cache-dir "${CACHE_DIR}" >/dev/null
	"${BIN_PATH}" check "${src}" "${dst}" --one-way >/dev/null
}

run_large_file_mix() {
	local src="${TMP_DIR}/large-src"
	local dst="${TMP_DIR}/large-dst"
	local job_id="resume-local-large-mix"

	log "场景 14: 大文件与混合文件集"
	rm -rf "${src}" "${dst}"
	mkdir -p "${src}" "${dst}"
	for i in $(seq 1 8); do
		mkdir -p "${src}/small-dir-${i}"
		printf 'small-%02d\n' "${i}" > "${src}/small-dir-${i}/small-${i}.txt"
	done
	dd if=/dev/zero of="${src}/z-large.bin" bs=1M count=24 status=none

	set +e
	timeout --signal=TERM 4s \
		"${BIN_PATH}" copy "${src}" "${dst}" \
		--resume \
		--resume-id "${job_id}" \
		--cache-dir "${CACHE_DIR}" \
		--transfers 1 \
		--checkers 1 \
		--bwlimit 512k >/dev/null 2>&1
	local rc=$?
	set -e
	[[ ${rc} -ne 0 ]] || fail "预期大文件混合场景第一次 copy 被中断"

	local status_json
	status_json="$(api_post "sync/resume/status" "{\"jobId\":\"${job_id}\"}")"
	assert_json "${status_json}" '.found == true'
	assert_json_gt "${status_json}" '.totals.files'

	"${BIN_PATH}" copy "${src}" "${dst}" \
		--resume \
		--resume-id "${job_id}" \
		--cache-dir "${CACHE_DIR}" >/dev/null

	"${BIN_PATH}" check "${src}" "${dst}" --one-way >/dev/null
	assert_file_exists "${dst}/z-large.bin"
}

run_s3_optional() {
	if [[ -z "${S3_REMOTE}" ]]; then
		log "场景 15: 跳过 S3 测试（未设置 RESUME_TEST_S3_REMOTE，且未检测到可用 eas-s3）"
		return
	fi

	local local_src="${TMP_DIR}/s3-upload-src"
	local local_dst="${TMP_DIR}/s3-download-dst"
	local bucket="resume-v1-$$_$(date +%s)"
	local remote_bucket="${S3_REMOTE%:}:${bucket}"
	local remote_src="${remote_bucket}/src"
	local job_id="resume-s3-interrupt"

	log "场景 15: S3 源中断后继续"
	make_large_tree "${local_src}"
	rm -rf "${local_dst}"
	mkdir -p "${local_dst}"

	rclone_cmd purge "${remote_bucket}" >/dev/null 2>&1 || true
	rclone_cmd mkdir "${remote_bucket}" >/dev/null
	rclone_cmd copy "${local_src}" "${remote_src}" --create-empty-src-dirs >/dev/null

	set +e
	timeout --signal=TERM "${S3_INTERRUPT_TIMEOUT}s" \
		"${BIN_PATH}" --config "${RCLONE_CONFIG}" copy "${remote_src}" "${local_dst}" \
		--resume \
		--resume-id "${job_id}" \
		--cache-dir "${CACHE_DIR}" \
		--transfers 1 \
		--checkers 1 \
		--bwlimit 128k \
		--create-empty-src-dirs >/dev/null 2>&1
	local rc=$?
	set -e
	[[ ${rc} -ne 0 ]] || fail "预期第一次 S3 copy 被中断"

	local status_json
	status_json="$(api_post "sync/resume/status" "{\"jobId\":\"${job_id}\"}")"
	assert_json "${status_json}" '.found == true'

	"${BIN_PATH}" --config "${RCLONE_CONFIG}" copy "${remote_src}" "${local_dst}" \
		--resume \
		--resume-id "${job_id}" \
		--cache-dir "${CACHE_DIR}" \
		--create-empty-src-dirs >/dev/null

	rclone_cmd check "${remote_src}" "${local_dst}" --one-way >/dev/null
	status_json="$(api_post "sync/resume/status" "{\"jobId\":\"${job_id}\"}")"
	assert_json "${status_json}" '.found == false'

	rclone_cmd purge "${remote_bucket}" >/dev/null 2>&1 || true
}

main() {
	need_cmd go
	need_cmd jq
	need_cmd curl
	need_cmd timeout
	need_cmd dd

	build_rclone
	setup_rclone_config
	start_rcd

	run_local_interrupt_resume
	run_failed_retry_resume
	run_clear_api
	run_error_limit_stop
	run_status_stats_fields
	run_multi_interrupt_resume
	run_preseeded_destination
	run_special_filenames
	run_many_small_files
	run_repeated_failures
	run_resume_id_isolation
	run_sigkill_interrupt_resume
	run_corrupt_state_recovery
	run_large_file_mix
	run_s3_optional

	log "全部 Resume V1 场景测试完成"
}

main "$@"
