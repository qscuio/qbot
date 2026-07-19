import assert from "node:assert/strict";
import { execFile as execFileCallback, spawn } from "node:child_process";
import {
  chmod,
  copyFile,
  mkdir,
  mkdtemp,
  readFile,
  rename,
  rm,
  stat,
  writeFile,
} from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { promisify } from "node:util";

const execFile = promisify(execFileCallback);

function deploymentStep(workflow, name) {
  const start = workflow.indexOf(`- name: ${name}`);
  const end = workflow.indexOf("\n      - name:", start + 1);
  assert.ok(start >= 0, `missing deployment step: ${name}`);
  return workflow.slice(start, end < 0 ? workflow.length : end);
}

function stepScript(step) {
  const marker = "          script: |\n";
  const start = step.indexOf(marker);
  assert.ok(start >= 0, "deployment step is missing a script block");
  return step
    .slice(start + marker.length)
    .split("\n")
    .map((line) => (line.startsWith("            ") ? line.slice(12) : line))
    .join("\n");
}

async function runRepairScenario(scenario, initialState) {
  const workflow = await readFile("../../.github/workflows/deploy.yml", "utf8");
  const script = stepScript(deploymentStep(workflow, "Start company intelligence repair"));
  const directory = await mkdtemp(join(tmpdir(), "qbot-repair-deploy-"));
  const bin = join(directory, "bin");
  const state = join(directory, "state");
  const log = join(directory, "commands.log");
  const repairScript = join(directory, "repair.sh");

  await mkdir(bin, { recursive: true });
  await writeFile(state, `${initialState}\n`);
  await writeFile(log, "");
  await writeFile(repairScript, script);
  await writeFile(
    join(bin, "sudo"),
    "#!/bin/sh\nexec \"$@\"\n",
  );
  await writeFile(
    join(bin, "systemctl"),
    `#!/bin/bash
set -eu
echo "systemctl $*" >> "$MOCK_LOG"
verb="$1"
shift

if [ "$verb" = "show" ]; then
  unit="$1"
  shift
  property=""
  for argument in "$@"; do
    case "$argument" in
      --property=*) property="\${argument#--property=}" ;;
    esac
  done
  if [ "$unit" = "qbot" ] && [ "$property" = "User" ]; then
    echo "ubuntu"
    exit 0
  fi
  phase="$(tr -d '\\n' < "$MOCK_STATE")"
  case "$phase:$property" in
    running:LoadState|stale:LoadState|fast-success:LoadState|fast-failure:LoadState|activating-failed:LoadState|activating-running:LoadState) echo loaded ;;
    gone:LoadState|missing:LoadState) echo not-found ;;
    running:ActiveState|fast-success:ActiveState) echo active ;;
    activating-failed:ActiveState|activating-running:ActiveState) echo activating ;;
    stale:ActiveState|fast-failure:ActiveState) echo failed ;;
    gone:ActiveState|missing:ActiveState) echo inactive ;;
    running:SubState) echo running ;;
    activating-failed:SubState|activating-running:SubState) echo start ;;
    fast-success:SubState) echo exited ;;
    stale:SubState|fast-failure:SubState) echo failed ;;
    gone:SubState|missing:SubState) echo dead ;;
    running:Result|fast-success:Result|gone:Result|missing:Result|activating-failed:Result|activating-running:Result) echo success ;;
    stale:Result|fast-failure:Result) echo exit-code ;;
    running:ExecMainStatus|fast-success:ExecMainStatus|gone:ExecMainStatus|missing:ExecMainStatus|activating-failed:ExecMainStatus|activating-running:ExecMainStatus) echo 0 ;;
    stale:ExecMainStatus|fast-failure:ExecMainStatus) echo 17 ;;
    *) echo "unexpected show request: $phase:$property" >&2; exit 64 ;;
  esac
  if [ "$property" = "ExecMainStatus" ]; then
    case "$phase" in
      activating-failed) echo fast-failure > "$MOCK_STATE" ;;
      activating-running) echo running > "$MOCK_STATE" ;;
    esac
  fi
  exit 0
fi

case "$verb" in
  stop)
    echo gone > "$MOCK_STATE"
    ;;
  reset-failed)
    ;;
  status)
    echo "mock status" >&2
    ;;
  *)
    echo "unexpected systemctl command: $verb $*" >&2
    exit 64
    ;;
esac
`,
  );
  await writeFile(
    join(bin, "systemd-run"),
    `#!/bin/bash
set -eu
echo "systemd-run $*" >> "$MOCK_LOG"
case "$MOCK_SCENARIO" in
  launch-failure)
    echo "mock launch rejected" >&2
    exit 23
    ;;
  fast-success) echo fast-success > "$MOCK_STATE" ;;
  fast-failure) echo fast-failure > "$MOCK_STATE" ;;
  activating-failure) echo activating-failed > "$MOCK_STATE" ;;
  activating-running) echo activating-running > "$MOCK_STATE" ;;
  stale-relaunch) echo running > "$MOCK_STATE" ;;
  *) echo "unexpected launch in scenario $MOCK_SCENARIO" >&2; exit 64 ;;
esac
`,
  );
  await writeFile(
    join(bin, "journalctl"),
    "#!/bin/sh\necho \"journalctl $*\" >> \"$MOCK_LOG\"\n",
  );
  await Promise.all(
    ["sudo", "systemctl", "systemd-run", "journalctl"].map((file) =>
      chmod(join(bin, file), 0o755),
    ),
  );

  let result;
  try {
    const output = await execFile("bash", [repairScript], {
      env: {
        ...process.env,
        PATH: `${bin}:${process.env.PATH}`,
        MOCK_LOG: log,
        MOCK_SCENARIO: scenario,
        MOCK_STATE: state,
      },
    });
    result = { status: 0, ...output };
  } catch (error) {
    result = {
      status: error.code,
      stdout: error.stdout ?? "",
      stderr: error.stderr ?? "",
    };
  }
  result.log = await readFile(log, "utf8");
  await rm(directory, { recursive: true, force: true });
  return result;
}

test("dashboard redirects never expose the private origin port", async () => {
  const config = await readFile("../../deploy/qbot-dashboard.nginx.conf", "utf8");

  assert.doesNotMatch(config, /return 302 \/dashboard\//);
  assert.equal(
    config.match(/return 302 https:\/\/\$host\/dashboard\//g)?.length,
    2,
  );
});

test("dashboard HTML is fresh while versioned assets can be cached", async () => {
  const config = await readFile("../../deploy/qbot-dashboard.nginx.conf", "utf8");
  const html = await readFile("index.html", "utf8");
  const app = await readFile("js/app.js", "utf8");
  const versions = [
    ...html.matchAll(/(?:dashboard\.css|lightweight-charts\.js|app\.js)\?v=([\w.-]+)/g),
    ...app.matchAll(/(?:api\.js|chart\.js|company-panels\.js|state\.js)\?v=([\w.-]+)/g),
  ].map((match) => match[1]);

  assert.equal(versions.length, 7);
  assert.equal(new Set(versions).size, 1);
  assert.equal(versions[0], "20260719.7");
  assert.match(config, /set \$dashboard_cache_control "no-store";/);
  assert.match(
    config,
    /set \$dashboard_cache_control "public, max-age=31536000, immutable";/,
  );
  assert.ok(
    config.includes(
      'if ($request_uri ~* "^/dashboard/.+\\.(?:css|js)\\?v=[A-Za-z0-9._-]+$") {',
    ),
  );
  assert.doesNotMatch(config, /if \(\$uri /);
  assert.match(config, /add_header Cache-Control \$dashboard_cache_control always;/);
});

test("test workflow provisions the dependencies required by SQLx tests", async () => {
  const workflow = await readFile("../../.github/workflows/test.yml", "utf8");

  assert.match(workflow, /services:[\s\S]*?postgres:[\s\S]*?image:\s*postgres:16/);
  assert.match(workflow, /services:[\s\S]*?redis:[\s\S]*?image:\s*redis:7-alpine/);
  assert.match(
    workflow,
    /DATABASE_URL:\s*postgresql:\/\/qbot:qbot@127\.0\.0\.1:5432\/qbot/,
  );
  assert.match(workflow, /REDIS_URL:\s*redis:\/\/127\.0\.0\.1:6379/);
  assert.match(workflow, /run:\s*cargo test --locked/);
  assert.doesNotMatch(workflow, /Tests \(no DB needed\)/);
});

test("deployment runs the resumable daily-bar repair after the service is healthy", async () => {
  const workflow = await readFile("../../.github/workflows/deploy.yml", "utf8");
  const healthCheck = workflow.indexOf("- name: Health check");
  const repair = workflow.indexOf("- name: Repair persisted OHLCV data");

  assert.ok(healthCheck >= 0);
  assert.ok(repair > healthCheck);
  assert.match(workflow, /command_timeout:\s*6h/);
  assert.match(workflow, /--repair-daily-bars/);
});

test("deployment atomically replaces a binary without disturbing its old running inode", async () => {
  const workflow = await readFile("../../.github/workflows/deploy.yml", "utf8");
  const deploy = deploymentStep(workflow, "Deploy to VPS");

  assert.match(workflow, /concurrency:[\s\S]*?cancel-in-progress:\s*false/);
  assert.match(deploy, /binary_tmp=/);
  assert.match(deploy, /install -m 0755 target\/release\/qbot "\$binary_tmp"/);
  assert.match(deploy, /mv -f "\$binary_tmp" \/opt\/qbot\/qbot/);
  assert.doesNotMatch(deploy, /cp target\/release\/qbot \/opt\/qbot\/qbot/);

  const directory = await mkdtemp(join(tmpdir(), "qbot-atomic-install-"));
  const destination = join(directory, "qbot");
  const replacement = join(directory, ".qbot-new");
  let oldProcess;
  try {
    await copyFile("/bin/sleep", destination);
    await chmod(destination, 0o755);
    const oldInode = (await stat(destination)).ino;
    oldProcess = spawn(destination, ["30"], { stdio: "ignore" });
    await new Promise((resolve, reject) => {
      oldProcess.once("spawn", resolve);
      oldProcess.once("error", reject);
    });

    await copyFile("/bin/true", replacement);
    await chmod(replacement, 0o755);
    await rename(replacement, destination);

    assert.notEqual((await stat(destination)).ino, oldInode);
    assert.equal(oldProcess.exitCode, null);
    await execFile(destination);
  } finally {
    if (oldProcess?.exitCode === null) {
      oldProcess.kill("SIGKILL");
      await new Promise((resolve) => oldProcess.once("exit", resolve));
    }
    await rm(directory, { recursive: true, force: true });
  }
});

test("deployment launches one detached resumable company repair after health", async () => {
  const workflow = await readFile("../../.github/workflows/deploy.yml", "utf8");
  const main = await readFile("../../src/main.rs", "utf8");
  const healthCheck = workflow.indexOf("- name: Health check");
  const repairStart = workflow.indexOf("- name: Start company intelligence repair");
  const nextStep = workflow.indexOf("\n      - name:", repairStart + 1);
  const repair = workflow.slice(repairStart, nextStep);

  assert.ok(healthCheck >= 0);
  assert.ok(repairStart > healthCheck);
  assert.match(repair, /set -e/);
  assert.match(repair, /systemctl show qbot --property=User --value/);
  assert.match(repair, /unit_property\(\)/);
  assert.match(repair, /unit_property LoadState/);
  assert.match(repair, /systemctl stop "\$unit"/);
  assert.match(repair, /systemctl reset-failed "\$unit"/);
  assert.match(repair, /systemd-run --no-block/);
  assert.match(repair, /--property=RemainAfterExit=yes/);
  assert.doesNotMatch(repair, /--collect/);
  assert.match(repair, /--unit="\$unit"/);
  assert.match(repair, /unit="qbot-company-intelligence-repair"/);
  assert.match(repair, /--property=Type=exec/);
  assert.match(
    repair,
    /--description="QBot resumable company intelligence benchmark and chip backfill"/,
  );
  assert.match(repair, /--property=EnvironmentFile=\/opt\/qbot\/\.env/);
  assert.match(repair, /--property="User=\$service_user"/);
  assert.match(repair, /--property=UMask=0077/);
  assert.match(repair, /--property=StandardOutput=journal/);
  assert.match(repair, /--property=StandardError=journal/);
  assert.match(repair, /--working-directory=\/opt\/qbot/);
  assert.match(repair, /\/opt\/qbot\/qbot --repair-company-intelligence/);
  assert.match(repair, /ActiveState/);
  assert.match(repair, /SubState/);
  assert.match(repair, /ExecMainStatus/);
  assert.match(repair, /active:running|activating:/);
  assert.match(repair, /active:exited:success:0/);
  assert.match(repair, /journalctl -u "\$unit"/);
  assert.doesNotMatch(repair, /--wait/);
  assert.doesNotMatch(repair, /github\.run_(?:id|attempt)/);
  assert.match(
    main,
    /run_company_intelligence_repair\([\s\S]*?run_chip_benchmark\(\)[\s\S]*?backfill_chips\(\)/,
  );
  assert.match(main, /chip_benchmark:/);
  assert.match(main, /chip_backfill:/);
});

test("an already-running company repair is skipped without a duplicate launch", async () => {
  const result = await runRepairScenario("running-skip", "running");

  assert.equal(result.status, 0, result.stderr);
  assert.doesNotMatch(result.log, /systemd-run/);
  assert.match(result.stdout, /already running/);
});

test("a fast successful company repair is observed and cleaned up", async () => {
  const result = await runRepairScenario("fast-success", "missing");

  assert.equal(result.status, 0, result.stderr);
  assert.match(result.log, /systemd-run --no-block/);
  assert.match(result.log, /systemctl stop qbot-company-intelligence-repair/);
  assert.match(result.stdout, /completed successfully/);
});

test("a rejected company repair launch fails deployment visibly", async () => {
  const result = await runRepairScenario("launch-failure", "missing");

  assert.notEqual(result.status, 0);
  assert.match(result.stderr, /mock launch rejected/);
});

test("a stale company repair is cleaned before one replacement is launched", async () => {
  const result = await runRepairScenario("stale-relaunch", "stale");

  assert.equal(result.status, 0, result.stderr);
  const cleanup = result.log.indexOf("systemctl stop qbot-company-intelligence-repair");
  const launch = result.log.indexOf("systemd-run --no-block");
  assert.ok(cleanup >= 0);
  assert.ok(launch > cleanup);
  assert.equal(result.log.match(/systemd-run/g)?.length, 1);
});

test("a fast failed company repair exposes journal diagnostics and fails", async () => {
  const result = await runRepairScenario("fast-failure", "missing");

  assert.notEqual(result.status, 0);
  assert.match(result.log, /journalctl -u qbot-company-intelligence-repair/);
});

test("an activating company repair is polled until its later failure is visible", async () => {
  const result = await runRepairScenario("activating-failure", "missing");

  assert.notEqual(result.status, 0);
  assert.match(result.log, /journalctl -u qbot-company-intelligence-repair/);
  assert.ok(
    (result.log.match(/--property=ActiveState/g)?.length ?? 0) >= 2,
    result.log,
  );
});

test("an activating company repair is polled until it is actually running", async () => {
  const result = await runRepairScenario("activating-running", "missing");

  assert.equal(result.status, 0, result.stderr);
  assert.match(result.stdout, /state: active\/running/);
  assert.ok(
    (result.log.match(/--property=ActiveState/g)?.length ?? 0) >= 2,
    result.log,
  );
});
