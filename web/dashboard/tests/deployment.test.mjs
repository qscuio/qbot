import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";

test("dashboard redirects never expose the private origin port", async () => {
  const config = await readFile("../../deploy/qbot-dashboard.nginx.conf", "utf8");

  assert.doesNotMatch(config, /return 302 \/dashboard\//);
  assert.equal(
    config.match(/return 302 https:\/\/\$host\/dashboard\//g)?.length,
    2,
  );
});
