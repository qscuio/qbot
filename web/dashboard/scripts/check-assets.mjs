import { access, readFile } from "node:fs/promises";

const required = [
  "index.html",
  "css/dashboard.css",
  "js/app.js",
  "js/api.js",
  "js/company-panels.js",
  "js/state.js",
  "js/chart.js",
  "vendor/lightweight-charts.js",
];

await Promise.all(required.map((path) => access(new URL(`../${path}`, import.meta.url))));
const html = await readFile(new URL("../index.html", import.meta.url), "utf8");
for (const reference of ["/dashboard/css/dashboard.css", "/dashboard/js/app.js", "/dashboard/vendor/lightweight-charts.js"]) {
  if (!html.includes(reference)) throw new Error(`index.html is missing ${reference}`);
}
console.log(`Dashboard asset check passed (${required.length} files).`);
