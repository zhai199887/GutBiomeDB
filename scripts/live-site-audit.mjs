#!/usr/bin/env node

import fs from "node:fs";
import path from "node:path";

const DEBUG_PORT = Number(process.env.CDP_DEBUG_PORT || 9222);
const BASE_URL = process.env.AUDIT_BASE_URL || "https://compendiumwebsite.vercel.app";
const OUTPUT_ROOT = process.env.AUDIT_OUTPUT_DIR || `./live_site_audit_${new Date().toISOString().slice(0, 10).replace(/-/g, "")}`;
const VIEWPORT_WIDTH = Number(process.env.AUDIT_VIEWPORT_WIDTH || 1920);
const VIEWPORT_HEIGHT = Number(process.env.AUDIT_VIEWPORT_HEIGHT || 1080);
const LOCALE = process.env.AUDIT_LOCALE || "zh";
const SCENARIO = process.env.AUDIT_SCENARIO || "home";

fs.mkdirSync(OUTPUT_ROOT, { recursive: true });

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function slugify(input) {
  return input.replace(/[^a-zA-Z0-9_-]+/g, "-").replace(/^-+|-+$/g, "");
}

async function httpJson(url, init) {
  const response = await fetch(url, init);
  if (!response.ok) {
    throw new Error(`HTTP ${response.status} for ${url}`);
  }
  return response.json();
}

async function createTarget(url) {
  return httpJson(`http://127.0.0.1:${DEBUG_PORT}/json/new?${encodeURIComponent(url)}`, { method: "PUT" });
}

class CdpPage {
  constructor(target) {
    this.target = target;
    this.ws = null;
    this.nextId = 0;
    this.pending = new Map();
    this.consoleMessages = [];
    this.pageErrors = [];
  }

  async connect() {
    this.ws = new WebSocket(this.target.webSocketDebuggerUrl);
    await new Promise((resolve, reject) => {
      this.ws.onopen = resolve;
      this.ws.onerror = (event) => reject(new Error(`WebSocket connect failed: ${event.message || event.type}`));
    });
    this.ws.onmessage = (event) => {
      const message = JSON.parse(event.data.toString());
      if (message.id && this.pending.has(message.id)) {
        const { resolve, reject, timer } = this.pending.get(message.id);
        clearTimeout(timer);
        this.pending.delete(message.id);
        if (message.error) reject(new Error(message.error.message || JSON.stringify(message.error)));
        else resolve(message.result);
        return;
      }
      this.handleEvent(message);
    };

    await this.send("Page.enable");
    await this.send("Runtime.enable");
    await this.send("Log.enable");
    await this.send("DOM.enable");
    await this.send("Network.enable");
    await this.send("Emulation.setDeviceMetricsOverride", {
      width: VIEWPORT_WIDTH,
      height: VIEWPORT_HEIGHT,
      deviceScaleFactor: 1,
      mobile: false,
      screenWidth: VIEWPORT_WIDTH,
      screenHeight: VIEWPORT_HEIGHT,
    });
  }

  handleEvent(message) {
    if (message.method === "Runtime.consoleAPICalled") {
      const text = (message.params?.args || [])
        .map((arg) => arg.value ?? arg.description ?? "")
        .join(" ");
      if (text) this.consoleMessages.push(text);
    }
    if (message.method === "Log.entryAdded") {
      const entry = message.params?.entry;
      if (entry?.text) this.consoleMessages.push(`${entry.level || "log"}: ${entry.text}`);
    }
    if (message.method === "Runtime.exceptionThrown") {
      const description = message.params?.exceptionDetails?.text
        || message.params?.exceptionDetails?.exception?.description
        || "Unknown exception";
      this.pageErrors.push(description);
    }
  }

  send(method, params = {}) {
    const id = ++this.nextId;
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        this.pending.delete(id);
        reject(new Error(`Timeout waiting for ${method}`));
      }, 30000);
      this.pending.set(id, { resolve, reject, timer });
      this.ws.send(JSON.stringify({ id, method, params }));
    });
  }

  async waitForReady(timeoutMs = 15000) {
    const started = Date.now();
    while (Date.now() - started < timeoutMs) {
      try {
        const result = await this.evaluate("document.readyState");
        if (result === "complete") {
          await sleep(600);
          return;
        }
      } catch {
        // ignore transient eval failures during navigation
      }
      await sleep(300);
    }
    throw new Error("Timed out waiting for readyState=complete");
  }

  async navigate(url) {
    await this.send("Page.navigate", { url });
    await this.waitForReady();
  }

  async evaluate(expression) {
    const result = await this.send("Runtime.evaluate", {
      expression,
      returnByValue: true,
      awaitPromise: true,
    });
    return result?.result?.value;
  }

  async evaluateRaw(expression) {
    return this.send("Runtime.evaluate", {
      expression,
      returnByValue: true,
      awaitPromise: true,
    });
  }

  async clickByText(text) {
    const escaped = JSON.stringify(text);
    return this.evaluate(`
      (() => {
        const normalize = (value) => value.replace(/\\s+/g, " ").trim();
        const wanted = normalize(${escaped});
        const elements = [...document.querySelectorAll('button,a,[role="button"],label,span,div')];
        const target = elements.find((node) => normalize(node.textContent || "") === wanted);
        if (!target) return false;
        target.scrollIntoView({ block: 'center', inline: 'center' });
        target.click();
        return true;
      })()
    `);
  }

  async clickSelector(selector) {
    const escaped = JSON.stringify(selector);
    return this.evaluate(`
      (() => {
        const node = document.querySelector(${escaped});
        if (!node) return false;
        node.scrollIntoView({ block: 'center', inline: 'center' });
        node.click();
        return true;
      })()
    `);
  }

  async setLocale(locale) {
    const escaped = JSON.stringify(locale);
    await this.evaluate(`
      (() => {
        localStorage.setItem("gut-atlas-locale", ${escaped});
        return true;
      })()
    `);
    await this.evaluate("window.location.reload()");
    await this.waitForReady();
  }

  async screenshot(filePath, { fullPage = true } = {}) {
    const makeViewportClip = async () => this.evaluate(`
      (() => ({
        x: 0,
        y: 0,
        width: window.innerWidth,
        height: window.innerHeight
      }))()
    `);

    const attempt = async (mode) => {
      const clip = mode === "viewport" ? await makeViewportClip() : null;
      return this.send("Page.captureScreenshot", {
        format: "png",
        fromSurface: true,
        captureBeyondViewport: mode === "full",
        ...(clip ? { clip: { ...clip, scale: 1 } } : {}),
      });
    };

    let result;
    try {
      result = await attempt(fullPage ? "full" : "viewport");
    } catch (error) {
      if (!fullPage) throw error;
      result = await attempt("viewport");
    }
    fs.writeFileSync(filePath, Buffer.from(result.data, "base64"));
  }

  async close() {
    try {
      await fetch(`http://127.0.0.1:${DEBUG_PORT}/json/close/${this.target.id}`);
    } catch {
      // ignore close failures
    }
    try {
      this.ws?.close();
    } catch {
      // ignore
    }
  }
}

async function openAuditPage(route) {
  const target = await createTarget(`${BASE_URL}${route}`);
  const page = new CdpPage(target);
  await page.connect();
  await page.waitForReady();
  await page.setLocale(LOCALE);
  await page.navigate(`${BASE_URL}${route}`);
  return page;
}

async function captureHome(page, outputDir) {
  const screenshot = path.join(outputDir, `home_${LOCALE}_${VIEWPORT_WIDTH}.png`);
  await page.screenshot(screenshot);
  const metrics = await page.evaluate(`
    (() => {
      const hero = document.querySelector('header');
      const overview = document.querySelector('section');
      const text = document.body.innerText;
      return {
        title: document.title,
        heroWidth: hero ? Math.round(hero.getBoundingClientRect().width) : null,
        bodyWidth: document.documentElement.clientWidth,
        hasForbiddenQueueText: text.includes('从队列级视角进入') || text.includes('Browse cohort composition'),
        hasForbiddenAnalysisText: text.includes('分析方法留在主视野') || text.includes('Move from descriptive browsing'),
        hasForbiddenReuseText: text.includes('复用边界明确可见') || text.includes('citation and licensing boundaries'),
        hasSchoolWord: text.includes('School') || text.includes('学校'),
        hasDuplicateTopLinks: ['部分探索', '菌属检索', '研究项目', '差异分析'].every((label) => text.includes(label)),
        has226: text.includes('226') && (text.includes('条件类别') || text.includes('condition categories')),
        overviewWidth: overview ? Math.round(overview.getBoundingClientRect().width) : null,
      };
    })()
  `);
  return { screenshot, metrics };
}

async function captureApiDocs(page, outputDir) {
  const screenshot = path.join(outputDir, `api_docs_${LOCALE}_${VIEWPORT_WIDTH}.png`);
  await page.screenshot(screenshot);
  const flags = await page.evaluate(`
    (() => {
      const text = document.body.innerText;
      return {
        title: document.title,
        hasOpsWarning: text.includes('公网 API 地址属于运维入口') || text.includes('公网 API 适合演示'),
        hasSwagger: text.includes('Swagger UI') || text.includes('OpenAPI'),
        bodyTextSample: text.slice(0, 500),
      };
    })()
  `);
  return { screenshot, flags };
}

async function captureLifecycle(page, outputDir) {
  const screenshot = path.join(outputDir, `lifecycle_${LOCALE}_${VIEWPORT_WIDTH}.png`);
  await page.screenshot(screenshot);
  const options = await page.evaluate(`
    (() => {
      const selects = [...document.querySelectorAll('select')];
      const countrySelect = selects.find((node) => [...node.options].some((opt) => opt.textContent.includes('AO') || opt.textContent.includes('MT')));
      if (!countrySelect) return { ao: null, mt: null };
      const ao = [...countrySelect.options].find((opt) => opt.value === 'AO' || opt.textContent.includes('AO'));
      const mt = [...countrySelect.options].find((opt) => opt.value === 'MT' || opt.textContent.includes('MT'));
      return {
        ao: ao ? ao.textContent.trim() : null,
        mt: mt ? mt.textContent.trim() : null,
      };
    })()
  `);
  return { screenshot, options };
}

async function captureNetwork(page, outputDir) {
  const screenshot = path.join(outputDir, `network_${LOCALE}_${VIEWPORT_WIDTH}.png`);
  await page.screenshot(screenshot);
  const flags = await page.evaluate(`
    (() => {
      const text = document.body.innerText;
      return {
        hasCompetitorName: text.includes('GMrepo') || text.includes('ResMicroDb'),
        fontTiny: [...document.querySelectorAll('svg text')].some((node) => parseFloat(getComputedStyle(node).fontSize) <= 10),
        title: document.title,
      };
    })()
  `);
  return { screenshot, flags };
}

async function captureStudies(page, outputDir) {
  const screenshot = path.join(outputDir, `studies_${LOCALE}_${VIEWPORT_WIDTH}.png`);
  await page.screenshot(screenshot);
  const metrics = await page.evaluate(`
    (() => {
      const table = document.querySelector('table');
      return {
        hasTable: Boolean(table),
        tableWidth: table ? Math.round(table.getBoundingClientRect().width) : null,
        pageWidth: document.documentElement.clientWidth,
      };
    })()
  `);
  return { screenshot, metrics };
}

async function captureDisease(page, outputDir) {
  const screenshot = path.join(outputDir, `disease_${LOCALE}_${VIEWPORT_WIDTH}.png`);
  await page.screenshot(screenshot);
  const metrics = await page.evaluate(`
    (() => {
      const listButtons = [...document.querySelectorAll('aside button')];
      const labels = listButtons.slice(0, 80).map((node) => node.textContent.replace(/\\s+/g, ' ').trim());
      return {
        visibleCount: listButtons.length,
        longestLabel: labels.sort((a, b) => b.length - a.length)[0] || '',
        sampleLabels: labels.slice(0, 20),
      };
    })()
  `);
  return { screenshot, metrics };
}

async function captureSearch(page, outputDir) {
  const query = "Faecalibacterium";
  await page.evaluate(`
    (() => {
      const input = document.querySelector('input[type="text"]');
      if (!input) return false;
      input.value = ${JSON.stringify(query)};
      input.dispatchEvent(new Event('input', { bubbles: true }));
      input.dispatchEvent(new Event('change', { bubbles: true }));
      return true;
    })()
  `);
  await sleep(300);
  await page.clickByText(LOCALE === "zh" ? "搜索" : "Search");
  await sleep(2500);
  const screenshot = path.join(outputDir, `search_${LOCALE}_${VIEWPORT_WIDTH}.png`);
  await page.screenshot(screenshot);
  const metrics = await page.evaluate(`
    (() => {
      const text = document.body.innerText;
      return {
        hasGenus: text.includes(${JSON.stringify(query)}),
        hasZeroDiseaseBars: text.includes('0.000%') || text.includes('0.00%'),
        title: document.title,
      };
    })()
  `);
  return { screenshot, metrics };
}

async function captureSpecies(page, outputDir) {
  await sleep(2500);
  const profileShot = path.join(outputDir, `species_profile_${LOCALE}_${VIEWPORT_WIDTH}.png`);
  await page.screenshot(profileShot);
  await page.clickByText(LOCALE === "zh" ? "共现" : "Co-occurrence");
  await sleep(2000);
  const cooccurrenceShot = path.join(outputDir, `species_cooccurrence_${LOCALE}_${VIEWPORT_WIDTH}.png`);
  await page.screenshot(cooccurrenceShot);
  const metrics = await page.evaluate(`
    (() => {
      const pageWidth = document.documentElement.clientWidth;
      const tables = [...document.querySelectorAll('table')];
      const table = tables[0];
      return {
        pageWidth,
        tableWidth: table ? Math.round(table.getBoundingClientRect().width) : null,
        tinyTextCount: [...document.querySelectorAll('svg text')].filter((node) => parseFloat(getComputedStyle(node).fontSize) <= 10).length,
        hasHorizontalScroll: document.documentElement.scrollWidth > document.documentElement.clientWidth + 10,
      };
    })()
  `);
  return { screenshots: [profileShot, cooccurrenceShot], metrics };
}

async function captureCompare(page, outputDir) {
  await page.evaluate(`
    (() => {
      const rows = [...document.querySelectorAll('div[class*="fieldRow"]')];
      for (const row of rows) {
        const label = row.querySelector('label')?.textContent?.trim();
        const select = row.querySelector('select');
        if (!label || !select) continue;
        if (/Sex|性别/.test(label)) {
          const value = row.closest('div[class*="groupPanel"]')?.innerText.includes('Group A') || row.closest('div[class*="groupPanel"]')?.innerText.includes('组 A')
            ? 'female'
            : 'male';
          select.value = value;
          select.dispatchEvent(new Event('change', { bubbles: true }));
        }
      }
      return true;
    })()
  `);
  await sleep(800);
  await page.clickByText(LOCALE === "zh" ? "运行分析" : "Run Analysis");
  await sleep(5000);
  const baseShot = path.join(outputDir, `compare_bar_${LOCALE}_${VIEWPORT_WIDTH}.png`);
  await page.screenshot(baseShot);
  await page.clickByText(LOCALE === "zh" ? "Alpha Diversity" : "Alpha Diversity");
  await sleep(1800);
  const alphaShot = path.join(outputDir, `compare_alpha_${LOCALE}_${VIEWPORT_WIDTH}.png`);
  await page.screenshot(alphaShot);

  await page.evaluate(`
    (() => {
      localStorage.setItem('crossStudyPreselect', JSON.stringify({
        disease: 'UC',
        projectIds: ['PRJNA237362', 'PRJNA398187', 'PRJNA431126', 'PRJNA475599']
      }));
      return true;
    })()
  `);
  await page.navigate(`${BASE_URL}/compare?tab=crossstudy`);
  await sleep(2000);
  await page.clickByText(LOCALE === "zh" ? "运行元分析" : "Run Meta-Analysis");
  await sleep(6000);
  await page.clickByText("Heatmap");
  await sleep(1200);
  const heatmapShot = path.join(outputDir, `compare_crossstudy_heatmap_${LOCALE}_${VIEWPORT_WIDTH}.png`);
  await page.screenshot(heatmapShot);

  const metrics = await page.evaluate(`
    (() => {
      const text = document.body.innerText;
      const svgTexts = [...document.querySelectorAll('svg text')].map((node) => ({
        text: node.textContent,
        fontSize: getComputedStyle(node).fontSize,
        y: node.getAttribute('y')
      }));
      return {
        hasUnexpectedError: text.includes('出现了一个问题') || text.includes('Something went wrong'),
        hasAlphaTab: text.includes('Alpha Diversity'),
        hasLegend: text.includes('Control-enriched') || text.includes('Disease-enriched'),
        tinyTextCount: svgTexts.filter((item) => parseFloat(item.fontSize) <= 10).length,
      };
    })()
  `);
  return { screenshots: [baseShot, alphaShot, heatmapShot], metrics };
}

const SCENARIOS = {
  home: { route: "/", run: captureHome },
  "api-docs": { route: "/api-docs", run: captureApiDocs },
  lifecycle: { route: "/lifecycle", run: captureLifecycle },
  network: { route: "/network", run: captureNetwork },
  studies: { route: "/studies", run: captureStudies },
  disease: { route: "/disease", run: captureDisease },
  search: { route: "/search", run: captureSearch },
  species: { route: "/species/Faecalibacterium", run: captureSpecies },
  compare: { route: "/compare", run: captureCompare },
};

async function main() {
  const scenario = SCENARIOS[SCENARIO];
  if (!scenario) {
    throw new Error(`Unknown scenario "${SCENARIO}"`);
  }
  const outputDir = path.join(OUTPUT_ROOT, `${slugify(SCENARIO)}_${LOCALE}_${VIEWPORT_WIDTH}`);
  fs.mkdirSync(outputDir, { recursive: true });
  const page = await openAuditPage(scenario.route);
  try {
    const result = await scenario.run(page, outputDir);
    const payload = {
      scenario: SCENARIO,
      route: scenario.route,
      locale: LOCALE,
      viewport: { width: VIEWPORT_WIDTH, height: VIEWPORT_HEIGHT },
      consoleMessages: page.consoleMessages,
      pageErrors: page.pageErrors,
      result,
      generatedAt: new Date().toISOString(),
      outputDir,
    };
    const reportPath = path.join(outputDir, `${slugify(SCENARIO)}_${LOCALE}_${VIEWPORT_WIDTH}.json`);
    fs.writeFileSync(reportPath, JSON.stringify(payload, null, 2));
    console.log(JSON.stringify({ ok: true, reportPath, outputDir }, null, 2));
  } finally {
    await page.close();
  }
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
