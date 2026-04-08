/**
 * Utility functions for exporting SVG charts as SVG or PNG files.
 * The export path must inline computed styles so downloaded files preserve
 * theme colors, fonts, and stroke settings outside the live DOM.
 */

interface Size {
  width: number;
  height: number;
}

const parseViewBox = (svgElement: SVGSVGElement): Size => {
  const viewBox = svgElement.getAttribute("viewBox");
  if (viewBox) {
    const parts = viewBox.split(/[\s,]+/).map(Number);
    const width = parts[2] ?? Number.NaN;
    const height = parts[3] ?? Number.NaN;
    if (Number.isFinite(width) && Number.isFinite(height)) {
      return { width, height };
    }
  }
  return {
    width: svgElement.clientWidth || 800,
    height: svgElement.clientHeight || 600,
  };
};

const EXPORT_TEXT_COLOR = "#1f2937";
const DOM_EXPORT_BACKGROUND = "#0b1220";

interface InlineStyleOptions {
  forceSvgTextColor?: string;
}

interface DomExportOptions {
  background?: string;
  padding?: number;
  scale?: number;
}

const exportErrorMessage = () => (
  navigator.language.toLowerCase().startsWith("zh")
    ? "PNG 导出失败，请先使用 SVG 导出。"
    : "PNG export failed. Please use SVG export first."
);

const inlineComputedStyles = (sourceRoot: Element, targetRoot: Element, options: InlineStyleOptions = {}) => {
  const sourceNodes = [sourceRoot, ...Array.from(sourceRoot.querySelectorAll("*"))];
  const targetNodes = [targetRoot, ...Array.from(targetRoot.querySelectorAll("*"))];

  sourceNodes.forEach((sourceNode, index) => {
    const targetNode = targetNodes[index];
    if (!targetNode) return;
    const computed = window.getComputedStyle(sourceNode);
    let styleText = "";
    for (let i = 0; i < computed.length; i += 1) {
      const name = computed.item(i);
      const value = computed.getPropertyValue(name);
      if (!name || !value) continue;
      styleText += `${name}:${value};`;
    }
    if (sourceNode instanceof SVGTextElement && options.forceSvgTextColor) {
      styleText += `fill:${options.forceSvgTextColor};color:${options.forceSvgTextColor};`;
    }
    if (sourceNode instanceof HTMLInputElement || sourceNode instanceof HTMLTextAreaElement) {
      targetNode.setAttribute("value", sourceNode.value);
    }
    targetNode.setAttribute("style", styleText);
  });
};

const svgMarkupToDataUrl = (svgMarkup: string) => {
  const encoded = window.btoa(unescape(encodeURIComponent(svgMarkup)));
  return `data:image/svg+xml;base64,${encoded}`;
};

const serializeSvgNode = (
  sourceSvg: SVGSVGElement,
  targetSvg: SVGSVGElement,
  options: { backgroundFill?: string | null; watermark?: boolean; forceTextColor?: string | null } = {},
) => {
  const clone = targetSvg.cloneNode(true) as SVGSVGElement;
  const { width, height } = parseViewBox(sourceSvg);

  clone.setAttribute("xmlns", "http://www.w3.org/2000/svg");
  clone.setAttribute("width", String(width));
  clone.setAttribute("height", String(height));
  if (!clone.getAttribute("viewBox")) {
    clone.setAttribute("viewBox", `0 0 ${width} ${height}`);
  }

  if (options.backgroundFill) {
    const background = document.createElementNS("http://www.w3.org/2000/svg", "rect");
    background.setAttribute("width", "100%");
    background.setAttribute("height", "100%");
    background.setAttribute("fill", options.backgroundFill);
    clone.insertBefore(background, clone.firstChild);
  }

  if (options.watermark) {
    const watermark = document.createElementNS("http://www.w3.org/2000/svg", "text");
    watermark.setAttribute("x", String(width - 10));
    watermark.setAttribute("y", String(height - 8));
    watermark.setAttribute("text-anchor", "end");
    watermark.setAttribute("font-size", "10");
    watermark.setAttribute("fill", "#9ca3af");
    watermark.setAttribute("font-family", "Arial, sans-serif");
    watermark.textContent = "Gut Microbiome Atlas";
    clone.appendChild(watermark);
  }

  if (options.forceTextColor) {
    const svgTextNodes = clone.querySelectorAll("text");
    svgTextNodes.forEach((node) => {
      const style = node.getAttribute("style") ?? "";
      node.setAttribute("style", `${style};fill:${options.forceTextColor};color:${options.forceTextColor};`);
    });
  }

  return new XMLSerializer().serializeToString(clone);
};

const prepareExportSvg = (svgElement: SVGSVGElement) => {
  const clone = svgElement.cloneNode(true) as SVGSVGElement;
  const { width, height } = parseViewBox(svgElement);

  clone.setAttribute("xmlns", "http://www.w3.org/2000/svg");
  clone.setAttribute("width", String(width));
  clone.setAttribute("height", String(height));
  if (!clone.getAttribute("viewBox")) {
    clone.setAttribute("viewBox", `0 0 ${width} ${height}`);
  }

  inlineComputedStyles(svgElement, clone, { forceSvgTextColor: EXPORT_TEXT_COLOR });
  const xml = serializeSvgNode(svgElement, clone, {
    backgroundFill: "white",
    watermark: true,
    forceTextColor: EXPORT_TEXT_COLOR,
  });

  return { xml, width, height };
};

const getElementBounds = (element: HTMLElement) => {
  const rect = element.getBoundingClientRect();
  return {
    width: Math.max(1, Math.ceil(Math.max(rect.width, element.scrollWidth, element.clientWidth))),
    height: Math.max(1, Math.ceil(Math.max(rect.height, element.scrollHeight, element.clientHeight))),
  };
};

const buildElementExportSvg = (element: HTMLElement, options: DomExportOptions = {}) => {
  const padding = options.padding ?? 16;
  const background = options.background ?? DOM_EXPORT_BACKGROUND;
  const { width, height } = getElementBounds(element);
  const exportWidth = width + padding * 2;
  const exportHeight = height + padding * 2;

  const clone = element.cloneNode(true) as HTMLElement;
  inlineComputedStyles(element, clone);
  clone.setAttribute("xmlns", "http://www.w3.org/1999/xhtml");
  clone.style.margin = "0";
  clone.style.boxSizing = "border-box";
  clone.style.width = `${width}px`;
  clone.style.maxWidth = `${width}px`;

  const sourceSvgs = Array.from(element.querySelectorAll<SVGSVGElement>("svg"));
  const targetSvgs = Array.from(clone.querySelectorAll<SVGSVGElement>("svg"));
  sourceSvgs.forEach((sourceSvg, index) => {
    const targetSvg = targetSvgs[index];
    if (!targetSvg || !targetSvg.parentNode) return;

    const svgMarkup = serializeSvgNode(sourceSvg, targetSvg, {
      backgroundFill: null,
      watermark: false,
      forceTextColor: null,
    });
    const svgUrl = svgMarkupToDataUrl(svgMarkup);
    const renderedWidth = Math.max(1, Math.ceil(sourceSvg.getBoundingClientRect().width || sourceSvg.clientWidth || parseViewBox(sourceSvg).width));
    const renderedHeight = Math.max(1, Math.ceil(sourceSvg.getBoundingClientRect().height || sourceSvg.clientHeight || parseViewBox(sourceSvg).height));
    const image = document.createElement("img");
    image.setAttribute("src", svgUrl);
    image.setAttribute("alt", "");
    image.setAttribute("width", String(renderedWidth));
    image.setAttribute("height", String(renderedHeight));
    image.style.display = "block";
    image.style.width = `${renderedWidth}px`;
    image.style.height = `${renderedHeight}px`;
    image.style.maxWidth = "100%";
    targetSvg.parentNode.replaceChild(image, targetSvg);
  });

  const xhtml = new XMLSerializer().serializeToString(clone);
  const watermark = `
    <text
      x="${exportWidth - 12}"
      y="${exportHeight - 10}"
      text-anchor="end"
      font-size="10"
      fill="#94a3b8"
      font-family="Arial, sans-serif"
    >Gut Microbiome Atlas</text>
  `;

  const svg = `
    <svg xmlns="http://www.w3.org/2000/svg" xmlns:xhtml="http://www.w3.org/1999/xhtml" width="${exportWidth}" height="${exportHeight}" viewBox="0 0 ${exportWidth} ${exportHeight}">
      <rect width="100%" height="100%" rx="18" ry="18" fill="${background}" />
      <foreignObject x="${padding}" y="${padding}" width="${width}" height="${height}">
        ${xhtml}
      </foreignObject>
      ${watermark}
    </svg>
  `.trim();

  return { svg, width: exportWidth, height: exportHeight, scale: options.scale ?? 2 };
};

const downloadBlob = (blob: Blob, filename: string) => {
  const url = URL.createObjectURL(blob);
  const anchor = document.createElement("a");
  anchor.href = url;
  anchor.download = filename;
  anchor.click();
  URL.revokeObjectURL(url);
};

const rasterizeSvgString = (svg: string, width: number, height: number, filename: string, scale = 2) => {
  const svgBlob = new Blob([svg], { type: "image/svg+xml;charset=utf-8" });
  const svgUrl = URL.createObjectURL(svgBlob);

  const image = new Image();
  image.onload = () => {
    const canvas = document.createElement("canvas");
    canvas.width = Math.round(width * scale);
    canvas.height = Math.round(height * scale);
    const context = canvas.getContext("2d");
    if (!context) {
      URL.revokeObjectURL(svgUrl);
      window.alert(exportErrorMessage());
      return;
    }
    context.fillStyle = "white";
    context.fillRect(0, 0, canvas.width, canvas.height);
    context.drawImage(image, 0, 0, canvas.width, canvas.height);
    canvas.toBlob((blob) => {
      if (!blob) {
        URL.revokeObjectURL(svgUrl);
        window.alert(exportErrorMessage());
        return;
      }
      downloadBlob(blob, `${filename}.png`);
      URL.revokeObjectURL(svgUrl);
    }, "image/png");
  };
  image.onerror = () => {
    URL.revokeObjectURL(svgUrl);
    window.alert(exportErrorMessage());
  };
  image.src = svgUrl;
};

/** Export an SVG element as an .svg file. */
export function exportSVG(svgElement: SVGSVGElement, filename: string) {
  const { xml } = prepareExportSvg(svgElement);
  const blob = new Blob([xml], { type: "image/svg+xml;charset=utf-8" });
  downloadBlob(blob, `${filename}.svg`);
}

/** Export an SVG element as a PNG file. */
export function exportPNG(svgElement: SVGSVGElement, filename: string, scale = 2) {
  const { xml, width, height } = prepareExportSvg(svgElement);
  rasterizeSvgString(xml, width, height, filename, scale);
}

/** Export an arbitrary HTML container as an SVG file. */
export function exportElementSVG(element: HTMLElement, filename: string, options: DomExportOptions = {}) {
  const { svg } = buildElementExportSvg(element, options);
  const blob = new Blob([svg], { type: "image/svg+xml;charset=utf-8" });
  downloadBlob(blob, `${filename}.svg`);
}

/** Export an arbitrary HTML container as a PNG file. */
export function exportElementPNG(element: HTMLElement, filename: string, options: DomExportOptions = {}) {
  const { svg, width, height, scale } = buildElementExportSvg(element, options);
  rasterizeSvgString(svg, width, height, filename, scale);
}

/** Find the nearest SVG element within or above a container. */
export function findSVG(container: HTMLElement | null): SVGSVGElement | null {
  if (!container) return null;
  return container.querySelector<SVGSVGElement>("svg");
}
