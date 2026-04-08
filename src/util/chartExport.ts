/**
 * Utility functions for exporting SVG charts as SVG or PNG files.
 * The export path must inline computed styles so downloaded files preserve
 * theme colors, fonts, and stroke settings outside the live DOM.
 */

const parseViewBox = (svgElement: SVGSVGElement) => {
  const viewBox = svgElement.getAttribute("viewBox");
  if (viewBox) {
    const [, , width, height] = viewBox.split(/[\s,]+/).map(Number);
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

const inlineComputedStyles = (sourceRoot: SVGSVGElement, targetRoot: SVGSVGElement) => {
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
    if (sourceNode instanceof SVGTextElement) {
      styleText += `fill:${EXPORT_TEXT_COLOR};color:${EXPORT_TEXT_COLOR};`;
    }
    targetNode.setAttribute("style", styleText);
  });
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

  inlineComputedStyles(svgElement, clone);

  const background = document.createElementNS("http://www.w3.org/2000/svg", "rect");
  background.setAttribute("width", "100%");
  background.setAttribute("height", "100%");
  background.setAttribute("fill", "white");
  clone.insertBefore(background, clone.firstChild);

  const watermark = document.createElementNS("http://www.w3.org/2000/svg", "text");
  watermark.setAttribute("x", String(width - 10));
  watermark.setAttribute("y", String(height - 8));
  watermark.setAttribute("text-anchor", "end");
  watermark.setAttribute("font-size", "10");
  watermark.setAttribute("fill", "#9ca3af");
  watermark.setAttribute("font-family", "Arial, sans-serif");
  watermark.textContent = "Gut Microbiome Atlas";
  clone.appendChild(watermark);

  return { clone, width, height };
};

/** Export an SVG element as an .svg file. */
export function exportSVG(svgElement: SVGSVGElement, filename: string) {
  const { clone } = prepareExportSvg(svgElement);
  const xml = new XMLSerializer().serializeToString(clone);
  const blob = new Blob([xml], { type: "image/svg+xml;charset=utf-8" });
  const url = URL.createObjectURL(blob);
  const anchor = document.createElement("a");
  anchor.href = url;
  anchor.download = `${filename}.svg`;
  anchor.click();
  URL.revokeObjectURL(url);
}

/** Export an SVG element as a PNG file. */
export function exportPNG(svgElement: SVGSVGElement, filename: string, scale = 2) {
  const { clone, width, height } = prepareExportSvg(svgElement);
  const xml = new XMLSerializer().serializeToString(clone);
  const svgBlob = new Blob([xml], { type: "image/svg+xml;charset=utf-8" });
  const svgUrl = URL.createObjectURL(svgBlob);

  const image = new Image();
  image.onload = () => {
    const canvas = document.createElement("canvas");
    canvas.width = Math.round(width * scale);
    canvas.height = Math.round(height * scale);
    const context = canvas.getContext("2d");
    if (!context) {
      URL.revokeObjectURL(svgUrl);
      return;
    }
    context.fillStyle = "white";
    context.fillRect(0, 0, canvas.width, canvas.height);
    context.drawImage(image, 0, 0, canvas.width, canvas.height);
    canvas.toBlob((blob) => {
      if (!blob) {
        URL.revokeObjectURL(svgUrl);
        return;
      }
      const pngUrl = URL.createObjectURL(blob);
      const anchor = document.createElement("a");
      anchor.href = pngUrl;
      anchor.download = `${filename}.png`;
      anchor.click();
      URL.revokeObjectURL(pngUrl);
      URL.revokeObjectURL(svgUrl);
    }, "image/png");
  };
  image.onerror = () => {
    URL.revokeObjectURL(svgUrl);
  };
  image.src = svgUrl;
}

/** Find the nearest SVG element within or above a container. */
export function findSVG(container: HTMLElement | null): SVGSVGElement | null {
  if (!container) return null;
  return container.querySelector<SVGSVGElement>("svg");
}
