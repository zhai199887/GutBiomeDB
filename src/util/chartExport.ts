/**
 * Utility functions for exporting D3/SVG charts as SVG or PNG files
 * 图表导出工具（SVG/PNG）
 */

/** Export an SVG element as an .svg file */
export function exportSVG(svgElement: SVGSVGElement, filename: string) {
  const clone = svgElement.cloneNode(true) as SVGSVGElement;
  clone.setAttribute("xmlns", "http://www.w3.org/2000/svg");
  // Add white background
  const bg = document.createElementNS("http://www.w3.org/2000/svg", "rect");
  bg.setAttribute("width", "100%");
  bg.setAttribute("height", "100%");
  bg.setAttribute("fill", "white");
  clone.insertBefore(bg, clone.firstChild);
  // Add watermark
  const text = document.createElementNS("http://www.w3.org/2000/svg", "text");
  const vb = clone.getAttribute("viewBox")?.split(" ").map(Number) ?? [0, 0, 800, 600];
  text.setAttribute("x", String(vb[2] - 10));
  text.setAttribute("y", String(vb[3] - 8));
  text.setAttribute("text-anchor", "end");
  text.setAttribute("font-size", "10");
  text.setAttribute("fill", "#aaa");
  text.textContent = "Gut Microbiome Atlas";
  clone.appendChild(text);

  const xml = new XMLSerializer().serializeToString(clone);
  const blob = new Blob([xml], { type: "image/svg+xml;charset=utf-8" });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = `${filename}.svg`;
  a.click();
  URL.revokeObjectURL(url);
}

/** Export an SVG element as a PNG file (2x resolution) */
export function exportPNG(svgElement: SVGSVGElement, filename: string, scale = 2) {
  const clone = svgElement.cloneNode(true) as SVGSVGElement;
  clone.setAttribute("xmlns", "http://www.w3.org/2000/svg");
  const bg = document.createElementNS("http://www.w3.org/2000/svg", "rect");
  bg.setAttribute("width", "100%");
  bg.setAttribute("height", "100%");
  bg.setAttribute("fill", "white");
  clone.insertBefore(bg, clone.firstChild);
  const text = document.createElementNS("http://www.w3.org/2000/svg", "text");
  const vb = clone.getAttribute("viewBox")?.split(" ").map(Number) ?? [0, 0, 800, 600];
  text.setAttribute("x", String(vb[2] - 10));
  text.setAttribute("y", String(vb[3] - 8));
  text.setAttribute("text-anchor", "end");
  text.setAttribute("font-size", "10");
  text.setAttribute("fill", "#aaa");
  text.textContent = "Gut Microbiome Atlas";
  clone.appendChild(text);

  const xml = new XMLSerializer().serializeToString(clone);
  const svgBlob = new Blob([xml], { type: "image/svg+xml;charset=utf-8" });
  const svgUrl = URL.createObjectURL(svgBlob);

  const img = new Image();
  img.onload = () => {
    const w = vb[2] * scale;
    const h = vb[3] * scale;
    const canvas = document.createElement("canvas");
    canvas.width = w;
    canvas.height = h;
    const ctx = canvas.getContext("2d")!;
    ctx.fillStyle = "white";
    ctx.fillRect(0, 0, w, h);
    ctx.drawImage(img, 0, 0, w, h);
    canvas.toBlob((blob) => {
      if (!blob) return;
      const pngUrl = URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = pngUrl;
      a.download = `${filename}.png`;
      a.click();
      URL.revokeObjectURL(pngUrl);
    }, "image/png");
    URL.revokeObjectURL(svgUrl);
  };
  img.src = svgUrl;
}

/**
 * Find the nearest SVG element within or above a container.
 */
export function findSVG(container: HTMLElement | null): SVGSVGElement | null {
  if (!container) return null;
  return container.querySelector<SVGSVGElement>("svg");
}
