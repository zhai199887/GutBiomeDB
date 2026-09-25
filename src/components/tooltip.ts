import tippy, { followCursor, hideAll } from "tippy.js";
import type { Instance, Props } from "tippy.js";
import "tippy.js/dist/tippy.css";

/** tippy options */
const options: Partial<Props> = {
  delay: [50, 0],
  duration: [100, 100],
  offset: [15, 15],
  allowHTML: true,
  appendTo: document.body,
  plugins: [followCursor],
  hideOnClick: true,
};

/** update all tooltips in document */
const updateAll = () =>
  document.querySelectorAll("[data-tooltip]").forEach(update);

/** update tippy instance */
const update = (element: Element & { _tippy?: Instance }) => {
  /** if element unmounted, remove */
  if (!element.isConnected) return element._tippy?.destroy();

  /** get tooltip content from attribute */
  const content = element.getAttribute("data-tooltip")?.trim() || "";

  /** don't show if content blank */
  if (!content) return element._tippy?.destroy();

  /** get existing tippy instance or create new */
  const instance = element._tippy ?? tippy(element, options);

  /** update tippy content */
  instance.setContent(content);

  /** force re-position after rendering updates */
  if (instance.popperInstance)
    window.setTimeout(instance.popperInstance.update, 20);
};

/** destroy any tooltip whose trigger was removed from the document */
const destroyRemoved = (node: Node) => {
  if (!(node instanceof Element)) return;
  const element = node as Element & { _tippy?: Instance };
  element._tippy?.destroy();
  node.querySelectorAll("*").forEach((child) => {
    (child as Element & { _tippy?: Instance })._tippy?.destroy();
  });
};

/** hide active tooltips when the user leaves the current interaction context */
const hideTooltips = () => hideAll({ duration: 0 });
document.addEventListener("pointerdown", hideTooltips, true);
window.addEventListener("blur", hideTooltips);
window.addEventListener("scroll", hideTooltips, true);

/** listen for changes to document */
new MutationObserver((mutations) => {
  mutations.forEach((mutation) =>
    mutation.removedNodes.forEach(destroyRemoved),
  );
  updateAll();
}).observe(document.body, {
  childList: true,
  subtree: true,
  attributes: true,
  attributeFilter: ["data-tooltip"],
});
