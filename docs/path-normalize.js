/*
 * Cloudflare Pages may serve mdBook chapters with extensionless URLs
 * (for example /backup), but mdBook sidebar links use .html paths.
 * Ensure the current chapter gets an .active link so toc.js can inject
 * current-page section headers into the sidebar.
 *
 * Also fix external links in the sidebar: mdBook appends .html to all
 * SUMMARY.md hrefs, so https://example.com/foo becomes
 * https://example.com/foo.html. Strip the suffix and open in new tab.
 */
(function () {
  function canonicalizePath(pathname) {
    if (!pathname || pathname.endsWith("/")) {
      return pathname + "index.html";
    }

    var lastSegment = pathname.substring(pathname.lastIndexOf("/") + 1);
    if (lastSegment.indexOf(".") === -1) {
      return pathname + ".html";
    }

    return pathname;
  }

  function markActiveChapterLink() {
    var currentPath = canonicalizePath(window.location.pathname);
    var links = document.querySelectorAll("#mdbook-sidebar a[href]");

    for (var i = 0; i < links.length; i++) {
      var link = links[i];
      var linkUrl = new URL(link.href, window.location.origin);
      if (linkUrl.origin !== window.location.origin) continue;
      var linkPath = canonicalizePath(linkUrl.pathname);
      if (linkPath === currentPath) {
        link.classList.add("active");
        return;
      }
    }
  }

  function fixExternalSidebarLinks() {
    var links = document.querySelectorAll("#mdbook-sidebar ol.chapter a[href]");
    for (var i = 0; i < links.length; i++) {
      var link = links[i];
      var linkUrl = new URL(link.href, window.location.origin);
      if (linkUrl.origin !== window.location.origin) {
        // mdBook appends .html to every SUMMARY.md href — strip it
        var href = link.getAttribute("href");
        if (href.endsWith(".html")) {
          link.setAttribute("href", href.slice(0, -5));
        }
        link.setAttribute("target", "_blank");
        link.setAttribute("rel", "noopener");
      }
    }
  }

  function onReady() {
    markActiveChapterLink();
    fixExternalSidebarLinks();
  }

  // Register as a capture listener so this runs before toc.js' default
  // DOMContentLoaded handler that checks for #mdbook-sidebar .active.
  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", onReady, {
      capture: true,
      once: true,
    });
  } else {
    onReady();
  }
})();
